/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.wormhole.sparkx.hdfs.hdfslog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import edp.wormhole.common.feedback.{ErrorPattern, FeedbackPriority}
import edp.wormhole.common.json.JsonParseUtils
import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sinks.utils.SinkCommonUtils._
import edp.wormhole.sparkx.common._
import edp.wormhole.sparkx.hdfs.{HdfsDirective, HdfsFinder, HdfsFlowConfig, PartitionResult}
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{KafkaInputConfig, WormholeConfig}
import edp.wormhole.ums.UmsSchemaUtils._
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.ums._
import edp.wormhole.util.{DateUtils, DtFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.KafkaException
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

//fileName:  ../oracle.oracle0.db.table/1/0/0/data_increment_data(data_initial_data)/right(wrong)/min ts 0740（文件编号4位，左补零）
//metaFile   ../oracle.oracle0.db.table/1/0/0/data_increment_data(data_initial_data)/right(wrong)/metadata_min ts 0740
//metaContent  currentyyyyMMddHHmmss0740_0_20171108181403252_20171106171538333_20171111171538333
//              文件编号_0/1(写完/未写完)_createtime_minUmsts_maxUmsts
object HdfsLogMainProcess extends EdpLogging {

  // Map[(protocoltype,namespace(accurate to table)), HashMap["right", HashMap[index,(filename, size, metaContent)]]]
  val namespace2FileStore = mutable.HashMap.empty[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]]

  //[namespace, hour]
  //  val directiveNamespaceRule = mutable.HashMap.empty[String, Int]

  val fileMaxSize = 128
  val metadata = "metadata_"
  val hdfsLog = "hdfslog/"
  var hdfsActiveUrl = ""

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig,session: SparkSession, appId: String, kafkaInput: KafkaInputConfig,ssc: StreamingContext): Unit = {
    var zookeeperFlag = false
    stream.foreachRDD(foreachFunc = (streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val batchId = UUID.randomUUID().toString
      val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
      streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
      val topicPartitionOffset = SparkUtils.getTopicPartitionOffset(offsetInfo)
      val hdfslogMap: Map[String, HdfsFlowConfig] = ConfMemoryStorage.getHdfslogMap

      try {
        val rddTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val directiveTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        HdfsDirective.doDirectiveTopic(config, stream)

        val sourceNamespaceSet = ConfMemoryStorage.getHdfslogNamespaceSet
        val streamTransformedRdd: RDD[((String, String), String)] = streamRdd.map(message => {
          val messageKey = SparkxUtils.getDefaultKey(message.key, sourceNamespaceSet, SparkxUtils.getDefaultKeyConfig(config.special_config))
          if (messageKey == null || messageKey.trim.isEmpty) {
            val namespace = UmsCommonUtils.getFieldContentFromJson(message.value, "namespace")
            var protocolType = UmsCommonUtils.getProtocolTypeFromUms(message.value)
            if (protocolType == null || protocolType.isEmpty) protocolType = UmsProtocolType.DATA_INCREMENT_DATA.toString
            ((protocolType, namespace), message.value)
          } else {
            val (protocol, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(messageKey)
            ((protocol.toString, namespace), message.value)
          }
        })

        logInfo(s"config.rdd_partition_number ${config.rdd_partition_number}")
        val dataParRdd: RDD[((String, String), String)] = if (config.rdd_partition_number == -1) {
          streamTransformedRdd
        }else if(config.rdd_partition_number < -1){
          streamTransformedRdd.partitionBy(new HashPartitioner( -config.rdd_partition_number ))
        } else {
          streamTransformedRdd.repartition(config.rdd_partition_number)
        }

        val namespace2FileMap: Map[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]] = namespace2FileStore.toMap
        //        val validNameSpaceMap: Map[String, Int] = directiveNamespaceRule.toMap //validNamespaceMap is NOT real namespace, has *
        //        logInfo("validNameSpaceMap:" + validNameSpaceMap)

        val mainDataTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        val wholeConfig=HdfsFinder.fillWormholeConfig(config,ssc.sparkContext.hadoopConfiguration)
        val partitionResultRdd = dataParRdd.mapPartitionsWithIndex { case (index, partition) =>
          // partition: ((protocol,namespace), message.value)
          val resultList = ListBuffer.empty[PartitionResult]
          val namespaceMap = mutable.HashMap.empty[(String, String), HdfsFlowConfig]
          val flowErrorList = mutable.ListBuffer.empty[FlowErrorInfo]
          //real namespace, do not have *
          val dataList = partition.toList
          dataList.foreach { case ((protocolType, sourceNamespace), _) =>
            val result: Map[String, HdfsFlowConfig] = checkValidNamespace(sourceNamespace, hdfslogMap)
            if (result.nonEmpty && (protocolType == UmsProtocolType.DATA_INITIAL_DATA.toString ||
              protocolType == UmsProtocolType.DATA_INCREMENT_DATA.toString || protocolType == UmsProtocolType.DATA_BATCH_DATA.toString)) {
              val (_, flowConfig) = result.head
              if (!namespaceMap.contains((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)))
                namespaceMap((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)) = flowConfig
              if (!namespaceMap.contains((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)))
                namespaceMap((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)) = flowConfig
              if (!namespaceMap.contains((UmsProtocolType.DATA_BATCH_DATA.toString, sourceNamespace)))
                namespaceMap((UmsProtocolType.DATA_BATCH_DATA.toString, sourceNamespace)) = flowConfig
            }
          }
          logInfo("check namespace ok. all data num=" + dataList.size + ",namespaceMap=" + namespaceMap)

          namespaceMap.foreach { case ((protocol, namespace), flowConfig) =>
            val namespaceDataList = ListBuffer.empty[String]
            dataList.foreach(data => {
              if (data._1._1 == protocol && data._1._2 == namespace) namespaceDataList.append(data._2)
            })
            logInfo("protocol=" + protocol + ",namespace=" + namespace + ",data num=" + namespaceDataList.size)
            var tmpMinTs = ""
            var tmpMaxTs = ""
            var tmpCount = 0
            try {
              if (namespaceDataList.nonEmpty) {
                val tmpResult: PartitionResult = doMainData(protocol, namespace, namespaceDataList, wholeConfig, flowConfig.hourDuration,
                  namespace2FileMap, config.zookeeper_path, hdfslogMap, index)
                tmpMinTs = tmpResult.minTs
                tmpMaxTs = tmpResult.maxTs
                tmpCount = tmpResult.allCount
                resultList += tmpResult
              }
            } catch {
              case e: Throwable =>
                logAlert("sink,sourceNamespace=" + namespace + ", count = " + tmpCount, e)
                flowErrorList.append(FlowErrorInfo(flowConfig.flowId, protocol, namespace, namespace, e, ErrorPattern.FlowError,
                  flowConfig.incrementTopics, -1))
            }
          }
          val res = ListBuffer.empty[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])]
          res.append((resultList, flowErrorList))
          res.toIterator
        }.cache

        val writeResult: Array[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])] = partitionResultRdd.collect
        /*logInfo(s"hdfs_main_process writeResult $writeResult")

        writeResult.foreach(writeone => {
          writeone._1.foreach(partitionResult => {
            logInfo(s"writeResult partitionResult $partitionResult")
          })
          writeone._2.foreach(flowErrorInfo => {
            logInfo(s"writeResult flowErrorInfo $flowErrorInfo")
          })
        })*/

        logInfo("writeResult.size:"+writeResult.size)

        writeResult.foreach(eachPartionResultError => {
          logInfo("eachPartionResult.size:"+eachPartionResultError._1.size)
          eachPartionResultError._1.foreach(eachResult => {
            if (!namespace2FileStore.contains((eachResult.protocol, eachResult.namespace))) {
              namespace2FileStore((eachResult.protocol, eachResult.namespace)) = mutable.HashMap.empty[String, mutable.HashMap[Int, (String, Int, String)]]
            }
            if (namespace2FileStore.contains((eachResult.protocol, eachResult.namespace))) {
              if (!namespace2FileStore(eachResult.protocol, eachResult.namespace).contains("right"))
                namespace2FileStore((eachResult.protocol, eachResult.namespace))("right") = mutable.HashMap.empty[Int, (String, Int, String)]
              if (!namespace2FileStore(eachResult.protocol, eachResult.namespace).contains("wrong"))
                namespace2FileStore((eachResult.protocol, eachResult.namespace))("wrong") = mutable.HashMap.empty[Int, (String, Int, String)]
            }

            if (eachResult.result && eachResult.allCount > 0 && eachResult.errorFileName != null)
              namespace2FileStore((eachResult.protocol, eachResult.namespace))("wrong")(eachResult.index) = (eachResult.errorFileName, eachResult.errorCount, eachResult.errorMetaContent)
            if (eachResult.result && eachResult.allCount > 0 && eachResult.correctFileName != null)
              namespace2FileStore((eachResult.protocol, eachResult.namespace))("right")(eachResult.index) = (eachResult.correctFileName, eachResult.correctCount, eachResult.correctMetaContent)
          })
        })
        logInfo("end writeResult")

        //logInfo(s"namespace2FileStore $namespace2FileStore")

        val flowIdSet = mutable.HashSet.empty[Long]
        writeResult.foreach(eachPartionResultError => {
          logInfo("eachPartionError.size:"+eachPartionResultError._2.size)
          if (eachPartionResultError._2.nonEmpty) {
            eachPartionResultError._2.foreach(flowErrorInfo => {
              if (!flowIdSet.contains(flowErrorInfo.flowId)) {
                try {
                  flowIdSet.add(flowErrorInfo.flowId)
                  SparkxUtils.setFlowErrorMessage(flowErrorInfo.incrementTopicList,
                    topicPartitionOffset, config, flowErrorInfo.matchSourceNamespace, flowErrorInfo.sinkNamespace, flowErrorInfo.count,
                    flowErrorInfo.error, batchId, flowErrorInfo.protocolType, flowErrorInfo.flowId, flowErrorInfo.errorPattern)
                } catch {
                  case e: Throwable =>
                    logError("setFlowErrorMessage", e)
                }
              }
            })
          }
        })

        logInfo(s"flowIdSet $flowIdSet")

        val statsProtocolNamespace: Set[(String, String, Long)] = writeResult.flatMap(eachPartionResultError => {
          eachPartionResultError._1.map(r => {
            (r.protocol, r.namespace, r.flowId)
          })
        }).toSet

        logInfo(s"statsProtocolNamespace $statsProtocolNamespace")

        statsProtocolNamespace.foreach { case (protocol, namespace, flowId) =>
          var count = 0
          var cdcTs = 0L
          writeResult.foreach(eachPartionResultError => {
            eachPartionResultError._1.foreach(r => {
              if (protocol == r.protocol && namespace == r.namespace) {
                count += r.allCount
                val tmpMaxTs = if (!r.maxTs.trim.equals("")) DateUtils.dt2date(r.maxTs).getTime else 0L
                if (cdcTs < tmpMaxTs) cdcTs = tmpMaxTs
              }
            })
          })
          val doneTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
          logInfo(s"count $count, cdcTs $cdcTs")
          if (count > 0 && cdcTs > 0){
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              UmsProtocolUtils.feedbackFlowStats(namespace, protocol, DateUtils.currentDateTime, config.spark_config.stream_id,
                batchId, namespace, topicPartitionOffset.toJSONString,
                count, DateUtils.dt2string(cdcTs*1000, DtFormat.TS_DASH_MILLISEC), rddTs, directiveTs, mainDataTs, mainDataTs, mainDataTs, doneTs.toString, flowId),
              Some(UmsProtocolType.FEEDBACK_FLOW_STATS + "." + flowId), config.kafka_output.brokers)
          }
          logInfo("finish one stat")

        }
        logInfo("finish stat ")
        logInfo("start printTopicPartition")
        SparkxUtils.printTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config, batchId)
        partitionResultRdd.unpersist()
      } catch {
        case e: KafkaException=>
          logError("kafka consumer error,"+e.getMessage, e)
          if(e.getMessage.contains("Failed to construct kafka consumer")){
            logError("kafka consumer error ,stop spark streaming")

            SparkxUtils.setFlowErrorMessage(List.empty[String],
              topicPartitionOffset, config, "testkerberos", "testkerberos", -1,
              e, batchId, UmsProtocolType.DATA_BATCH_DATA.toString + "," + UmsProtocolType.DATA_INCREMENT_DATA.toString + "," + UmsProtocolType.DATA_INITIAL_DATA.toString,
              -config.spark_config.stream_id, ErrorPattern.StreamError)

            stream.stop()

            throw e
          }
        case e: Throwable =>
          logAlert("batch error", e)
          hdfslogMap.foreach { case (sourceNamespace, flowConfig) =>
            SparkxUtils.setFlowErrorMessage(flowConfig.incrementTopics,
              topicPartitionOffset, config, sourceNamespace, sourceNamespace, -1,
              e, batchId, UmsProtocolType.DATA_BATCH_DATA.toString + "," + UmsProtocolType.DATA_INCREMENT_DATA.toString + "," + UmsProtocolType.DATA_INITIAL_DATA.toString,
              flowConfig.flowId, ErrorPattern.StreamError)

          }
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetInfo.toArray)
      if (!zookeeperFlag) {
        logInfo("write appid to zookeeper," + appId)
        SparkContextUtils.checkSparkRestart(config.zookeeper_address, config.zookeeper_path, config.spark_config.stream_id, appId)
        SparkContextUtils.deleteZookeeperOldAppidPath(appId, config.zookeeper_address, config.zookeeper_path, config.spark_config.stream_id)
        WormholeZkClient.createPath(config.zookeeper_address, config.zookeeper_path + "/" + config.spark_config.stream_id + "/" + appId)
        zookeeperFlag = true
      }
    })
  }


  private def createFile(filePrefixShardingSlash: String, configuration: Configuration, minTs: String,
                         maxTs: String, zookeeperPath: String, index: Int): (String, String) = {
    val processTime = DateUtils.currentyyyyMMddHHmmssmls
    val fileTime = if(minTs==null||minTs.isEmpty) processTime else DateUtils.yyyyMMddHHmmssmls(minTs)
    val indexStr = "000" + index
    val incrementalId = fileTime + indexStr.substring(indexStr.length - 4, indexStr.length)

    val metaName = if (minTs == null) filePrefixShardingSlash + "wrong" + "/" + "metadata_" + incrementalId else filePrefixShardingSlash + "right" + "/" + "metadata_" + incrementalId
    val metaContent: String = if (minTs == null) incrementalId + "_" + "0_" + processTime + "_" + processTime else incrementalId + "_" + "0_" + processTime + "_" + minTs + "_" + maxTs
    val dataName = if (minTs == null) filePrefixShardingSlash + "wrong" + "/" + incrementalId else filePrefixShardingSlash + "right" + "/" + incrementalId
    logInfo("dataName:" + dataName)
    logInfo("metaName:" + metaName)
    HdfsUtils.createPath(configuration, metaName)
    HdfsUtils.createPath(configuration, dataName)
    HdfsUtils.writeString(configuration, metaContent, metaName)
    (metaContent, dataName)
  }

  private def getMinMaxTs(message: String, namespace: String, hdfslogMap: Map[String, HdfsFlowConfig]) = {
    var currentUmsTsMin: String = ""
    var currentUmsTsMax: String = ""
    val validMap: Map[String, HdfsFlowConfig] = checkValidNamespace(namespace, hdfslogMap)
    //    if (hdfslogMap.contains(namespace)) {
    //      val mapValue = hdfslogMap(namespace)
    if (validMap != null && validMap.nonEmpty && validMap.head._2.dataType == DataTypeEnum.UMS_EXTENSION.toString) {
      val mapValue = validMap.head._2
      val value: Seq[UmsTuple] = JsonParseUtils.dataParse(message, mapValue.jsonSchema.fieldsInfo, mapValue.jsonSchema.twoFieldsArr)
      val schema = mapValue.jsonSchema.schemaField
      val umsTsIndex = schema.map(_.name).indexOf(TS.toString)
      value.foreach(tuple => {
        val umsTs = tuple.tuple(umsTsIndex)
        if (currentUmsTsMin == "") {
          currentUmsTsMin = umsTs
          currentUmsTsMax = umsTs
        } else {
          currentUmsTsMax = if (firstTimeAfterSecond(umsTs, currentUmsTsMax)) umsTs else currentUmsTsMax
          currentUmsTsMin = if (firstTimeAfterSecond(currentUmsTsMin, umsTs)) umsTs else currentUmsTsMin
        }
      })
    } else {
      val ums = toUms(message)
      var umsTsIndex: Int = -1
      ums.schema.fields.get.foreach(f => {
        if (f.name.toLowerCase == TS.toString) umsTsIndex = ums.schema.fields.get.indexOf(f)
      })

      ums.payload_get.foreach(tuple => {
        val umsTs = tuple.tuple(umsTsIndex)
        if (currentUmsTsMin == "") {
          currentUmsTsMin = umsTs
          currentUmsTsMax = umsTs
        } else {
          currentUmsTsMax = if (firstTimeAfterSecond(umsTs, currentUmsTsMax)) umsTs else currentUmsTsMax
          currentUmsTsMin = if (firstTimeAfterSecond(currentUmsTsMin, umsTs)) umsTs else currentUmsTsMin
        }
      })
    }
    (DateUtils.yyyyMMddHHmmssmls(currentUmsTsMin), DateUtils.yyyyMMddHHmmssmls(currentUmsTsMax))
  }


  private def writeAndCreateFile(currentMetaContent: String, fileName: String, configuration: Configuration, input: ByteArrayOutputStream,
                                 content: Array[Byte], minTs: String, maxTs: String, finalMinTs: String, finalMaxTs: String,
                                 splitMark: Array[Byte], zookeeperPath: String, index: Int) = {
    val metaName = getMetaName(fileName)
    setMetaDataFinished(metaName, currentMetaContent, configuration, minTs, finalMinTs, finalMaxTs)
    val bytes = input.toByteArray
    val in = new ByteArrayInputStream(bytes)
    HdfsUtils.appendToFile(configuration, fileName, in)
    input.reset()
    var currentSize = 0

    val slashPosition = fileName.lastIndexOf("/")
    val filePrefix = fileName.substring(0, slashPosition + 1)
    val filePrefixShardingSlash = filePrefix.substring(0, filePrefix.length - 6)
    val (newMeta, newFileName) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
    currentSize += content.length + splitMark.length
    input.write(content)
    input.write(splitMark)
    (newFileName, newMeta, currentSize)
  }

  private def doMainData(protocol: String,
                         namespace: String,
                         dataList: Seq[String],
                         config: WormholeConfig,
                         hour: Int,
                         namespace2FileMap: Map[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]],
                         zookeeperPath: String,
                         hdfslogMap: Map[String, HdfsFlowConfig],
                         index: Int): PartitionResult = {
    var valid = true
    val namespaceSplit = namespace.split("\\.")
    val namespaceDb = namespaceSplit.slice(0, 3).mkString(".")
    val namespaceTable = namespaceSplit(3)
    val version = namespaceSplit(4)
    val sharding1 = namespaceSplit(5)
    val sharding2 = namespaceSplit(6)
    //logInfo(s"namespace2FileMap $namespace2FileMap, protocol $protocol, namespace $namespace")

    val index2FileRightMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("right")) {
      namespace2FileMap(protocol, namespace)("right")
    } else null

    val index2FileWrongMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("wrong")) {
      namespace2FileMap(protocol, namespace)("wrong")
    } else null

    //logInfo(s"index2FileRightMap $index2FileRightMap, index $index")
    var (correctFileName, correctCurrentSize, currentCorrectMetaContent) = if (index2FileRightMap != null && index2FileRightMap.contains(index))
      (index2FileRightMap(index)._1, index2FileRightMap(index)._2, index2FileRightMap(index)._3) else (null, 0, null)

    var (errorFileName, errorCurrentSize, currentErrorMetaContent) = if (index2FileWrongMap != null && index2FileWrongMap.contains(index))
      (index2FileWrongMap(index)._1, index2FileWrongMap(index)._2, index2FileWrongMap(index)._3) else (null, 0, null)

    var finalMinTs: String = if (currentCorrectMetaContent == null) "" else currentCorrectMetaContent.split("_")(3)
    var finalMaxTs: String = if (currentCorrectMetaContent == null) "" else currentCorrectMetaContent.split("_")(4)
    val count = dataList.size
    val inputCorrect = new ByteArrayOutputStream()
    val inputError = new ByteArrayOutputStream()
    try {
      val (configuration, hdfsRoot, hdfsPath) = HdfsFinder.getHadoopConfiguration(config)
      val filePrefixShardingSlash = hdfsPath + "/" + "hdfslog" + "/" + namespaceDb.toLowerCase + "/" + namespaceTable.toLowerCase + "/" + version + "/" + sharding1 + "/" + sharding2 + "/" + protocol + "/"
      //修改map为只存储后半部分，此处改为拼接
      correctFileName = if(correctFileName==null) null else if(correctFileName.startsWith("/")) hdfsRoot + correctFileName else hdfsRoot + "/" + correctFileName
      errorFileName = if(errorFileName==null) null else if(errorFileName.startsWith("/")) hdfsRoot + errorFileName else hdfsRoot + "/" + errorFileName
      logInfo(s"correctFileName:$correctFileName,errorFileName:$errorFileName")
      dataList.foreach(data => {
        val splitMark = "\n".getBytes()
        val splitMarkLength = splitMark.length
        var minTs: String = null
        var maxTs: String = null
        try {
          val timePair = getMinMaxTs(data, namespace, hdfslogMap)
          minTs = timePair._1
          maxTs = timePair._2
        } catch {
          case NonFatal(e) => logError(s"message convert failed:\n$data", e)
        }

        if (minTs == null && errorFileName == null) {
          //获取minTs和maxTs异常，并且未创建错误文件，则创建meta文件和data文件
          logInfo("获取minTs和maxTs异常，并且未创建错误文件，创建meta文件和data文件")
          val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
          logInfo("end 获取minTs和maxTs异常,并且未创建错误文件，创建meta文件和data文件")
          errorFileName = name
          currentErrorMetaContent = meta
          errorCurrentSize = 0
        }

        if (minTs == null && errorFileName != null) {
          logInfo("获取minTs和maxTs异常，已创建错误文件")
          val errorMetaName = getMetaName(errorFileName)
          val metaContent = currentErrorMetaContent
          val metaContentSplit = metaContent.split("_")
          val length = metaContentSplit.length
          val originalProcessTime = metaContentSplit(length - 3)
          if (DateUtils.dt2timestamp(DateUtils.dt2dateTime(originalProcessTime).plusHours(hour)).compareTo(DateUtils.dt2timestamp(DateUtils.dt2dateTime(DateUtils.currentyyyyMMddHHmmssmls))) < 0) {
            logInfo("获取minTs和maxTs异常，已创建错误文件，但文件已超过创建间隔，需要重新创建")
            setMetaDataFinished(errorMetaName, metaContent, configuration, minTs, finalMinTs, finalMaxTs)
            logInfo("end 获取minTs和maxTs异常，已创建错误文件，但文件已超过创建间隔，需要重新创建")
            val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
            logInfo("end createFile 获取minTs和maxTs异常，已创建错误文件，但文件已超过创建间隔，需要重新创建")
            errorFileName = name
            currentErrorMetaContent = meta
            errorCurrentSize = 0
          }
        }

        if (minTs != null && correctFileName == null) {
          logInfo("启动后第一次写数据，先创建文件")
          val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
          logInfo("end 启动后第一次写数据，先创建文件")
          correctFileName = name
          currentCorrectMetaContent = meta
          correctCurrentSize = 0
          finalMinTs = minTs
          finalMaxTs = maxTs
        }

        if (minTs != null && correctFileName != null) {
          val correctMetaName = getMetaName(correctFileName)
          val metaContent = currentCorrectMetaContent
          val metaContentSplit = metaContent.split("_")
          val length = metaContentSplit.length
          val originalProcessTime = metaContentSplit(length - 3)
          if (DateUtils.dt2timestamp(DateUtils.dt2dateTime(originalProcessTime).plusHours(hour)).compareTo(DateUtils.dt2timestamp(DateUtils.dt2dateTime(DateUtils.currentyyyyMMddHHmmssmls))) < 0) {
            logInfo("正常获取minTs和maxTs，已创建文件，但文件已超过创建间隔，需要重新创建")
            setMetaDataFinished(correctMetaName, metaContent, configuration, minTs, finalMinTs, finalMaxTs)
            logInfo("end 正常获取minTs和maxTs，已创建文件，但文件已超过创建间隔，需要重新创建")
            val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
            logInfo("end createFile 正常获取minTs和maxTs，已创建文件，但文件已超过创建间隔，需要重新创建")
            correctFileName = name
            currentCorrectMetaContent = meta
            correctCurrentSize = 0
            finalMinTs = minTs
            finalMaxTs = maxTs
          }
        }

        val content = data.getBytes(StandardCharsets.UTF_8)

        if (minTs == null) {
          if (errorCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) {
            //60 * 1024 * 1024 Bytes = 60MB
            logInfo("因获取minTs和maxTs异常，将所有数据写入错误文件中")
            errorCurrentSize += content.length + splitMarkLength
            inputError.write(content)
            inputError.write(splitMark)
            val indexLastUnderScore = currentErrorMetaContent.lastIndexOf("_")
            currentErrorMetaContent = currentErrorMetaContent.substring(0, indexLastUnderScore + 1) + DateUtils.currentyyyyMMddHHmmssmls

          } else {
            logInfo("因获取minTs和maxTs异常，并且错误文件已经达到最大值，先新建错误文件，再将所有数据写入错误文件中")
            val errorTuple = writeAndCreateFile(currentErrorMetaContent, errorFileName, configuration, inputError,
              content, minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, index)
            logInfo("end 因获取minTs和maxTs异常，并且错误文件已经达到最大值，先新建错误文件，再将所有数据写入错误文件中")
            errorFileName = errorTuple._1
            currentErrorMetaContent = errorTuple._2
            errorCurrentSize = errorTuple._3

          }
        } else {
          if (correctCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) {
            correctCurrentSize += content.length + splitMarkLength
            inputCorrect.write(content)
            inputCorrect.write(splitMark)
            finalMinTs = if (finalMinTs.isEmpty || firstTimeAfterSecond(finalMinTs, minTs)) minTs else finalMinTs
            finalMaxTs = if (finalMaxTs.isEmpty || firstTimeAfterSecond(maxTs, finalMaxTs)) maxTs else finalMaxTs
            val currentCorrectMetaContentSplit = currentCorrectMetaContent.split("_")
            currentCorrectMetaContent = currentCorrectMetaContentSplit(0) + "_0_" + currentCorrectMetaContentSplit(2) + "_" + finalMinTs + "_" + finalMaxTs
          } else {
            logInfo("文件已经达到最大值，先新建文件")
            val correctTuple = writeAndCreateFile(currentCorrectMetaContent, correctFileName, configuration, inputCorrect, content,
              minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, index)
            logInfo("end 文件已经达到最大值，先新建文件")
            correctFileName = correctTuple._1
            currentCorrectMetaContent = correctTuple._2
            correctCurrentSize = correctTuple._3
            finalMinTs = minTs
            finalMaxTs = maxTs
          }
        }
      })
      val bytesError = inputError.toByteArray
      val bytesCorrect = inputCorrect.toByteArray
      val inError = new ByteArrayInputStream(bytesError)
      val inCorrect = new ByteArrayInputStream(bytesCorrect)
      if (bytesError.nonEmpty) {
        logInfo("存在错误数据，写入错误文件，并更新错误meta文件")
        HdfsUtils.appendToFile(configuration, errorFileName, inError)
        val errorMetaName = getMetaName(errorFileName)
        logInfo("end appendToFile 存在错误数据，写入错误文件，并更新错误meta文件")
        updateMeta(errorMetaName, currentErrorMetaContent, configuration)
        logInfo("end updateMeta 存在错误数据，写入错误文件，并更新错误meta文件")
      }
      if (bytesCorrect.nonEmpty) {
        logInfo("存在解析正确的数据，写入文件，并更新meta文件，共计：" + dataList.size)
        logInfo(s"configuration:$configuration,correctFileName:$correctFileName")
        HdfsUtils.appendToFile(configuration, correctFileName, inCorrect)
        logInfo("end appendToFile 存在解析正确的数据，写入文件，并更新meta文件，共计：" + dataList.size)
        val correctMetaName = getMetaName(correctFileName)
        updateMeta(correctMetaName, currentCorrectMetaContent, configuration)
        logInfo("end updateMeta 存在解析正确的数据，写入文件，并更新meta文件，共计：" + dataList.size)
      }
    } catch {
      case e: Throwable =>
        logAlert("batch error", e)
        valid = false
    } finally {
      try {
        if (inputError != null)
          inputError.close()
        if (inputCorrect != null)
          inputCorrect.close()
      } catch {
        case e: Throwable =>
          logWarning("close", e)
      }
    }
    //    val flowId = if (hdfslogMap.contains(namespace)) hdfslogMap(namespace).flowId else -1
    val vaildMap: Map[String, HdfsFlowConfig] = checkValidNamespace(namespace, hdfslogMap)
    val flowId = if (vaildMap != null && vaildMap.nonEmpty) vaildMap.head._2.flowId else -1

    //logInfo(s"index, valid, errorFileName, errorCurrentSize, currentErrorMetaContent, correctFileName, correctCurrentSize, currentCorrectMetaContent, protocol, namespace, finalMinTs, finalMaxTs, count, flowId: $index, $valid, $errorFileName, $errorCurrentSize, $currentErrorMetaContent, $correctFileName, $correctCurrentSize, $currentCorrectMetaContent, $protocol, $namespace, $finalMinTs, $finalMaxTs, $count, $flowId")

    PartitionResult(index, valid, HdfsFinder.getHdfsRelativeFileName(errorFileName), errorCurrentSize, currentErrorMetaContent, HdfsFinder.getHdfsRelativeFileName(correctFileName),
      correctCurrentSize, currentCorrectMetaContent, protocol, namespace, finalMinTs, finalMaxTs, count, flowId)
  }

  def setMetaDataFinished(metaName: String, currentMetaContent: String, configuration: Configuration, minTs: String, finalMinTs: String, finalMaxTs: String): Unit = {
    var newMetaContent: String = null
    val splitCurrentMetaContent = currentMetaContent.split("_")
    if (minTs != null) {
      newMetaContent = splitCurrentMetaContent(0) + "_1_" + splitCurrentMetaContent(2) + "_" + finalMinTs + "_" + finalMaxTs
    } else {
      newMetaContent = splitCurrentMetaContent(0) + "_1_" + splitCurrentMetaContent(2) + "_" + DateUtils.currentyyyyMMddHHmmssmls
    }
    HdfsUtils.writeString(configuration, newMetaContent, metaName)
  }

  def updateMeta(metaName: String, metaContent: String, configuration: Configuration): Unit = {
    HdfsUtils.writeString(configuration, metaContent, metaName)
  }

  def getMetaName(fileName: String): String = {
    val incrementalId = fileName.substring(fileName.lastIndexOf("/") + 1).trim
    fileName.substring(0, fileName.lastIndexOf("/") + 1) + metadata + incrementalId
  }


  def matchNameSpace(rule: String, namespace: String): Boolean = {
    val regex = rule.replace(".", "\\.") replace("*", ".*")
    namespace.matches(regex)
  }


  def checkValidNamespace(namespace: String, validNameSpaceMap: Map[String, HdfsFlowConfig]): Map[String, HdfsFlowConfig] = {
    validNameSpaceMap.filter { case (rule, _) =>
      val namespaceLowerCase = namespace.toLowerCase
      matchNameSpace(rule, namespaceLowerCase)
    }
  }
}
