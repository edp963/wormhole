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


package edp.wormhole.sparkx.hdfslog

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
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.UmsSchemaUtils._
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.ums._
import edp.wormhole.util.{DateUtils, DtFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

//fileName:  ../oracle.oracle0.db.table/1/0/0/data_increment_data(data_initial_data)/right(wrong)/currentyyyyMMddHHmmss0740（文件编号4位，左补零）
//metaFile   ../oracle.oracle0.db.table/1/0/0/data_increment_data(data_initial_data)/right(wrong)/metadata_currentyyyyMMddHHmmss0740
//metaContent  currentyyyyMMddHHmmss0740_0_20171108181403252_20171106171538333_20171111171538333
//              文件编号_0/1(写完/未写完)_createtime_minUmsts_maxUmsts
object HdfsMainProcess extends EdpLogging {

  // Map[(protocoltype,namespace(accurate to table)), HashMap["right", HashMap[index,(filename, size, metaContent)]]]
  val namespace2FileStore = mutable.HashMap.empty[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]]

  //[namespace, hour]
  //  val directiveNamespaceRule = mutable.HashMap.empty[String, Int]

  val fileMaxSize = 128
  val metadata = "metadata_"
  val hdfsLog = "hdfslog/"

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, appId: String, kafkaInput: KafkaInputConfig): Unit = {
    var zookeeperFlag = false
    stream.foreachRDD(foreachFunc = (streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val batchId = UUID.randomUUID().toString
      val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
      streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
      val topicPartitionOffset = SparkUtils.getTopicPartitionOffset(offsetInfo)
      val hdfslogMap: Map[String, HdfsLogFlowConfig] = ConfMemoryStorage.getHdfslogMap

      try {
        val rddTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val directiveTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        HdfsDirective.doDirectiveTopic(config, stream)

        val streamTransformedRdd: RDD[((String, String), String)] = streamRdd.map(message => {
          if (message.key == null || message.key.trim.isEmpty) {
            val namespace = UmsCommonUtils.getFieldContentFromJson(message.value, "namespace")
            var protocolType = UmsCommonUtils.getProtocolTypeFromUms(message.value)
            if (protocolType == null || protocolType.isEmpty) protocolType = UmsProtocolType.DATA_INCREMENT_DATA.toString
            ((protocolType, namespace), message.value)
          } else {
            val (protocol, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(message.key)
            ((protocol.toString, namespace), message.value)
          }
        })

        val dataParRdd = if (config.rdd_partition_number != -1) {
          streamTransformedRdd.repartition(config.rdd_partition_number) //.partitionBy(new HashPartitioner(config.rdd_partition_number))
        } else streamTransformedRdd

        val namespace2FileMap: Map[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]] = namespace2FileStore.toMap
        //        val validNameSpaceMap: Map[String, Int] = directiveNamespaceRule.toMap //validNamespaceMap is NOT real namespace, has *
        //        logInfo("validNameSpaceMap:" + validNameSpaceMap)

        val mainDataTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        val partitionResultRdd = dataParRdd.mapPartitionsWithIndex { case (index, partition) =>
          // partition: ((protocol,namespace), message.value)
          val resultList = ListBuffer.empty[PartitionResult]
          val namespaceMap = mutable.HashMap.empty[(String, String), HdfsLogFlowConfig]
          val flowErrorList = mutable.ListBuffer.empty[FlowErrorInfo]
          //real namespace, do not have *
          val dataList = partition.toList
          dataList.foreach { case ((protocolType, sourceNamespace), _) =>
            val result: Map[String, HdfsLogFlowConfig] = checkValidNamespace(sourceNamespace, hdfslogMap)
            if (result.nonEmpty && (protocolType == UmsProtocolType.DATA_INITIAL_DATA.toString ||
              protocolType == UmsProtocolType.DATA_INCREMENT_DATA.toString)) {
              val (_, flowConfig) = result.head
              if (!namespaceMap.contains((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)))
                namespaceMap((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)) = flowConfig
              if (!namespaceMap.contains((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)))
                namespaceMap((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)) = flowConfig
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
                val tmpResult: PartitionResult = doMainData(protocol, namespace, namespaceDataList, config, flowConfig.hourDuration,
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
        writeResult.head._1.foreach(eachResult => {
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

        if (writeResult.head._2.nonEmpty) {
          val flowIdSet = mutable.HashSet.empty[Long]
          writeResult.head._2.foreach(flowErrorInfo => {
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

        val statsProtocolNamespace: Set[(String, String, Long)] = writeResult.head._1.map(r => {
          (r.protocol, r.namespace, r.flowId)
        }).toSet

        statsProtocolNamespace.foreach { case (protocol, namespace, flowId) =>
          var count = 0
          var cdcTs = 0L
          writeResult.head._1.foreach(r => {
            if (protocol == r.protocol && namespace == r.namespace) {
              count += r.allCount
              val tmpMaxTs = if (!r.maxTs.trim.equals("")) DateUtils.dt2date(r.maxTs).getTime else 0L
              if (cdcTs < tmpMaxTs) cdcTs = tmpMaxTs
            }
          })
          val doneTs = System.currentTimeMillis
          if (count > 0 && cdcTs > 0)
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              UmsProtocolUtils.feedbackFlowStats(namespace, protocol, DateUtils.currentDateTime, config.spark_config.stream_id,
                batchId, namespace, topicPartitionOffset.toJSONString,
                count, DateUtils.dt2string(cdcTs, DtFormat.TS_DASH_MILLISEC), rddTs, directiveTs, mainDataTs, mainDataTs, mainDataTs, doneTs.toString, flowId),
              Some(UmsProtocolType.FEEDBACK_FLOW_STATS + "." + flowId), config.kafka_output.brokers)

        }
        partitionResultRdd.unpersist()
      } catch {
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

    //    val filePrefixShardingSlashSplit = filePrefixShardingSlash.split("/")
    //    val length = filePrefixShardingSlashSplit.length
    //    val nodePath = WormholeConstants.CheckpointRootPath + hdfsLog + filePrefixShardingSlashSplit.slice(length - 5, length).mkString("/")
    val processTime = DateUtils.currentyyyyMMddHHmmssmls
    val indexStr = "000" + index

    val incrementalId = processTime + indexStr.substring(indexStr.length - 4, indexStr.length)
    //WormholeZkClient.getNextAtomicIncrement(zookeeperPath, nodePath)
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

  private def getMinMaxTs(message: String, namespace: String, hdfslogMap: Map[String, HdfsLogFlowConfig]) = {
    var currentUmsTsMin: String = ""
    var currentUmsTsMax: String = ""
    val validMap: Map[String, HdfsLogFlowConfig] = checkValidNamespace(namespace, hdfslogMap)
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
                         hdfslogMap: Map[String, HdfsLogFlowConfig],
                         index: Int): PartitionResult = {
    var valid = true
    val namespaceSplit = namespace.split("\\.")
    val namespaceDb = namespaceSplit.slice(0, 3).mkString(".")
    val namespaceTable = namespaceSplit(3)
    val version = namespaceSplit(4)
    val sharding1 = namespaceSplit(5)
    val sharding2 = namespaceSplit(6)
    val filePrefixShardingSlash = config.stream_hdfs_address.get + "/" + "hdfslog" + "/" + namespaceDb.toLowerCase + "/" + namespaceTable.toLowerCase + "/" + version + "/" + sharding1 + "/" + sharding2 + "/" + protocol + "/"
    val index2FileRightMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("right")) {
      namespace2FileMap(protocol, namespace)("right")
    } else null

    val index2FileWrongMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("wrong")) {
      namespace2FileMap(protocol, namespace)("wrong")
    } else null

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
      val configuration = new Configuration()
      val hdfsPath = config.stream_hdfs_address.get
      val hdfsPathGrp = hdfsPath.split("//")
      val hdfsRoot = if (hdfsPathGrp(1).contains("/"))
        hdfsPathGrp(0) + "//" + hdfsPathGrp(1).substring(0, hdfsPathGrp(1).indexOf("/"))
      else hdfsPathGrp(0) + "//" + hdfsPathGrp(1)
      configuration.set("fs.defaultFS", hdfsRoot)
      configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
      if (config.hdfs_namenode_hosts.nonEmpty) {
        val clusterName = hdfsRoot.split("//")(1)
        configuration.set("dfs.nameservices", clusterName)
        configuration.set(s"dfs.ha.namenodes.$clusterName", config.hdfs_namenode_ids.get)
        val namenodeAddressSeq = config.hdfs_namenode_hosts.get.split(",")
        val namenodeIdSeq = config.hdfs_namenode_ids.get.split(",")
        for (i <- 0 until namenodeAddressSeq.length) {
          configuration.set(s"dfs.namenode.rpc-address.$clusterName." + namenodeIdSeq(i), namenodeAddressSeq(i))
        }
        configuration.set(s"dfs.client.failover.proxy.provider.$clusterName", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
      }

      if (config.kerberos && config.hdfslog_server_kerberos.nonEmpty && !config.hdfslog_server_kerberos.get) {
        configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true")
      }

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
          val originalProcessTime = metaContentSplit(length - 2)
          if (DateUtils.dt2timestamp(DateUtils.dt2dateTime(originalProcessTime).plusHours(hour)).compareTo(DateUtils.dt2timestamp(DateUtils.dt2dateTime(DateUtils.currentyyyyMMddHHmmssmls))) < 0) {
            logInfo("获取minTs和maxTs异常，已创建错误文件，但文件已超过创建间隔，需要重新创建")
            setMetaDataFinished(errorMetaName, metaContent, configuration, minTs, finalMinTs, finalMaxTs)
            val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
            errorFileName = name
            currentErrorMetaContent = meta
            errorCurrentSize = 0
          }
        }

        if (minTs != null && correctFileName == null) {
          logInfo("启动后第一次写数据，先创建文件")
          val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
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
            val (meta, name) = createFile(filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath, index)
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
            errorCurrentSize += content.length + splitMarkLength
            inputError.write(content)
            inputError.write(splitMark)
            val indexLastUnderScore = currentErrorMetaContent.lastIndexOf("_")
            currentErrorMetaContent = currentErrorMetaContent.substring(0, indexLastUnderScore + 1) + DateUtils.currentyyyyMMddHHmmssmls
            logInfo("因获取minTs和maxTs异常，将所有数据写入错误文件中")
          } else {
            val errorTuple = writeAndCreateFile(currentErrorMetaContent, errorFileName, configuration, inputError,
              content, minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, index)
            errorFileName = errorTuple._1
            currentErrorMetaContent = errorTuple._2
            errorCurrentSize = errorTuple._3
            logInfo("因获取minTs和maxTs异常，并且错误文件已经达到最大值，先新建错误文件，再将所有数据写入错误文件中")
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
            val correctTuple = writeAndCreateFile(currentCorrectMetaContent, correctFileName, configuration, inputCorrect, content,
              minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, index)
            correctFileName = correctTuple._1
            currentCorrectMetaContent = correctTuple._2
            correctCurrentSize = correctTuple._3
            finalMinTs = minTs
            finalMaxTs = maxTs
            logInfo("文件已经达到最大值，先新建文件")
          }
        }
      })
      val bytesError = inputError.toByteArray
      val bytesCorrect = inputCorrect.toByteArray
      val inError = new ByteArrayInputStream(bytesError)
      val inCorrect = new ByteArrayInputStream(bytesCorrect)
      if (bytesError.nonEmpty) {
        HdfsUtils.appendToFile(configuration, errorFileName, inError)
        val errorMetaName = getMetaName(errorFileName)
        updateMeta(errorMetaName, currentErrorMetaContent, configuration)
        logInfo("存在错误数据，写入错误文件，并更新错误meta文件")
      }
      if (bytesCorrect.nonEmpty) {
        HdfsUtils.appendToFile(configuration, correctFileName, inCorrect)
        val correctMetaName = getMetaName(correctFileName)
        updateMeta(correctMetaName, currentCorrectMetaContent, configuration)
        logInfo("存在解析正确的数据，写入文件，并更新meta文件，共计：" + dataList.size)
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
    val vaildMap: Map[String, HdfsLogFlowConfig] = checkValidNamespace(namespace, hdfslogMap)
    val flowId = if (vaildMap != null && vaildMap.nonEmpty) vaildMap.head._2.flowId else -1

    PartitionResult(index, valid, errorFileName, errorCurrentSize, currentErrorMetaContent, correctFileName,
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


  def checkValidNamespace(namespace: String, validNameSpaceMap: Map[String, HdfsLogFlowConfig]): Map[String, HdfsLogFlowConfig] = {
    validNameSpaceMap.filter { case (rule, _) =>
      val namespaceLowerCase = namespace.toLowerCase
      matchNameSpace(rule, namespaceLowerCase)
    }
  }
}
