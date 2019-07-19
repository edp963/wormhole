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


package edp.wormhole.sparkx.hdfs.hdfscsv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.common.feedback.{ErrorPattern, FeedbackPriority}
import edp.wormhole.common.json.JsonParseUtils
import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sinks.utils.SinkCommonUtils.firstTimeAfterSecond
import edp.wormhole.sparkx.common._
import edp.wormhole.sparkx.hdfs.{HdfsDirective, HdfsFlowConfig, PartitionResult}
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums._
import edp.wormhole.util.{DateUtils, DtFormat, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.KafkaException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

//fileName:  ../oracle.oracle0.db.table/1/0/0/data_increment_data(data_initial_data)/right(wrong)/currentyyyyMMddHHmmss0740（文件编号4位，左补零）
//metaFile   ../oracle.oracle0.db.table/1/metadata
//metaContent  schema:    {"fields": [{"name": "ums_id_","type": "long","nullable": false},{"name": "ums_ts_","type": "datetime","nullable": false},{"name": "ums_op_","type": "string","nullable": false},{"name": "key","type": "int","nullable": false},{"name": "value1","type": "string","nullable": true},{"name": "value2","type": "long","nullable": false}]}

object HdfsCsvMainProcess extends EdpLogging {

  val namespace2FileStore = mutable.HashMap.empty[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]]

  var schemaFlag = false

  val fileMaxSize = 128
  val schema = "schema"
  val hdfscsv = "hdfscsv/"
  val rightFlag = "right"
  val wrongFlag = "wrong"

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, session: SparkSession, appId: String, kafkaInput: KafkaInputConfig, ssc: StreamingContext): Unit = {
    var zookeeperFlag = false
    stream.foreachRDD(foreachFunc = (streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val batchId = UUID.randomUUID().toString

      val offsetInfo: ArrayBuffer[OffsetRange] = getOffsetInfo(streamRdd)
      val topicPartitionOffset: JSONObject = SparkUtils.getTopicPartitionOffset(offsetInfo)

      val hdfscsvMap: Map[String, HdfsFlowConfig] = ConfMemoryStorage.getHdfscsvMap

      try {
        val rddTs: String = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val directiveTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)
        HdfsDirective.doDirectiveTopic(config, stream)

        logInfo(s"config.rdd_partition_number ${config.rdd_partition_number}")
        val dataParRdd: RDD[((String, String), String)] = formatRDD(config.rdd_partition_number, streamRdd)

        val mainDataTs = DateUtils.dt2string(DateUtils.currentDateTime, DtFormat.TS_DASH_MILLISEC)

        val namespace2FileMap = namespace2FileStore.toMap
        val partitionResultRdd: RDD[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])] = dataParRdd.mapPartitionsWithIndex { case (index, partition) =>
          val resultList = ListBuffer.empty[PartitionResult]
          val namespaceMap = mutable.HashMap.empty[(String, String), HdfsFlowConfig]
          val flowErrorList = mutable.ListBuffer.empty[FlowErrorInfo]

          val dataList = partition.toList
          dataList.foreach { case ((protocolType, sourceNamespace), _) =>
            namespaceMap ++= uniqueProtocolNamespaceConfig(namespaceMap, sourceNamespace, protocolType, hdfscsvMap)
          }
          logInfo("check namespace ok. all data num=" + dataList.size + ",namespaceMap=" + namespaceMap)

          namespaceMap.foreach { case ((protocol, namespace), flowConfig) =>
            val namespaceDataList = ListBuffer.empty[String]
            dataList.foreach(data => {
              if (data._1._1 == protocol && data._1._2 == namespace) namespaceDataList.append(data._2)
            })
            logInfo("protocol=" + protocol + ",namespace=" + namespace + ",data num=" + namespaceDataList.size)

            try {
              if (namespaceDataList.nonEmpty) {
                val tmpResult: PartitionResult = doMainData(protocol, namespace, namespaceDataList, config, flowConfig.hourDuration,
                  namespace2FileMap, config.zookeeper_path, hdfscsvMap, index)
                resultList += tmpResult
              }
            } catch {
              case e: Throwable =>
                logAlert("sink,sourceNamespace=" + namespace, e)
                flowErrorList.append(FlowErrorInfo(flowConfig.flowId, protocol, namespace, namespace, e, ErrorPattern.FlowError,
                  flowConfig.incrementTopics, -1))
            }
          }
          val res = ListBuffer.empty[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])]
          res.append((resultList, flowErrorList))
          res.toIterator
        }.cache

        val writeResult: Array[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])] = partitionResultRdd.collect
        logInfo("writeResult.size:" + writeResult.length)

        updateNamespace2FileStore(writeResult)
        logInfo("end writeResult")

        feedbackError(writeResult, topicPartitionOffset, config, batchId)

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
          if (count > 0 && cdcTs > 0) {
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              UmsProtocolUtils.feedbackFlowStats(namespace, protocol, DateUtils.currentDateTime, config.spark_config.stream_id,
                batchId, namespace, topicPartitionOffset.toJSONString,
                count, DateUtils.dt2string(cdcTs, DtFormat.TS_DASH_MILLISEC), rddTs, directiveTs, mainDataTs, mainDataTs, mainDataTs, doneTs.toString, flowId),
              Some(UmsProtocolType.FEEDBACK_FLOW_STATS + "." + flowId), config.kafka_output.brokers)
          }
          logInfo("finish one stat")

        }
        logInfo("finish stat ")
        partitionResultRdd.unpersist()
      } catch {
        case e: KafkaException =>
          logError("kafka consumer error," + e.getMessage, e)
          if (e.getMessage.contains("Failed to construct kafka consumer")) {
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
          hdfscsvMap.foreach { case (sourceNamespace, flowConfig) =>
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

  private def feedbackError(writeResult: Array[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])],
                            topicPartitionOffset: JSONObject, config: WormholeConfig, batchId: String): Unit = {
    val flowIdSet = mutable.HashSet.empty[Long]
    writeResult.foreach(eachPartionResultError => {
      logInfo("eachPartionError.size:" + eachPartionResultError._2.size)
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
  }

  private def updateNamespace2FileStore(writeResult: Array[(ListBuffer[PartitionResult], ListBuffer[FlowErrorInfo])]): Unit = {
    writeResult.foreach(eachPartionResultError => {
      logInfo("eachPartionResult.size:" + eachPartionResultError._1.size)
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
  }

  private def uniqueProtocolNamespaceConfig(namespaceMap: mutable.HashMap[(String, String), HdfsFlowConfig], sourceNamespace: String, protocolType: String, hdfscsvMap: Map[String, HdfsFlowConfig]): mutable.HashMap[(String, String), HdfsFlowConfig] = {
    val result: Map[String, HdfsFlowConfig] = checkValidNamespace(sourceNamespace, hdfscsvMap)
    if (result.nonEmpty && (protocolType == UmsProtocolType.DATA_INITIAL_DATA.toString || protocolType == UmsProtocolType.DATA_INCREMENT_DATA.toString)) {
      val (_, flowConfig) = result.head
      if (!namespaceMap.contains((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)))
        namespaceMap((UmsProtocolType.DATA_INITIAL_DATA.toString, sourceNamespace)) = flowConfig
      if (!namespaceMap.contains((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)))
        namespaceMap((UmsProtocolType.DATA_INCREMENT_DATA.toString, sourceNamespace)) = flowConfig
    }
    namespaceMap
  }

  private def getOffsetInfo(streamRdd: RDD[ConsumerRecord[String, String]]): ArrayBuffer[OffsetRange] = {
    val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
    streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
    offsetInfo
  }

  private def formatRDD(partitionNum: Int, streamRdd: RDD[ConsumerRecord[String, String]]): RDD[((String, String), String)] = {
    val dataParRdd: RDD[((String, String), String)] = if (partitionNum != -1) streamRdd.map(row => {
      (UmsCommonUtils.checkAndGetProtocolNamespace(row.key, row.value), row.value)
    }).repartition(partitionNum)
    else streamRdd.map(row => {
      (UmsCommonUtils.checkAndGetProtocolNamespace(row.key, row.value), row.value)
    })
    dataParRdd
  }


  private def createFile(filePrefixShardingSlash: String, timestamp: String, configuration: Configuration, index: Int, fileType: String): String = {

    val filename = if (timestamp == null || timestamp.isEmpty) {
      DateUtils.currentyyyyMMddHHmmssmls
    } else {
      DateUtils.yyyyMMddHHmmssmls(timestamp)
    }

    val r = Random.nextInt(10000)

    val dataName = filePrefixShardingSlash + fileType + "/" + filename + "_" + r + "_" + index
    logInfo("dataName:" + dataName)
    HdfsUtils.createPath(configuration, dataName)
    dataName
  }

  private def appendToFile(fileName: String, configuration: Configuration, input: ByteArrayOutputStream): Unit = {
    val bytes = input.toByteArray
    val in = new ByteArrayInputStream(bytes)
    HdfsUtils.appendToFile(configuration, fileName, in)
    input.reset()

  }

  private def getIndex2FileMap(namespace2FileMap: Map[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]],
                               protocol: String,
                               namespace: String):
  (mutable.Map[Int, (String, Int, String)], mutable.Map[Int, (String, Int, String)]) = {
    val index2FileRightMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("right")) {
      namespace2FileMap(protocol, namespace)("right")
    } else null

    val index2FileWrongMap: mutable.Map[Int, (String, Int, String)] = if (namespace2FileMap.contains((protocol, namespace)) &&
      namespace2FileMap((protocol, namespace)).contains("wrong")) {
      namespace2FileMap(protocol, namespace)("wrong")
    } else null
    (index2FileRightMap, index2FileWrongMap)
  }

  private def getHadoopConfiguration(config: WormholeConfig): Configuration = {
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
      configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
      configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
      configuration.set(s"dfs.client.failover.proxy.provider.$clusterName", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    }

    if (config.kerberos && config.hdfslog_server_kerberos.nonEmpty && !config.hdfslog_server_kerberos.get) {
      configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true")
    }

    configuration
  }

  def parseData(data: String, hdfsFlowConfig: HdfsFlowConfig): ListBuffer[String] = {
    val dataList = ListBuffer.empty[String]

    if (DataTypeEnum.UMS_EXTENSION.toString == hdfsFlowConfig.dataType) {
      val umsTupleList = JsonParseUtils.dataParse(data, hdfsFlowConfig.jsonSchema.fieldsInfo, hdfsFlowConfig.jsonSchema.twoFieldsArr)
      umsTupleList.foreach((umsTuple: UmsTuple) => {
        dataList += JsonUtils.caseClass2json[Seq[String]](umsTuple.tuple)
      })
    } else {
      val jsonObj: JSONObject = JSON.parseObject(data)
      if (jsonObj.containsKey("payload")) {
        val payloadJsonArr = jsonObj.getJSONArray("payload")

        for (i <- 0 until payloadJsonArr.size()) {
          val tuple: JSONObject = payloadJsonArr.get(i).asInstanceOf[JSONObject]
          val tupleStr: String = tuple.getString("tuple")
          dataList += tupleStr
        }
      }
    }
    dataList
  }

  private def checkAndSetSchema(schemaFilePath: String, data: String, configuration: Configuration, hdfsFlowConfig: HdfsFlowConfig): Boolean = {
    try {
      if (!HdfsUtils.isPathExist(configuration, schemaFilePath)) {
        val fields = if (DataTypeEnum.UMS_EXTENSION.toString == hdfsFlowConfig.dataType) {
          val ja = new JSONArray()
          hdfsFlowConfig.jsonSchema.schemaField.foreach(uf => {
            val jo = new JSONObject()
            jo.put("name", uf.name)
            jo.put("type", uf.`type`.toString)
            jo.put("nullable", uf.nullable.get)
            ja.add(jo)
          })

          val json = ja.toJSONString
          logInfo("schema:" + json)
          json
        } else {
          val jsonObj: JSONObject = JSON.parseObject(data)
          val schemaJSON = jsonObj.getJSONObject("schema")
          val json = schemaJSON.getString("fields")
          logInfo("schema:" + json)
          json
        }

        HdfsUtils.writeString(configuration, fields, schemaFilePath)
      }
      true
    } catch {
      case e: Throwable =>
        logError("get schema error", e)
        false
    }
  }

  private def getUmsTsIndex(data: String, hdfsFlowConfig: HdfsFlowConfig): Int = {
    var umsTsIndex = 0
    if (DataTypeEnum.UMS_EXTENSION.toString == hdfsFlowConfig.dataType) {
      for (i <- hdfsFlowConfig.jsonSchema.schemaField.indices) {
        if (hdfsFlowConfig.jsonSchema.schemaField(i).name == UmsSysField.TS.toString) umsTsIndex = i
      }
    } else {
      val jsonObj: JSONObject = JSON.parseObject(data)
      val schemaJSON = jsonObj.getJSONObject("schema")
      val jsonA = schemaJSON.getJSONArray("fields")
      for(i <- 0 until jsonA.size()){
        val jsonO = jsonA.getJSONObject(i)
        if(jsonO.getString("name")==UmsSysField.TS.toString)umsTsIndex = i
      }
    }
    umsTsIndex
  }

  private def doMainData(protocol: String,
                         namespace: String,
                         dataList: Seq[String],
                         config: WormholeConfig,
                         hour: Int,
                         namespace2FileMap: Map[(String, String), mutable.HashMap[String, mutable.HashMap[Int, (String, Int, String)]]],
                         zookeeperPath: String,
                         hdfscsvMap: Map[String, HdfsFlowConfig],
                         index: Int): PartitionResult = {
    var valid = true
    val vaildMap: Map[String, HdfsFlowConfig] = checkValidNamespace(namespace, hdfscsvMap)
    val hdfsFlowConfig: HdfsFlowConfig = vaildMap.head._2
    logInfo("vaildMap:" + vaildMap)
    val flowId = if (vaildMap != null && vaildMap.nonEmpty) hdfsFlowConfig.flowId else -1

    val (filePrefixShardingSlash, schemaFilePath) = getFilePrefixShardingSlash(namespace, config, protocol)

    val (index2FileRightMap, index2FileWrongMap) = getIndex2FileMap(namespace2FileMap, protocol, namespace)

    var (correctFileName, correctCurrentSize, currentCorrectMetaContent) = if (index2FileRightMap != null && index2FileRightMap.contains(index))
      (index2FileRightMap(index)._1, index2FileRightMap(index)._2, index2FileRightMap(index)._3) else (null, 0, null)

    var (errorFileName, errorCurrentSize, currentErrorMetaContent) = if (index2FileWrongMap != null && index2FileWrongMap.contains(index))
      (index2FileWrongMap(index)._1, index2FileWrongMap(index)._2, index2FileWrongMap(index)._3) else (null, 0, null)

    val count = dataList.size
    val inputCorrect = new ByteArrayOutputStream()
    val inputError = new ByteArrayOutputStream()

    var minTs = ""
    var maxTs = ""

    try {
      val configuration = getHadoopConfiguration(config)
      logInfo(s"configuration:$configuration")
      logInfo(s"config:$config")

      if (dataList.nonEmpty) {
        if (schemaFlag && index == 1) schemaFlag = checkAndSetSchema(schemaFilePath, dataList.head, configuration, hdfsFlowConfig)

        val umsTsIndex = getUmsTsIndex(dataList.head, hdfsFlowConfig)

        dataList.foreach((data: String) => {
          val splitMark = "\n".getBytes()
          val splitMarkLength = splitMark.length

          try {
            val tupleList: mutable.Seq[String] = parseData(data, hdfsFlowConfig)
            tupleList.foreach((tuple: String) => {

              val tmpJA = JSON.parseArray(tuple)
              //            logInfo("tmpJA:"+tmpJA)
              val umsTs = tmpJA.getString(umsTsIndex)
              if (minTs == "") {
                minTs = umsTs
                maxTs = umsTs
              } else {
                maxTs = if (firstTimeAfterSecond(umsTs, maxTs)) umsTs else maxTs
                minTs = if (firstTimeAfterSecond(minTs, umsTs)) umsTs else minTs
              }

              val content = tuple.getBytes(StandardCharsets.UTF_8)
              if (correctCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) {
                correctCurrentSize += content.length + splitMarkLength
                inputCorrect.write(content)
                inputCorrect.write(splitMark)
              } else {
                logInfo("正确数据缓存满，写入文件")
                if (correctFileName == null) {
                  correctFileName = createFile(filePrefixShardingSlash, minTs, configuration, index, rightFlag)
                }
                appendToFile(correctFileName, configuration, inputCorrect)
                correctFileName = null
                inputCorrect.reset()
                inputCorrect.write(content)
                inputCorrect.write(splitMark)
                correctCurrentSize = content.length + splitMarkLength
                logInfo("end 正确数据缓存满，写入文件")
              }
            })
          } catch {
            case e: Throwable =>
              logError("parse data error", e)
              val content = data.getBytes(StandardCharsets.UTF_8)
              if (errorCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) {
                errorCurrentSize += content.length + splitMarkLength
                inputError.write(content)
                inputError.write(splitMark)
              } else {
                logInfo("错误数据缓存满，写入文件")
                if (errorFileName == null) {
                  errorFileName = createFile(filePrefixShardingSlash, minTs, configuration, index, wrongFlag)
                }
                appendToFile(errorFileName, configuration, inputError)
                errorFileName = null
                inputError.reset()
                inputError.write(content)
                inputError.write(splitMark)
                errorCurrentSize = content.length + splitMarkLength
                logInfo("end 错误数据缓存满，写入文件")
              }
          }
        })

        val bytesError = inputError.toByteArray
        val inError = new ByteArrayInputStream(bytesError)
        if (bytesError.nonEmpty) {
          if (errorFileName == null) {
            errorFileName = createFile(filePrefixShardingSlash, minTs, configuration, index, wrongFlag)
          }
          logInfo("存在错误数据，写入错误文件")
          HdfsUtils.appendToFile(configuration, errorFileName, inError)
          logInfo("end appendToFile 存在错误数据，写入错误文件")
        }

        val bytesCorrect = inputCorrect.toByteArray
        val inCorrect = new ByteArrayInputStream(bytesCorrect)
        if (bytesCorrect.nonEmpty) {
          if (correctFileName == null) {
            correctFileName = createFile(filePrefixShardingSlash, minTs, configuration, index, rightFlag)
          }
          logInfo("存在解析正确的数据，写入文件，共计：" + dataList.size)
          HdfsUtils.appendToFile(configuration, correctFileName, inCorrect)
          logInfo("end appendToFile 存在解析正确的数据，写入文件，共计：" + dataList.size)
        }
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

    logInfo(s"minTs:$minTs,maxTs:$maxTs")
    PartitionResult(index, valid, errorFileName, errorCurrentSize, currentErrorMetaContent, correctFileName,
      correctCurrentSize, currentCorrectMetaContent, protocol, namespace, minTs, maxTs, count, flowId)
  }

  private def getFilePrefixShardingSlash(namespace: String, config: WormholeConfig, protocol: String): (String, String) = {
    val namespaceSplit = namespace.split("\\.")
    val namespaceDb = namespaceSplit.slice(0, 3).mkString(".")
    val namespaceTable = namespaceSplit(3)
    val version = namespaceSplit(4)
    val sharding1 = namespaceSplit(5)
    val sharding2 = namespaceSplit(6)
    val filePrefixShardingSlash = config.stream_hdfs_address.get + "/" + hdfscsv + "/" + namespaceDb.toLowerCase + "/" + namespaceTable.toLowerCase + "/" + version + "/" + sharding1 + "/" + sharding2 + "/" + protocol + "/"
    val schemaFilePath = config.stream_hdfs_address.get + "/" + hdfscsv + "/" + namespaceDb.toLowerCase + "/" + namespaceTable.toLowerCase + "/" + version + "/" + schema
    (filePrefixShardingSlash, schemaFilePath)
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
