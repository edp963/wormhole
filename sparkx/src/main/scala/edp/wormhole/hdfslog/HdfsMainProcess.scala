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


package edp.wormhole.hdfslog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import edp.wormhole.common.WormholeUtils.dataParse
import edp.wormhole.common._
import edp.wormhole.common.util.DateUtils
import edp.wormhole.core.WormholeConfig
import edp.wormhole.kafka.WormholeKafkaProducer
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import edp.wormhole.common.hadoop.HdfsUtils._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.zookeeper.WormholeZkClient
import edp.wormhole.sinks.utils.SinkCommonUtils._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsSchemaUtils._
import edp.wormhole.ums._
import edp.wormhole.ums.UmsSysField._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}
import org.apache.spark.HashPartitioner

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

//fileName:  ../oracle.oracle0.db.table/1/0.0/right/currentTime_minTs_maxTs
//           ../oracle.oracle0.db.table/1/0.0/wrong/minCurrentTime_maxCurrentTime
object HdfsMainProcess extends EdpLogging {
  val namespace2FileStore = mutable.HashMap.empty[String, mutable.HashMap[String, (String, Int, String)]]
  // Map[namespace(accurate to table), HashMap["right", (filename, size,metaContent)]]
  val directiveNamespaceRule = mutable.HashMap.empty[String, Int] //[namespace, hour]
  val jsonSourceMap = mutable.HashMap.empty[String, (Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename, Seq[UmsField])]

  val fileMaxSize = 128
  val metadata = "metadata_"
  val hdfsLog = "hdfslog/"

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig): Unit = {
    val zookeeperPath = config.zookeeper_path
    stream.foreachRDD(foreachFunc = (streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
      try {
        val statsId = UUID.randomUUID().toString
        val rddTs = System.currentTimeMillis
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val directiveTs = System.currentTimeMillis
        HdfsDirective.doDirectiveTopic(config, stream)


        streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)

        val streamTransformedRdd: RDD[(String, (String, String))] = streamRdd.map(message => {
          val (protocol, namespace) = WormholeUtils.getTypeNamespaceFromKafkaKey(message.key)
          (namespace, (protocol.toString, message.value))
        })

        val dataParRdd = if (config.rdd_partition_number != -1) {
          streamTransformedRdd.partitionBy(new HashPartitioner(config.rdd_partition_number))
        } else streamTransformedRdd

        val namespace2FileMap: Map[String, mutable.HashMap[String, (String, Int, String)]] = namespace2FileStore.toMap
        val validNameSpaceMap: Map[String, Int] = directiveNamespaceRule.toMap //validNamespaceMap is NOT real namespace, has *
        val jsonInfoMap: Map[String, (Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename, Seq[UmsField])] = jsonSourceMap.toMap
        val mainDataTs = System.currentTimeMillis
        val partitionResultRdd = dataParRdd.mapPartitions(partition => {
          // partition: namespace, (protocol, message.value)
          val resultList = ListBuffer.empty[(Boolean, String, Int, String, String, Int, String, String, String, String, Int)]
          val namespaceMap = mutable.HashMap.empty[String, Int] //real namespace, do not have *
          val dataList = partition.toList

          dataList.foreach(data => {
            val result: Map[String, Int] = checkValidNamespace(data._1, validNameSpaceMap)
            if (result.nonEmpty && (data._2._1 == UmsProtocolType.DATA_INITIAL_DATA.toString || data._2._1 == UmsProtocolType.DATA_INCREMENT_DATA.toString)) {
              val (_, hour) = result.head
              if (!namespaceMap.contains(data._1)) namespaceMap(data._1) = hour
            }
          })

          namespaceMap.foreach { case (namespace, hour) =>
            val namespaceDataList = ListBuffer.empty[(String, String)]
            dataList.foreach(data => {
              if (data._1 == namespace && (data._2._1 == UmsProtocolType.DATA_INITIAL_DATA.toString || data._2._1 == UmsProtocolType.DATA_INCREMENT_DATA.toString)) namespaceDataList.append((data._2._2, data._2._1 + "." + namespace))
            })
            var tmpMinTs = ""
            var tmpMaxTs = ""
            var tmpCount = 0
            try {
              if (namespaceDataList.nonEmpty) {
                val tmpResult: (Boolean, String, Int, String, String, Int, String, String, String, String, Int) = doMainData(namespace, namespaceDataList, config, hour, namespace2FileMap, zookeeperPath,jsonInfoMap)
                tmpMinTs = tmpResult._9
                tmpMaxTs = tmpResult._10
                tmpCount = tmpResult._11
                resultList += tmpResult
              }
            } catch {
              case e: Throwable =>
                logAlert("batch error", e)
                WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackFlowError(namespace, config.spark_config.stream_id, currentDateTime, "", UmsWatermark(tmpMinTs), UmsWatermark(tmpMaxTs), tmpCount, ""), None, config.kafka_output.brokers)
            }
          }


          resultList.toIterator
        }).cache

        partitionResultRdd.collect.foreach { case (valid, errorFileName, errorCurrentSize, currentErrorMetaContent, correctFileName, correctCurrentSize, currentCorrectMetaContent, namespace, _, maxTs, count)
        =>

          if (!namespace2FileStore.contains(namespace)) namespace2FileStore(namespace) = mutable.HashMap.empty[String, (String, Int, String)]
          if (valid && count > 0 && errorFileName != null) {
            namespace2FileStore(namespace)("wrong") = (errorFileName, errorCurrentSize, currentErrorMetaContent)
          }
          if (valid && count > 0 && correctFileName != null) {
            namespace2FileStore(namespace)("right") = (correctFileName, correctCurrentSize, currentCorrectMetaContent)
          }
          val doneTs = System.currentTimeMillis
          if (count > 0 && maxTs != "")
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
              UmsProtocolUtils.feedbackFlowStats(namespace, UmsProtocolType.DATA_INCREMENT_DATA.toString, currentDateTime, config.spark_config.stream_id, statsId, namespace,
                count, DateUtils.dt2date(maxTs).getTime, rddTs, directiveTs, mainDataTs, mainDataTs, mainDataTs, doneTs), None, config.kafka_output.brokers)
        }
        partitionResultRdd.unpersist()
        WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      } catch {
        case e: Throwable =>
          logAlert("batch error", e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackStreamBatchError(config.spark_config.stream_id, currentDateTime, UmsFeedbackStatus.FAIL, e.getMessage), None, config.kafka_output.brokers)
          WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetInfo.toArray)
    })
  }


  private def createFile(message: String, filePrefixShardingSlash: String, configuration: Configuration, minTs: String, maxTs: String, zookeeperPath: String): (String, String) = {

    val filePrefixShardingSlashSplit = filePrefixShardingSlash.split("/")
    val length = filePrefixShardingSlashSplit.length
    val nodePath = WormholeConstants.CheckpointRootPath + hdfsLog + filePrefixShardingSlashSplit.slice(length - 5, length).mkString("/")
    val processTime = currentyyyyMMddHHmmssmls
    val incrementalId = WormholeZkClient.getNextAtomicIncrement(zookeeperPath, nodePath)
    val metaName = if (minTs == null) filePrefixShardingSlash + "wrong" + "/" + "metadata_" + incrementalId else filePrefixShardingSlash + "right" + "/" + "metadata_" + incrementalId
    val metaContent: String = if (minTs == null) incrementalId + "_" + "0_" + processTime + "_" + processTime else incrementalId + "_" + "0_" + processTime + "_" + minTs + "_" + maxTs
    val dataName = if (minTs == null) filePrefixShardingSlash + "wrong" + "/" + incrementalId else filePrefixShardingSlash + "right" + "/" + incrementalId
    createPath(configuration, metaName)
    createPath(configuration, dataName)
    writeString(metaContent, metaName)
    (metaContent, dataName)
  }

  private def getMinMaxTs(message: String, namespace: String,jsonInfoMap:Map[String, (Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename, Seq[UmsField])]) = {
    var currentUmsTsMin: String = ""
    var currentUmsTsMax: String = ""
    if (jsonInfoMap.contains(namespace)) {
      val mapValue = jsonInfoMap(namespace)
      val value: Seq[UmsTuple] = dataParse(message, mapValue._1, mapValue._2)
      val schema = mapValue._4
      val umsTsIndex = schema.map(_.name).indexOf(mapValue._3.umsSysTs)
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
    (yyyyMMddHHmmssmls(currentUmsTsMin), yyyyMMddHHmmssmls(currentUmsTsMax))
  }


  private def writeAndCreateFile(currentMetaContent: String, fileName: String, configuration: Configuration, input: ByteArrayOutputStream, content: Array[Byte], message: String, minTs: String, maxTs: String, finalMinTs: String, finalMaxTs: String, splitMark: Array[Byte], zookeeperPath: String, protocolNsBytes: Array[Byte]) = {
    val metaName = getMetaName(fileName)
    setMetaDataFinished(metaName, currentMetaContent, configuration, minTs, finalMinTs, finalMaxTs)
    val bytes = input.toByteArray
    val in = new ByteArrayInputStream(bytes)
    appendToFile(configuration, fileName, in)
    input.reset()
    var currentSize = 0

    val slashPosition = fileName.lastIndexOf("/")
    val filePrefix = fileName.substring(0, slashPosition + 1)
    val filePrefixShardingSlash = filePrefix.substring(0, filePrefix.length - 6)
    val (newMeta, newFileName) = createFile(message, filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath)
    currentSize += content.length + splitMark.length + protocolNsBytes.length
    input.write(protocolNsBytes)
    input.write(content)
    input.write(splitMark)
    (newFileName, newMeta, currentSize)
  }

  private def doMainData(namespace: String, dataList: Seq[(String, String)], config: WormholeConfig, hour: Int, namespace2FileMap: Map[String, mutable.HashMap[String, (String, Int, String)]], zookeeperPath: String,jsonInfoMap: Map[String, (Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename, Seq[UmsField])]) = {
    var valid = true
    val namespaceSplit = namespace.split("\\.")
    val namespaceDb = namespaceSplit.slice(0, 3).mkString(".")
    val namespaceTable = namespaceSplit(3)
    val version = namespaceSplit(4)
    val sharding1 = namespaceSplit(5)
    val sharding2 = namespaceSplit(6)
    val filePrefixShardingSlash = config.stream_hdfs_address.get + "/" + "hdfslog" + "/" + namespaceDb.toLowerCase + "/" + namespaceTable.toLowerCase + "/" + version + "/" + sharding1 + "/" + sharding2 + "/"
    var correctFileName: String = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("right")) namespace2FileMap(namespace)("right")._1 else null
    var correctCurrentSize: Int = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("right")) namespace2FileMap(namespace)("right")._2 else 0
    var errorFileName: String = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("wrong")) namespace2FileMap(namespace)("wrong")._1 else null
    var errorCurrentSize: Int = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("wrong")) namespace2FileMap(namespace)("wrong")._2 else 0

    var currentErrorMetaContent: String = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("wrong")) namespace2FileMap(namespace)("wrong")._3 else null
    var currentCorrectMetaContent: String = if (namespace2FileMap.contains(namespace) && namespace2FileMap(namespace).contains("right")) namespace2FileMap(namespace)("right")._3 else null

    var finalMinTs: String = if (currentCorrectMetaContent == null) "" else currentCorrectMetaContent.split("_")(3)
    var finalMaxTs: String = if (currentCorrectMetaContent == null) "" else currentCorrectMetaContent.split("_")(4)
    val count = dataList.size
    val inputCorrect = new ByteArrayOutputStream()
    val inputError = new ByteArrayOutputStream()
    try {
      val configuration = new Configuration()
      val hdfsPath = config.stream_hdfs_address.get
      val hdfsPathGrp = hdfsPath.split("//")
      val hdfsRoot = if (hdfsPathGrp(1).contains("/")) hdfsPathGrp(0) + "//" + hdfsPathGrp(1).substring(0, hdfsPathGrp(1).indexOf("/")) else hdfsPathGrp(0) + "//" + hdfsPathGrp(1)
      configuration.set("fs.defaultFS", hdfsRoot)
      configuration.setBoolean("fs.hdfs.impl.disable.cache", true)

      dataList.foreach { case (message, protocolNs) => {
        val protocolNsBytes = protocolNs.getBytes()
        val protocolNsBytesLength = protocolNsBytes.length
        val splitMark = "\n".getBytes()
        val splitMarkLength = splitMark.length
        var minTs: String = null
        var maxTs: String = null
        try {
          val timePair = getMinMaxTs(message, namespace,jsonInfoMap)
          minTs = timePair._1
          maxTs = timePair._2
        } catch {
          case NonFatal(e) => logError(s"message convert failed:\n$message", e)
        }

        if (minTs == null && errorFileName == null) {
          val (meta, name) = createFile(message, filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath)
          errorFileName = name
          currentErrorMetaContent = meta
          errorCurrentSize = 0
        }

        if (minTs == null && errorFileName != null) {
          val errorMetaName = getMetaName(errorFileName)
          val metaContent = currentErrorMetaContent
          val metaContentSplit = metaContent.split("_")
          val length = metaContentSplit.length
          val originalProcessTime = metaContentSplit(length - 2)
          if (dt2timestamp(dt2dateTime(originalProcessTime).plusHours(hour)).compareTo(dt2timestamp(dt2dateTime(currentyyyyMMddHHmmssmls))) < 0) {
            setMetaDataFinished(errorMetaName, metaContent, configuration, minTs, finalMinTs, finalMaxTs)
            val (meta, name) = createFile(message, filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath)
            errorFileName = name
            currentErrorMetaContent = meta
            errorCurrentSize = 0
          }
        }

        if (minTs != null && correctFileName == null) {
          val (meta, name) = createFile(message, filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath)
          correctFileName = name
          currentCorrectMetaContent = meta
          correctCurrentSize = 0
        }

        if (minTs != null && correctFileName != null) {
          val correctMetaName = getMetaName(correctFileName)
          val metaContent = currentCorrectMetaContent
          val metaContentSplit = metaContent.split("_")
          val length = metaContentSplit.length
          val originalProcessTime = metaContentSplit(length - 3)
          if (dt2timestamp(dt2dateTime(originalProcessTime).plusHours(hour)).compareTo(dt2timestamp(dt2dateTime(currentyyyyMMddHHmmssmls))) < 0) {
            setMetaDataFinished(correctMetaName, metaContent, configuration, minTs, finalMinTs, finalMaxTs)
            val (meta, name) = createFile(message, filePrefixShardingSlash, configuration, minTs, maxTs, zookeeperPath)
            correctFileName = name
            currentCorrectMetaContent = meta
            correctCurrentSize = 0
          }
        }

        val content = message.getBytes(StandardCharsets.UTF_8)

        if (minTs == null) {
          if (protocolNsBytesLength + errorCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) { //60 * 1024 * 1024 Bytes = 60MB
            errorCurrentSize += content.length + splitMarkLength + protocolNsBytesLength
            inputError.write(protocolNsBytes)
            inputError.write(content)
            inputError.write(splitMark)
            val indexLastUnderScore = currentErrorMetaContent.lastIndexOf("_")
            currentErrorMetaContent = currentErrorMetaContent.substring(0, indexLastUnderScore + 1) + currentyyyyMMddHHmmssmls
          } else {
            val errorTuple = writeAndCreateFile(currentErrorMetaContent, errorFileName, configuration, inputError, content, message, minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, protocolNsBytes)
            errorFileName = errorTuple._1
            currentErrorMetaContent = errorTuple._2
            errorCurrentSize = errorTuple._3
          }
        } else {
          if (protocolNsBytesLength + correctCurrentSize + content.length + splitMarkLength < fileMaxSize * 1024 * 1024) {
            correctCurrentSize += content.length + splitMarkLength + protocolNsBytesLength
            inputCorrect.write(protocolNsBytes)
            inputCorrect.write(content)
            inputCorrect.write(splitMark)
            finalMinTs = if (finalMinTs.isEmpty || firstTimeAfterSecond(finalMinTs, minTs)) minTs else finalMinTs
            finalMaxTs = if (finalMaxTs.isEmpty || firstTimeAfterSecond(maxTs, finalMaxTs)) maxTs else finalMaxTs
            val currentCorrectMetaContentSplit = currentCorrectMetaContent.split("_")
            currentCorrectMetaContent = currentCorrectMetaContentSplit(0) + "_0_" + currentCorrectMetaContentSplit(2) + "_" + finalMinTs + "_" + finalMaxTs
          } else {
            val correctTuple = writeAndCreateFile(currentCorrectMetaContent, correctFileName, configuration, inputCorrect, content, message, minTs, maxTs, finalMinTs, finalMaxTs, splitMark, zookeeperPath, protocolNsBytes)
            correctFileName = correctTuple._1
            currentCorrectMetaContent = correctTuple._2
            correctCurrentSize = correctTuple._3
            finalMinTs = minTs
            finalMaxTs = maxTs
          }
        }
      }
      }
      val bytesError = inputError.toByteArray
      val bytesCorrect = inputCorrect.toByteArray
      val inError = new ByteArrayInputStream(bytesError)
      val inCorrect = new ByteArrayInputStream(bytesCorrect)
      if (bytesError.nonEmpty) {
        appendToFile(configuration, errorFileName, inError)
        val errorMetaName = getMetaName(errorFileName)
        updateMeta(errorMetaName, currentErrorMetaContent, configuration)
      }
      if (bytesCorrect.nonEmpty) {
        appendToFile(configuration, correctFileName, inCorrect)
        val correctMetaName = getMetaName(correctFileName)
        updateMeta(correctMetaName, currentCorrectMetaContent, configuration)
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

    (valid, errorFileName, errorCurrentSize, currentErrorMetaContent, correctFileName, correctCurrentSize, currentCorrectMetaContent, namespace, finalMinTs, finalMaxTs, count)
  }

  def setMetaDataFinished(metaName: String, currentMetaContent: String, configuration: Configuration, minTs: String, finalMinTs: String, finalMaxTs: String): Unit = {
    var newMetaContent: String = null
    val splitCurrentMetaContent = currentMetaContent.split("_")
    if (minTs != null) {
      newMetaContent = splitCurrentMetaContent(0) + "_1_" + splitCurrentMetaContent(2) + "_" + finalMinTs + "_" + finalMaxTs
    } else {
      newMetaContent = splitCurrentMetaContent(0) + "_1_" + splitCurrentMetaContent(2) + "_" + currentyyyyMMddHHmmssmls
    }
    writeString(newMetaContent, metaName)
  }

  def updateMeta(metaName: String, metaContent: String, configuration: Configuration): Unit = {
    writeString(metaContent, metaName)
  }

  def getMetaName(fileName: String): String = {
    val incrementalId = fileName.substring(fileName.lastIndexOf("/") + 1).trim
    fileName.substring(0, fileName.lastIndexOf("/") + 1) + metadata + incrementalId
  }


  def matchNameSpace(rule: String, namespace: String): Boolean = {
    val regex = rule.replace(".", "\\.") replace("*", ".*")
    namespace.matches(regex)
  }


  def checkValidNamespace(namespace: String, validNameSpaceMap: Map[String, Int]): Map[String, Int] = {
    validNameSpaceMap.filter { case (rule, _) =>
      val namespaceLowerCase = namespace.toLowerCase
      matchNameSpace(rule, namespaceLowerCase)
    }
  }
}


