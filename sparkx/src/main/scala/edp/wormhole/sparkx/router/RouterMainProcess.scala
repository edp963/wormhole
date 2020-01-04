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


package edp.wormhole.sparkx.router

import java.util.UUID

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.feedback.ErrorPattern
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.common._
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{KafkaInputConfig, WormholeConfig}
import edp.wormhole.ums._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.KafkaException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RouterMainProcess extends EdpLogging {

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, session: SparkSession, appId: String, kafkaInput: KafkaInputConfig,ssc: StreamingContext): Unit = {
    var zookeeperFlag = false
    stream.foreachRDD((streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val batchId = UUID.randomUUID().toString
      val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
      streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
      val topicPartitionOffset = SparkUtils.getTopicPartitionOffset(offsetInfo)
      val routerMap = ConfMemoryStorage.getRouterMap
      try {
        logInfo("start foreachRDD")
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())

        logInfo("start doDirectiveTopic")
        RouterDirective.doDirectiveTopic(config, stream)

        logInfo("start Repartition")

        val routerKeys = ConfMemoryStorage.getRouterKeys
        val dataRepartitionRdd: RDD[(String, String)] =
          if (config.rdd_partition_number != -1) streamRdd.map(row => {
            val rowKey = SparkxUtils.getDefaultKey(row.key, routerKeys, SparkxUtils.getDefaultKeyConfig(config.special_config))
            (UmsCommonUtils.checkAndGetKey(rowKey, row.value), row.value)
          }).repartition(config.rdd_partition_number)
          else {
            streamRdd.map(row => {
              val rowKey = SparkxUtils.getDefaultKey(row.key, routerKeys, SparkxUtils.getDefaultKeyConfig(config.special_config))
              (UmsCommonUtils.checkAndGetKey(rowKey, row.value), row.value)
            })
          }

        val errorFlows = dataRepartitionRdd.mapPartitions { partition =>
          routerMap.foreach { case (_, (map, _)) =>
            map.foreach { case (_, routerFlowConfig) =>
              WormholeKafkaProducer.initWithoutAcksAll(routerFlowConfig.brokers, None, config.kafka_output.kerberos)
            }
          }

          //flowid,protocoltype,matchsourcenamespace,sinknamespace,errormsg,errorpattern,incrementTopicList
          val flowErrorList = mutable.ListBuffer.empty[FlowErrorInfo]

          partition.foreach { case (key, value) =>
            val keys: Array[String] = key.split("\\.")
            val (protocolType, sourceNamespace) = if (keys.length > 7) (keys(0).toLowerCase, keys.slice(1, 8).mkString(".")) else (keys(0).toLowerCase, "")
            val sinkNamespaceMap = mutable.HashMap.empty[String, String]

            val matchNamespace = (sourceNamespace.split("\\.").take(4).mkString(".") + ".*.*.*").toLowerCase()
            if (ConfMemoryStorage.existNamespace(routerKeys, matchNamespace)) {
              try {
                if (routerMap(matchNamespace)._2 == "ums") {
                  logInfo("start process namespace: " + matchNamespace)
                  val messageIndex = value.lastIndexOf(sourceNamespace)
                  val prefix = value.substring(0, messageIndex)
                  val suffix = value.substring(messageIndex + sourceNamespace.length)
                  routerMap(matchNamespace)._1.foreach { case (sinkNamespace, routerFlowConfig) =>
                    try {
                      if (!sinkNamespaceMap.contains(sinkNamespace)) sinkNamespaceMap(sinkNamespace) = getNewSinkNamespace(keys, sinkNamespace)
                      val newSinkNamespace = sinkNamespaceMap(sinkNamespace)
                      val messageBuf = new StringBuilder
                      messageBuf ++= prefix ++= newSinkNamespace ++= suffix
                      val kafkaMessage = messageBuf.toString
                      WormholeKafkaProducer.sendMessage(routerFlowConfig.topic, kafkaMessage, Some(protocolType + "." + newSinkNamespace + "..." + UUID.randomUUID().toString), routerFlowConfig.brokers)
                    } catch {
                      case e: Throwable =>
                        logAlert(value, e)
                        flowErrorList.append(FlowErrorInfo(routerFlowConfig.flowId, protocolType, matchNamespace, sinkNamespace, e, ErrorPattern.FlowError, routerFlowConfig.incrementTopics, 1))
                    }
                  }
                } else {
                  routerMap(matchNamespace)._1.foreach { case (sinkNamespace, routerFlowConfig) =>
                    WormholeKafkaProducer.sendMessage(routerFlowConfig.topic, value, Some(protocolType + "." + sinkNamespace + "..." + UUID.randomUUID().toString), routerFlowConfig.brokers)
                  }
                }
              } catch {
                case e: Throwable =>
                  logAlert(value, e)
                  routerMap(matchNamespace)._1.foreach { case (sinkNamespace, routerFlowConfig) =>
                    flowErrorList.append(FlowErrorInfo(routerFlowConfig.flowId, protocolType, matchNamespace, sinkNamespace, e, ErrorPattern.FlowError, routerFlowConfig.incrementTopics, 1))
                  }
              }
            }
          }
          flowErrorList.toIterator
        }.collect()

        if (errorFlows.nonEmpty) {
          val flowIdSet = mutable.HashSet.empty[Long]
          errorFlows.foreach(flowErrorInfo => {
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

      } catch {
        case e: KafkaException=>
          logError("kafka consumer error,"+e.getMessage, e)
          if(e.getMessage.contains("Failed to construct kafka consumer")){
            logError("kafka consumer error ,stop spark streaming")
            stream.stop()

            throw e
          }
        case e: Throwable =>
          logAlert("batch error", e)
          routerMap.foreach { case (sourceNamespace, outV) =>
            outV._1.foreach { case (sinkNamespace, routerFlowConfig) =>
              SparkxUtils.setFlowErrorMessage(routerFlowConfig.incrementTopics,
                topicPartitionOffset: JSONObject, config, sourceNamespace, sinkNamespace, 1,
                e, batchId, UmsProtocolType.DATA_BATCH_DATA.toString + "," + UmsProtocolType.DATA_INCREMENT_DATA.toString + "," + UmsProtocolType.DATA_INITIAL_DATA.toString,
                routerFlowConfig.flowId, ErrorPattern.StreamError)
            }
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
    }

    )
  }



  def getNewSinkNamespace(keys: Array[String], sinkNamespace: String): String = {
    val sinkNsGrp = sinkNamespace.split("\\.")
    val tableName = if (sinkNsGrp(3) == "*") keys(4) else sinkNsGrp(3)
    val sinkVersion = if (sinkNsGrp(4) == "*") keys(5) else sinkNsGrp(4)
    val sinkDbPar = if (sinkNsGrp(5) == "*") keys(6) else sinkNsGrp(5)
    val sinkTablePar = if (sinkNsGrp(6) == "*") keys(7) else sinkNsGrp(6)

    val messageBuf = new StringBuilder
    messageBuf ++= sinkNsGrp(0) ++= "." ++= sinkNsGrp(1) ++= "." ++= sinkNsGrp(2) ++= "." ++= tableName ++= "." ++= sinkVersion ++= "." ++= sinkDbPar ++= "." ++= sinkTablePar

    messageBuf.toString()
  }
}