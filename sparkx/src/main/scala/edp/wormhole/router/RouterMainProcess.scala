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


package edp.wormhole.router

import java.util.UUID

import edp.wormhole.common.util.DateUtils.currentDateTime
import edp.wormhole.common.{FeedbackPriority, SparkUtils, WormholeConfig, WormholeUtils}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsFeedbackStatus, UmsProtocolUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable.ArrayBuffer

object RouterMainProcess extends EdpLogging {

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, session: SparkSession): Unit = {
    stream.foreachRDD((streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
      streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
      try {
        logInfo("start foreachRDD")
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val statsId = UUID.randomUUID().toString
        val rddTs = System.currentTimeMillis

        logInfo("start doDirectiveTopic")
        val directiveTs = System.currentTimeMillis
        RouterDirective.doDirectiveTopic(config, stream)

        logInfo("start Repartition")
        val mainDataTs = System.currentTimeMillis

        val routerKeys = ConfMemoryStorage.getRouterKeys

        logInfo("routerMap keys:" + routerKeys.size)
        routerKeys.foreach(println)

        val dataRepartitionRdd: RDD[(String, String)] =
          if (config.rdd_partition_number != -1) streamRdd.map(row => (row.key, row.value)).repartition(config.rdd_partition_number)
          else streamRdd.map(row => (row.key, row.value))

        val routerMap = ConfMemoryStorage.getRouterMap

        dataRepartitionRdd.foreachPartition { partition =>
          routerMap.foreach { case (_, (map, _)) =>
            map.foreach { case (_, (kafkaBroker, _)) => {
              WormholeKafkaProducer.init(kafkaBroker, None)
            }
            }
          }
          partition.foreach { case (key, value) => {
            val keys = key.split("\\.")
            val (protocolType, namespace) = if (keys.length > 7) (keys(0).toLowerCase, keys.slice(1, 8).mkString(".")) else (keys(0).toLowerCase, "")
            logInfo("dbus namespace: " + namespace)
            val matchNamespace = namespace.split("\\.").take(4).mkString(".") + ".*.*.*".toLowerCase()
            logInfo("wormhole namespace: " + matchNamespace)
            if (ConfMemoryStorage.existNamespace(routerKeys, matchNamespace)) {
              if (routerMap(matchNamespace)._2 == "ums") {
                logInfo("start process namespace: " + matchNamespace)
                val messageIndex = value.lastIndexOf(namespace)
                val prefix = value.substring(0, messageIndex)
                val suffix = value.substring(messageIndex + namespace.length)
                routerMap(matchNamespace)._1.foreach { case (sinkNamespace, (kafkaBroker, kafkaTopic)) =>
                  val messageBuf = new StringBuilder
                  messageBuf ++= prefix ++= sinkNamespace ++= suffix
                  val kafkaMessage = messageBuf.toString
                  WormholeKafkaProducer.sendMessage(kafkaTopic, kafkaMessage, Some(protocolType + "." + sinkNamespace + "..." + UUID.randomUUID().toString), kafkaBroker)
                }
              } else {
                routerMap(matchNamespace)._1.foreach { case (sinkNamespace, (kafkaBroker, kafkaTopic)) =>
                  WormholeKafkaProducer.sendMessage(kafkaTopic, value, Some(protocolType + "." + sinkNamespace + "..." + UUID.randomUUID().toString), kafkaBroker)
                }
              }
            }
          }
          }
        }
        WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      } catch {
        case e: Throwable =>
          logAlert("batch error", e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackStreamBatchError(config.spark_config.stream_id, currentDateTime, UmsFeedbackStatus.SUCCESS, ""), None, config.kafka_output.brokers)
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetInfo.toArray)
    })
  }

  def removeFromRouterMap(sourceNamespace: String, sinkNamespace: String): Unit = {
    if (ConfMemoryStorage.routerMap.contains(sourceNamespace) && ConfMemoryStorage.routerMap(sourceNamespace)._1.contains(sinkNamespace)) {
      //todo concurrent problem？
      ConfMemoryStorage.routerMap(sourceNamespace)._1.remove(sinkNamespace)
      if (ConfMemoryStorage.routerMap(sourceNamespace)._1.isEmpty) {
        ConfMemoryStorage.routerMap.remove(sourceNamespace)
      }
    } else {
      logAlert("router from " + sourceNamespace + " to " + sinkNamespace + " does not exists")
    }
  }
}