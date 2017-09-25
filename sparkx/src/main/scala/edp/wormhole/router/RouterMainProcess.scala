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

import edp.wormhole.common.{FeedbackPriority, SparkUtils, WormholeUtils}
import edp.wormhole.common.util.DateUtils
import edp.wormhole.common.util.DateUtils.currentDateTime
import edp.wormhole.core.WormholeConfig
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsFeedbackStatus, UmsProtocolType, UmsProtocolUtils, UmsSysField}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RouterMainProcess extends EdpLogging {
  //[(source,sink),(broker, topic)]
  val routerMap = mutable.HashMap.empty[String, mutable.HashMap[String, (String, String)]]

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
        val dataRepartitionRdd: RDD[(String, String)] =
          if (config.rdd_partition_number != -1) streamRdd.map(row => (row.key, row.value)).repartition(config.rdd_partition_number)
          else streamRdd.map(row => (row.key, row.value))
        val sinkTs = System.currentTimeMillis
        val namespaceTs: RDD[((String, String), (String,Int))] = dataRepartitionRdd.mapPartitions { partition =>
          routerMap.foreach { case (_, map) =>
            map.foreach { case (_, (kafkaBroker, _)) => WormholeKafkaProducer.init(kafkaBroker, None)
            }
          }
          val tsMap = mutable.HashMap.empty[(String, String), (String,Int)] //namespace(lowercase), ums_ts_
          partition.foreach { case (key, value) =>
            val keys = key.split("\\.")

            val (protocolType, namespace) = if (keys.length > 7) (keys(0).toLowerCase, keys.slice(1, 8).mkString(".")) else (keys(0).toLowerCase, "")
            if (routerMap.contains(namespace.toLowerCase)) {
              val messageIndex = value.lastIndexOf(namespace)
              val prefix = value.substring(0, messageIndex)
              val suffix = value.substring(messageIndex + namespace.length)
              routerMap(namespace.toLowerCase).foreach { case (sinkNamespace, (kafkaBroker, kafkaTopic)) =>
                if (!tsMap.contains((namespace.toLowerCase, sinkNamespace)) &&
                  (protocolType == UmsProtocolType.DATA_INITIAL_DATA.toString
                    || protocolType == UmsProtocolType.DATA_INCREMENT_DATA.toString
                    || protocolType == UmsProtocolType.DATA_BATCH_DATA.toString)) {
                  val ums = WormholeUtils.json2Ums(value)
                  val index = ums.schema.fields_get.map(_.name).indexOf(UmsSysField.TS.toString)
                  val ts = ums.payload_get.head.tuple(index)
                  tsMap((namespace.toLowerCase, sinkNamespace)) = (ts,1)
                }else{
                  val(ts,count) = tsMap((namespace.toLowerCase, sinkNamespace))
                  val tmpCount = count+1
                  tsMap((namespace.toLowerCase, sinkNamespace)) = (ts,tmpCount)
                }
                val messageBuf = new StringBuilder
                messageBuf ++= prefix ++= sinkNamespace ++= suffix
                val kafkaMessage = messageBuf.toString
                WormholeKafkaProducer.sendMessage(kafkaTopic, kafkaMessage, Some(protocolType + "." + sinkNamespace), kafkaBroker)
              }
            }

          }
          tsMap.toIterator
        }
        val doneTs = System.currentTimeMillis
        val namespace2tsMap = namespaceTs.collect().toMap
        namespace2tsMap.foreach { case ((sourceNamespace, sinkNamespace), (ts,count)) =>
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
            UmsProtocolUtils.feedbackFlowStats(sourceNamespace, UmsProtocolType.DATA_INCREMENT_DATA.toString, currentDateTime, config.spark_config.stream_id, statsId, sinkNamespace,
              count, DateUtils.dt2date(ts).getTime, rddTs, directiveTs, mainDataTs, sinkTs, sinkTs, doneTs), None, config.kafka_output.brokers)
        }
        WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      } catch {
        case e: Throwable =>
          logAlert("batch error", e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackStreamBatchError(config.spark_config.stream_id, currentDateTime, UmsFeedbackStatus.FAIL, e.getMessage), None, config.kafka_output.brokers)
          WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      }
    })
  }

  def removeFromRouterMap(sourceNamespace: String, sinkNamespace: String): Unit = {
    if (routerMap.contains(sourceNamespace) && routerMap(sourceNamespace).contains(sinkNamespace)) {
      routerMap(sourceNamespace).remove(sinkNamespace)
      if (routerMap(sourceNamespace).isEmpty) {
        routerMap.remove(sourceNamespace)
      }
    } else {
      logAlert("router from " + sourceNamespace + " to " + sinkNamespace + " does not exists")
    }
  }
}

