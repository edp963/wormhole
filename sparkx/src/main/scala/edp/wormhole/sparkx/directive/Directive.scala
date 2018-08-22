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


package edp.wormhole.sparkx.directive

import edp.wormhole.sinks.common
import edp.wormhole.sparkx.common.{KafkaTopicConfig, PartitionOffsetConfig, WormholeConfig}
import edp.wormhole.sparkx.memorystorage.OffsetPersistenceManager
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.{Ums, UmsFieldType}
import org.apache.spark.streaming.kafka010.WormholeDirectKafkaInputDStream

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait Directive extends EdpLogging{

  def flowStartProcess(ums: Ums, feedbackTopicName: String, brokers: String): Unit = {

  }

  def doDirectiveTopic(config: WormholeConfig, stream: WormholeDirectKafkaInputDStream[String, String]):Unit = {
    val addTopicList = ListBuffer.empty[(KafkaTopicConfig, Long)]
    val delTopicList = mutable.ListBuffer.empty[(String, Long)]
    if (OffsetPersistenceManager.directiveList.size() > 0) {
      while (OffsetPersistenceManager.directiveList.size() > 0) {
        val (subscribeTopic, unsubscribeTopic) = OffsetPersistenceManager.directiveList.poll()
        logInfo("subscribeTopic:" + subscribeTopic)
        if (subscribeTopic != null)
          try {
            addTopicList ++= topicSubscribeParse(subscribeTopic, config, stream)
          } catch {
            case e: Throwable => logAlert("subscribeTopic error" + subscribeTopic, e)
          }
        if (unsubscribeTopic != null)
          try {
            delTopicList ++= topicUnsubscribeParse(unsubscribeTopic, config)
          } catch {
            case e: Throwable => logAlert("unsubscribeTopic error" + unsubscribeTopic, e)
          }
      }

      val addTpMap = mutable.HashMap.empty[(String, Int), (Long, Long)]
      addTopicList.foreach(topic => {
        val topicName = topic._1.topic_name
        topic._1.topic_partition.foreach(partition => {
          addTpMap((topicName, partition.partition_num)) = (partition.offset, topic._1.topic_rate)
        })
      })
      stream.updateTopicOffset(addTpMap.map(tp => {
        (tp._1, (tp._2._1, tp._2._2))
      }).toMap, delTopicList.map(_._1).toSet)

      OffsetPersistenceManager.doTopicPersistence(config, addTopicList, delTopicList)
    }
  }




  def topicSubscribeParse(ums: Ums, config: WormholeConfig, stream: WormholeDirectKafkaInputDStream[String, String]): ListBuffer[(KafkaTopicConfig, Long)] = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val topicConfigList = ListBuffer.empty[(KafkaTopicConfig, Long)]
    payloads.foreach(tuple => {
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val topicName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_name").toString
      val topicRate = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_rate").toString.toInt
      val partitionsOffset = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "partitions_offset").toString
      val partitionsOffsetSeq = partitionsOffset.split(",").map(partitionOffset => {
        val partitionOffsetArray = partitionOffset.split(":")
        PartitionOffsetConfig(partitionOffsetArray(0).toInt, partitionOffsetArray(1).toLong)
      })
      topicConfigList += ((KafkaTopicConfig(topicName, topicRate, partitionsOffsetSeq), directiveId))
    })

    topicConfigList
  }


  def topicUnsubscribeParse(ums: Ums, config: WormholeConfig): ListBuffer[(String, Long)] = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val topicList = ListBuffer.empty[(String, Long)]
    payloads.foreach(tuple => {
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val topicName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_name").toString
      topicList += ((topicName, directiveId))
    })

    topicList
  }

}
