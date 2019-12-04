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

import edp.wormhole.sparkx.memorystorage.{ConfMemoryStorage, OffsetPersistenceManager}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts
import edp.wormhole.sparkxinterface.swifts.{KafkaTopicConfig, PartitionOffsetConfig, TopicType, WormholeConfig}
import edp.wormhole.ums.{Ums, UmsFieldType, UmsSysField}
import edp.wormhole.util.DateUtils
import org.apache.spark.streaming.kafka010.WormholeDirectKafkaInputDStream

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait Directive extends EdpLogging {

  def flowStartProcess(ums: Ums): String = {
    null
  }

  def doDirectiveTopic(config: WormholeConfig, stream: WormholeDirectKafkaInputDStream[String, String]): Unit = {
    val addTopicList = ListBuffer.empty[(KafkaTopicConfig, Long,Long)]
    val delTopicList = mutable.ListBuffer.empty[(String, Long,Long)]
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

      println(s"addTopicList:$addTopicList,delTopicList:$delTopicList")
      val (finalAddTopicList,finalDelTopicList) =getFinalTopics(addTopicList,delTopicList)
      println(s"finalAddTopicList:$finalAddTopicList,finalDelTopicList:$finalDelTopicList")

      val initialTopics = finalAddTopicList.filter(topic => topic._1.topic_type == TopicType.INITIAL).map(_._1.topic_name)
      ConfMemoryStorage.initialTopicSet ++= initialTopics
      ConfMemoryStorage.initialTopicSet --= finalDelTopicList.map(_._1)
      val addTpMap = mutable.HashMap.empty[(String, Int), (Long, Long)]
      finalAddTopicList.foreach(topic => {
        val topicName = topic._1.topic_name
        topic._1.topic_partition.foreach(partition => {
          addTpMap((topicName, partition.partition_num)) = (partition.offset, topic._1.topic_rate)
        })
      })
      stream.updateTopicOffset(addTpMap.map(tp => {
        (tp._1, (tp._2._1, tp._2._2))
      }).toMap, finalDelTopicList.map(_._1).toSet)

      OffsetPersistenceManager.doTopicPersistence(config, finalAddTopicList, finalDelTopicList)
    }
  }

  def getFinalTopics(addTopicList:ListBuffer[(KafkaTopicConfig, Long,Long)],delTopicList:mutable.ListBuffer[(String, Long,Long)]):
  (ListBuffer[(KafkaTopicConfig, Long,Long)],mutable.ListBuffer[(String, Long,Long)]) ={
    val addRemove = mutable.HashSet.empty[String]
    val delRemove = mutable.HashSet.empty[String]
    addTopicList.foreach(addOne=>{
      delTopicList.foreach(delOne=>{
        if(addOne._1.topic_name.toLowerCase==delOne._1.toLowerCase){
          if(addOne._3 >= delOne._3){
            delRemove += delOne._1
          }else{
            addRemove += addOne._1.topic_name
          }
        }
      })
    })

    val finalAddTopicList = addTopicList.filter(addOne=>{
      !addRemove.contains(addOne._1.topic_name)
    })

    val finalDelTopicList = delTopicList.filter(delOne=>{
      !delRemove.contains(delOne._1)
    })

    (finalAddTopicList,finalDelTopicList)
  }


  def topicSubscribeParse(ums: Ums, config: WormholeConfig, stream: WormholeDirectKafkaInputDStream[String, String]): ListBuffer[(KafkaTopicConfig, Long,Long)] = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val topicConfigList = ListBuffer.empty[(KafkaTopicConfig, Long,Long)]
    payloads.foreach(tuple => {
      val timeStamp = DateUtils.dt2long(UmsFieldType.umsFieldValue(tuple.tuple, schemas, UmsSysField.TS.toString).toString)
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val topicName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_name").toString
      println(s"topicSubscribeParse,$topicName,$timeStamp")
      val topicRate = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_rate").toString.toInt
      val topicType =
        if (UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_type") != null)
          UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_type").toString
        else TopicType.INCREMENT.toString
      val partitionsOffset = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "partitions_offset").toString
      val partitionsOffsetSeq = partitionsOffset.split(",").map(partitionOffset => {
        val partitionOffsetArray = partitionOffset.split(":")
        PartitionOffsetConfig(partitionOffsetArray(0).toInt, partitionOffsetArray(1).toLong)
      })
      topicConfigList += ((KafkaTopicConfig(topicName, topicRate, partitionsOffsetSeq, TopicType.topicType(topicType)), directiveId,timeStamp))
    })

    topicConfigList
  }


  def topicUnsubscribeParse(ums: Ums, config: WormholeConfig): ListBuffer[(String, Long,Long)] = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val topicList = ListBuffer.empty[(String, Long,Long)]
    payloads.foreach(tuple => {
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val topicName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "topic_name").toString
      val timeStamp = DateUtils.dt2long(UmsFieldType.umsFieldValue(tuple.tuple, schemas, UmsSysField.TS.toString).toString)
      println(s"topicUnsubscribeParse,$topicName,$timeStamp")
      topicList += ((topicName, directiveId,timeStamp))
    })

    topicList
  }

}
