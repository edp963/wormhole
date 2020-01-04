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


package edp.wormhole.sparkx.memorystorage

import java.util.concurrent.ConcurrentLinkedQueue

import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx
import edp.wormhole.sparkx.common._
import edp.wormhole.sparkx.directive.DirectiveOffsetWatch
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts
import edp.wormhole.sparkxinterface.swifts._
import edp.wormhole.ums.UmsSchemaUtils.toUms
import edp.wormhole.ums._
import edp.wormhole.util.JsonUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OffsetPersistenceManager extends EdpLogging {

  val directiveList = new ConcurrentLinkedQueue[(Ums, Ums)]

  val topicTypePath = "topictype"
  val rateRelativePath = "rate"
  val partitionRelativePath = "partition"
  val offsetRelativePath = "/offset"
  val kafkaBaseConfigRelativePath = "kafkaconfig"

  def initOffset(config: WormholeConfig, appId: String): KafkaInputConfig = {
    val kafkaBaseConfig: KafkaInputBaseConfig = config.kafka_input
    val topicConfigMap = mutable.HashMap.empty[String, KafkaTopicConfig]
    logInfo("appId=" + appId)
    val zookeeperAddress = config.zookeeper_address

    val offsetPath = config.zookeeper_path + "/" + config.spark_config.stream_id + OffsetPersistenceManager.offsetRelativePath
    val appIdPath = config.zookeeper_path + "/" + config.spark_config.stream_id + "/" + appId
    val persistenceTopicConfig = readFromPersistence(zookeeperAddress, offsetPath)
    var inWatch = true
    //appid exists means spark restart,user config is valid, both use persistence config
    if (WormholeZkClient.checkExist(zookeeperAddress, appIdPath) || config.kafka_persistence_config_isvalid) {
      inWatch = false
      //take topic config from persistence
      if (persistenceTopicConfig != null) {
        persistenceTopicConfig.foreach(topic => {
          topicConfigMap(topic.topic_name) = topic
        })
      }
    } else {
      //take directive config from watch
      val (subscribeTopicUms, unsubscribeTopicUms) = readFromWatch(zookeeperAddress, offsetPath + "/" + DirectiveOffsetWatch.watchRelativePath)
      if (subscribeTopicUms != null) {
        //add topic of watch
        if (subscribeTopicUms.payload.nonEmpty)
          getWatchSubscribeTopic(subscribeTopicUms).foreach(topic => {
            //if not exist means seted before, wh3 will update offset,offset in persistence may newer
            if (!topicConfigMap.contains(topic.topic_name)) topicConfigMap(topic.topic_name) = topic
          })
      }
      //config in watch is valid
      if (unsubscribeTopicUms != null) {
        //remove topic config from watch
        if (unsubscribeTopicUms.payload.nonEmpty)
          getWatchUnsubscribeTopic(unsubscribeTopicUms).foreach(topicName => {
            topicConfigMap -= topicName
          })
      }

      val topicNamesInZk: Seq[String] = persistenceTopicConfig.map(_.topic_name)
      deleteTopics(zookeeperAddress, offsetPath, topicNamesInZk)
      persistTopic(topicConfigMap.values.toList, offsetPath, zookeeperAddress)
    }

    if (topicConfigMap == null) throw new Exception("do not config kafka any topic,include heardbeat topic")

    DirectiveOffsetWatch.offsetWatch(config, appId)
    val topics = topicConfigMap.values.toList

    topics.foreach((topic: KafkaTopicConfig) =>{
      logInfo("start topics:"+topic)
    })
    KafkaInputConfig(kafkaBaseConfig, topics, inWatch, config.kafka_input.kerberos)
  }

  private def deleteTopics(zookeeperAddress: String, offsetPath: String, topicList: Seq[String]): Unit = {
    topicList.foreach(topic => {
      WormholeZkClient.delete(zookeeperAddress, offsetPath + "/" + topic)
    })

  }

  def getWatchUnsubscribeTopic(unsubscribeTopic: Ums): Seq[String] = {
    unsubscribeTopic.payload_get.map(umsTuple => {
      UmsFieldType.umsFieldValue(umsTuple.tuple, unsubscribeTopic.schema.fields_get, "topic_name").toString
    })
  }

  def getWatchSubscribeTopic(subscribeTopic: Ums): Seq[KafkaTopicConfig] = {
    subscribeTopic.payload_get.map(umsTuple => {
      val topicName = UmsFieldType.umsFieldValue(umsTuple.tuple, subscribeTopic.schema.fields_get, "topic_name").toString
      val topicRate = UmsFieldType.umsFieldValue(umsTuple.tuple, subscribeTopic.schema.fields_get, "topic_rate").toString
      val topicPartition = UmsFieldType.umsFieldValue(umsTuple.tuple, subscribeTopic.schema.fields_get, "partitions_offset").toString
      val topicType = UmsFieldType.umsFieldValue(umsTuple.tuple, subscribeTopic.schema.fields_get, "topic_type").toString
      val poc = topicPartition.split(",").map(tp => {
        val tpo = tp.split(":")
        PartitionOffsetConfig(tpo(0).toInt, tpo(1).toLong)
      })
      KafkaTopicConfig(topicName, topicRate.toInt, poc, TopicType.topicType(topicType))
    })
  }

  def getKafkaBaseConfig(kafkaConfig: Ums): KafkaInputBaseConfig = {
    val umsTuple: UmsTuple = kafkaConfig.payload_get.head
    kafkaConfig.schema.fields_get.foreach(x => println(x.name))
    umsTuple.tuple.foreach(println)
    val kafkaConfigStr = UmsFieldType.umsFieldValue(umsTuple.tuple, kafkaConfig.schema.fields_get, "kafka_config").toString
    JsonUtils.json2caseClass[KafkaInputBaseConfig](kafkaConfigStr)
  }

  def readFromWatch(zookeeperAddress: String, watchPath: String): (Ums, Ums) = {
    val topicStr = new String(WormholeZkClient.getData(zookeeperAddress, watchPath))
    getSubAndUnsubUms(topicStr)
  }

  def readFromPersistence(zookeeperAddress: String, offsetPath: String): mutable.Seq[KafkaTopicConfig] = {
    val topicConfigList = ListBuffer.empty[KafkaTopicConfig]
    WormholeZkClient.getChildren(zookeeperAddress, offsetPath).toArray.foreach(topicNameRef => {
      val topicName = topicNameRef
      if (topicName != OffsetPersistenceManager.kafkaBaseConfigRelativePath && topicName != DirectiveOffsetWatch.watchRelativePath) {
        try {
          val topicTypeFullPath:String = s"${offsetPath}/${topicName}/${topicTypePath}"
          val topicType = if(WormholeZkClient.checkExist(zookeeperAddress,topicTypeFullPath))
            new String(WormholeZkClient.getData(zookeeperAddress, topicTypeFullPath))
          else TopicType.INITIAL.toString

          val rateStr = new String(WormholeZkClient.getData(zookeeperAddress, offsetPath + "/" + topicName + "/" + rateRelativePath))
          val partitionNum = new String(WormholeZkClient.getData(zookeeperAddress, offsetPath + "/" + topicName + "/" + partitionRelativePath)).toInt
          val pocSeq: Seq[PartitionOffsetConfig] = for (i <- 0 until partitionNum) yield PartitionOffsetConfig(i, 0)

          topicConfigList += KafkaTopicConfig(topicName, rateStr.toInt, pocSeq, TopicType.topicType(topicType))
        } catch {
          case e: Throwable => logWarning("readFromPersistence topic " + topicName, e)
        }
      }
    })
    topicConfigList
  }

  def persistTopic(topicConfigList: Seq[KafkaTopicConfig], offsetPath: String, zookeeperAddress: String): Unit = {
    topicConfigList.foreach(topic => {
      WormholeZkClient.createAndSetData(zookeeperAddress, offsetPath + "/" + topic.topic_name + "/" + topicTypePath, topic.topic_type.toString.getBytes)
      WormholeZkClient.createAndSetData(zookeeperAddress, offsetPath + "/" + topic.topic_name + "/" + rateRelativePath, topic.topic_rate.toString.getBytes)
      WormholeZkClient.createAndSetData(zookeeperAddress, offsetPath + "/" + topic.topic_name + "/" + partitionRelativePath, topic.topic_partition.size.toString.getBytes)
    })
  }

  def doTopicPersistence(config: WormholeConfig, addTopicList: ListBuffer[(KafkaTopicConfig, Long,Long)], delTopicList: mutable.ListBuffer[(String, Long,Long)]): Unit = {
    if (addTopicList.nonEmpty) {
      val offsetPath = config.zookeeper_path + "/" + config.spark_config.stream_id + OffsetPersistenceManager.offsetRelativePath
      OffsetPersistenceManager.persistTopic(addTopicList.map(_._1), offsetPath, config.zookeeper_path)

      // todo topic register feedback暂未处理，先不发送
      //      addTopicList.foreach(tp => {
      //        WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority, UmsProtocolUtils.feedbackDirective(DateUtils.currentDateTime, tp._2, UmsFeedbackStatus.SUCCESS,config.spark_config.stream_id,""), Some(UmsProtocolType.FEEDBACK_DIRECTIVE+"."+config.spark_config.stream_id), config.kafka_output.brokers)
      //      })
    }
    if (delTopicList.nonEmpty) {
      //      delTopicList.foreach(tp => {
      //        WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority, UmsProtocolUtils.feedbackDirective(DateUtils.currentDateTime, tp._2, UmsFeedbackStatus.SUCCESS,config.spark_config.stream_id,""), Some(UmsProtocolType.FEEDBACK_DIRECTIVE+"."+config.spark_config.stream_id), config.kafka_output.brokers)
      //      })
      OffsetPersistenceManager.removeTopic(delTopicList.map(_._1), config.spark_config.stream_id, config.zookeeper_address, config.zookeeper_path)
    }

  }

  def removeTopic(topicNameList: Seq[String], streamId: Long, address: String, path: String): Unit = {
    topicNameList.foreach(topicName => {
      val topicPath = path + "/" + streamId + offsetRelativePath + "/" + topicName
      logInfo("remove topic:" + topicPath)
      WormholeZkClient.delete(address, topicPath)
    })
  }

  def getSubAndUnsubUms(data: String): (Ums, Ums) = {
    val umsArray: Array[Ums] = data.split("\\\n").map(row => toUms(row.trim))
    val subscribeUmsArray: Array[Ums] = umsArray.filter(_.protocol.`type` == UmsProtocolType.DIRECTIVE_TOPIC_SUBSCRIBE)
    val subscribeTuple: Array[UmsTuple] = subscribeUmsArray.flatMap(_.payload_get)
    val allSubscribeUms = if (subscribeUmsArray.length > 0) Ums(subscribeUmsArray.head.protocol, subscribeUmsArray.head.schema, Some(subscribeTuple)) else null.asInstanceOf[Ums]
    val unsubscribeUmsArray = umsArray.filter(_.protocol.`type` == UmsProtocolType.DIRECTIVE_TOPIC_UNSUBSCRIBE)
    val unsubscribeTuple = unsubscribeUmsArray.flatMap(_.payload_get)
    val allUmsubscribeUms = if (unsubscribeUmsArray.length > 0) Ums(unsubscribeUmsArray.head.protocol, unsubscribeUmsArray.head.schema, Some(unsubscribeTuple)) else null.asInstanceOf[Ums]

    (allSubscribeUms, allUmsubscribeUms)
  }

}



