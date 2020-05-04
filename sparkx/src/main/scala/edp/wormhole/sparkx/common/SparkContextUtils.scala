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


package edp.wormhole.sparkx.common

import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx.spark.kafka010.{WormholeKafkaUtils, WormholePerPartitionConfig}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.KafkaInputConfig
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable

object SparkContextUtils extends EdpLogging{

  def checkSparkRestart(zookeeperAddress: String, zookeeperPath: String, streamId: Long, appId: String): Boolean = {
    val appIdPath = zookeeperPath + "/" + streamId + "/" + appId
    if (WormholeZkClient.checkExist(zookeeperAddress, appIdPath)) {
      logAlert("WormholeStarter restart")
      true
    }else false
  }

  def setLoggerLevel(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)
  }

//  def doSparkStreaming(config:WormholeConfig,appId:String,sparkContext:SparkContext): StreamingContext = {
//    val kafkaInput: KafkaInputConfig = OffsetPersistenceManager.initOffset(config, appId)
//    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(config.kafka_input.batch_duration_seconds))
//
//    val kafkaStream = createKafkaStream(ssc, kafkaInput)
//    MainProcess.process(kafkaStream, config)
//    ssc
//  }

  def startSparkStreaming(ssc:StreamingContext): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }

  def deleteZookeeperOldAppidPath(appid: String, address: String, path: String, streamId:Long): Unit = {
    val appidParentPath = path + "/" + streamId
    val appidPaths: Seq[String] = WormholeZkClient.getChildren(address, appidParentPath)
    for (i <- appidPaths.indices) {
      val appidName = appidPaths(i)
      if (appidName.startsWith("application_") && appidName != appid) WormholeZkClient.delete(address, appidParentPath + "/" + appidName)
    }
  }

  def createKafkaStream(ssc: StreamingContext, kafkaInput: KafkaInputConfig): WormholeDirectKafkaInputDStream[String, String] = {
    logInfo(s" stream start from topic:" + kafkaInput.toString)
    val partitionOffsetMap = mutable.HashMap.empty[TopicPartition, Long]
    val partitionRateMap = mutable.HashMap.empty[TopicPartition, Long]
    val topicList = mutable.ListBuffer.empty[String]

    if (kafkaInput.kafka_topics != null && kafkaInput.kafka_topics.nonEmpty) {
      kafkaInput.kafka_topics.foreach(topic => {
        topicList += topic.topic_name
      })

      kafkaInput.kafka_topics.foreach(topic => {
        topic.topic_partition.foreach(partitionOffset => {
          val tmpPar = new TopicPartition(topic.topic_name, partitionOffset.partition_num)
          partitionRateMap(tmpPar) = topic.topic_rate
          partitionOffsetMap(tmpPar) = partitionOffset.offset
        })
      })
    }

    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    val perConfig: WormholePerPartitionConfig = new WormholePerPartitionConfig(partitionRateMap)
    val consumerStrategy: ConsumerStrategy[String, String] = if(kafkaInput.inWatch){
      ConsumerStrategies.Subscribe[String, String](topicList, kafkaInput.inputBrokers, partitionOffsetMap.toMap)
    }else{
      ConsumerStrategies.Subscribe[String, String](topicList, kafkaInput.inputBrokers)
    }
    WormholeKafkaUtils.createWormholeDirectStream[String, String](ssc, locationStrategy, consumerStrategy, perConfig)
  }

}
