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


package edp.rider.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.service.util.{CacheMap, FeedbackOffsetUtil}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Iterable
import scala.collection.mutable

object TopicSource extends RiderLogger {

  def createPerPartition(groupId: String)(implicit system: ActorSystem) = {
    //    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    //      .withBootstrapServers(modules.config.getString("akka.kafka.consumer.kafka-clients.bootstrap.servers"))
    //      .withGroupId(modules.config.getString("akka.kafka.consumer.kafka-clients.group.id"))
    //      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumerSettings = new ConsumerSettings(Map.empty, Some(RiderConfig.consumer.keyDeserializer),
      Some(RiderConfig.consumer.valueDeserializer),
      RiderConfig.consumer.pollInterval,
      RiderConfig.consumer.pollTimeout,
      RiderConfig.consumer.stopTimeout,
      RiderConfig.consumer.closeTimeout,
      RiderConfig.consumer.commitTimeout,
      RiderConfig.consumer.wakeupTimeout,
      RiderConfig.consumer.maxWakeups,
      RiderConfig.consumer.dispatcher)
      .withBootstrapServers(RiderConfig.consumer.brokers)
      .withGroupId(RiderConfig.consumer.group_id)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(RiderConfig.consumer.feedbackTopic))
  }


  def createFromOffset(groupId: String)(implicit system: ActorSystem): Seq[Source[ConsumerRecord[Array[Byte], String], Control]] = {
    //    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    //      .withBootstrapServers(RiderConfig.consumer.brokers)
    //      .withGroupId(RiderConfig.consumer.group_id)
    val propertyMap = new mutable.HashMap[String, String]()
    propertyMap("session.timeout.ms") = RiderConfig.getIntConfig("kafka.consumer.session.timeout.ms", 60000).toString
    propertyMap("heartbeat.interval.ms") = RiderConfig.getIntConfig("kafka.consumer.heartbeat.interval.ms", 50000).toString
    propertyMap("max.poll.records") = RiderConfig.getIntConfig("kafka.consumer.max.poll.records", 500).toString
    propertyMap("request.timeout.ms") = RiderConfig.getIntConfig("kafka.consumer.request.timeout.ms", 80000).toString
    propertyMap("max.partition.fetch.bytes") = RiderConfig.getIntConfig("kafka.consumer.max.partition.fetch.bytes", 10485760).toString
    propertyMap("fetch.min.bytes") = 0.toString
    propertyMap("enable.auto.commit") = "false"

    val consumerSettings = new ConsumerSettings(propertyMap.toMap, Some(RiderConfig.consumer.keyDeserializer),
      Some(RiderConfig.consumer.valueDeserializer),
      RiderConfig.consumer.pollInterval,
      RiderConfig.consumer.pollTimeout,
      RiderConfig.consumer.stopTimeout,
      RiderConfig.consumer.closeTimeout,
      RiderConfig.consumer.commitTimeout,
      RiderConfig.consumer.wakeupTimeout,
      RiderConfig.consumer.maxWakeups,
      RiderConfig.consumer.dispatcher)
      .withBootstrapServers(RiderConfig.consumer.brokers)
      .withGroupId(RiderConfig.consumer.group_id)
    val topicMap: mutable.Map[TopicPartition, Long] = FeedbackOffsetUtil.getTopicMapForDB(0, RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.partitions)
    val earliestMap = {
      try {
        KafkaUtils.getKafkaEarliestOffset(RiderConfig.consumer.brokers, RiderConfig.consumer.feedbackTopic)
          .split(",").map(partition => {
          val partitionOffset = partition.split(":")
          (new TopicPartition(RiderConfig.consumer.feedbackTopic, partitionOffset(0).toInt), partitionOffset(1).toLong)
        }).toMap[TopicPartition, Long]
      } catch {
        case ex: Exception =>
          "0:0,1:0,2:0,3:0".split(",").map(partition => {
            val partitionOffset = partition.split(":")
            (new TopicPartition(RiderConfig.consumer.feedbackTopic, partitionOffset(0).toInt), partitionOffset(1).toLong)
          }).toMap[TopicPartition, Long]
      }

    }


    topicMap.foreach(partition => {
      if (partition._2 < earliestMap(partition._1))
        topicMap(partition._1) = earliestMap(partition._1)
    })

//    if (topicMap == null || topicMap.isEmpty) {
//      riderLogger.error(s"topicMap is empty")
//    }
//
//    topicMap.foreach(part => {
//      CacheMap.setOffsetMap(0, RiderConfig.consumer.feedbackTopic, part._1.partition(), part._2)
//      riderLogger.info(s"topic ${RiderConfig.consumer.feedbackTopic} partition ${part._1.partition()} offset ${part._2}")
//    })

    topicMap.toMap.map(
      topic => Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topic))
    ).toSeq

    //    Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topicMap.toMap))

    //    Consumer.plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
  }

}
