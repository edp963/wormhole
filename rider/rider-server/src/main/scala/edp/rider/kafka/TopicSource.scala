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
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import edp.rider.RiderStarter.modules
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.service.util.FeedbackOffsetUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object TopicSource extends RiderLogger {

  def createPerPartition(groupId: String)(implicit system: ActorSystem) = {
    //    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    //      .withBootstrapServers(modules.config.getString("akka.kafka.consumer.kafka-clients.bootstrap.servers"))
    //      .withGroupId(modules.config.getString("akka.kafka.consumer.kafka-clients.group.id"))
    //      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumerSettings = new ConsumerSettings(null, Some(RiderConfig.consumer.keyDeserializer),
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
    Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(RiderConfig.consumer.topic))
  }


  def createFromOffset(groupId: String)(implicit system: ActorSystem): Source[CommittableMessage[Array[Byte], String], Control] = {
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(RiderConfig.consumer.brokers)
      .withGroupId(RiderConfig.consumer.group_id)
    val topicMap: mutable.Map[TopicPartition, Long] = FeedbackOffsetUtil.getTopicMapForDB(0,RiderConfig.consumer.topic, RiderConfig.consumer.partitions)
    if (topicMap == null || topicMap.isEmpty) {
      riderLogger.error(s"topicMap is empty")
    }
    Consumer.committableSource(consumerSettings, Subscriptions.assignmentWithOffset(topicMap.toMap))
  }

}
