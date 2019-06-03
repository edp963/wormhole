/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole.flinkx.util

import java.lang

import edp.wormhole.flinkx.common.WormholeFlinkxConfig
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

import scala.collection.mutable


class WormholeFlinkxConfigUtils(config: WormholeFlinkxConfig) {
  private lazy val partitionNumIndex = 0
  private lazy val partitionOffsetIndex = 1

  def getKafkaTopicList: java.util.ArrayList[String] = {
    val topics = new java.util.ArrayList[String]()
    config.kafka_input.kafka_topics.map(topic => topics.add(topic.topic_name))
    topics
  }

  def getTopicPartitionOffsetMap: java.util.HashMap[KafkaTopicPartition, lang.Long] = {
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    config.kafka_input.kafka_topics.foreach(topic => {
      val partitionConfig = topic.topic_partition
      partitionConfig.split(",").foreach { par =>
        val partitionOffset = par.split(":")
        specificStartOffsets.put(new KafkaTopicPartition(topic.topic_name, partitionOffset(partitionNumIndex).toInt), partitionOffset(partitionOffsetIndex).toLong)
      }
    })
    specificStartOffsets
  }

  def getTopicOffsetMap: Map[String, Seq[(Int, Long)]] = {
    val specificStartOffsets = mutable.Map.empty[String, Seq[(Int, Long)]]
    config.kafka_input.kafka_topics.foreach(topic => {
      val partitionConfig: Seq[(Int, Long)] = topic.topic_partition.split(",").map { par =>
        val partitionOffset = par.split(":")
        (partitionOffset(partitionNumIndex).toInt, partitionOffset(partitionOffsetIndex).toLong)
      }
      specificStartOffsets += (topic.topic_name -> partitionConfig)
    })
    specificStartOffsets.toMap
  }
}