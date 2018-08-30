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

package edp.wormhole.flinkx.common

import edp.wormhole.util.config.KVConfig

case class WormholeFlinkxConfig(kafka_input: KafkaInputConfig,
                                kafka_output: KafkaOutputConfig,
                                flink_config: String,
                                parallelism: Int,
                                zookeeper_address: String)


case class KafkaInputConfig(kafka_base_config: KafkaInputBaseConfig,
                            kafka_topics: Seq[KafkaTopicConfig]
                           ) {
  lazy val autoCommit = true
  lazy val groupId: String = kafka_base_config.group_id
  lazy val sessionTimeout: String = kafka_base_config.`session.timeout.ms`.toString

  lazy val inputBrokers = Map("bootstrap.servers" -> kafka_base_config.brokers,
    "key.deserializer" -> "org.apache.kafka.edp.wormhole.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.edp.wormhole.common.serialization.StringDeserializer",
    "session.timeout.ms" -> kafka_base_config.`session.timeout.ms`.toString,
    "group.id" -> kafka_base_config.group_id,
    "enable.auto.commit" -> "false")
}


case class KafkaInputBaseConfig(
                                 `key.deserializer`: String,
                                 `value.deserializer`: String,
                                 `session.timeout.ms`: Int,
                                 `group.max.session.timeout.ms`: Int,
                                 `auto.offset.reset`: String,
                                 group_id: String,
                                 brokers: String)


case class KafkaOutputConfig(feedback_topic_name: String,
                             brokers: String,
                             config: Option[Seq[KVConfig]])

case class KafkaTopicConfig(topic_name: String,
                            topic_partition: String)

case class PartitionOffsetConfig(partition_num: Int, offset: Long)


case class FlinkConfig(stream_id: Long,
                       stream_name: String)
