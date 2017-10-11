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


package edp.wormhole.core

import edp.wormhole.common.KVConfig

case class WormholeConfig(kafka_input: KafkaInputBaseConfig,
                          kafka_output: KafkaOutputConfig,
//                          udf: Option[Seq[String]],
                          spark_config: SparkConfig,
                          rdd_partition_number: Int, //-1 do not repartition
                          zookeeper_path: String,
                          kafka_persistence_config_isvalid: Boolean,
                          stream_hdfs_address: Option[String])//for parquet，data is main namespace or join namespace

case class SparkConfig(//batch_duration_seconds: Int,
                       stream_id: Long,
                       stream_name: String,
                       master: String,
                       `spark.sql.shuffle.partitions`: Int)

case class KafkaOutputConfig(feedback_topic_name: String, brokers: String, config: Option[Seq[KVConfig]])

case class KafkaInputConfig(kafka_base_config: KafkaInputBaseConfig,
                            kafka_topics: Seq[KafkaTopicConfig],
                            inWatch:Boolean) {
  lazy val inputBrokers = Map("bootstrap.servers" -> kafka_base_config.brokers,
    "max.partition.fetch.bytes" -> kafka_base_config.`max.partition.fetch.bytes`.toString,
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "session.timeout.ms" -> kafka_base_config.`session.timeout.ms`.toString,
    "group.id" -> kafka_base_config.group_id,
    "enable.auto.commit" -> "false")
}

case class KafkaInputBaseConfig(`max.partition.fetch.bytes`: Int,
                                `key.deserializer`: String,
                                `value.deserializer`: String,
                                `session.timeout.ms`: Int,
                                group_id: String,
                                batch_duration_seconds: Int,
                                brokers: String)
//                                heartbeat_topic_name: String,
//                                heartbeat_topic_rate: Long,
//                                heartbeat_topic_offset: Long)

case class KafkaTopicConfig(topic_name: String,
                            topic_rate: Int,
                            topic_partition: Seq[PartitionOffsetConfig])

case class PartitionOffsetConfig(partition_num: Int, offset: Long)

//case class PartitionOffsetSeq(topic_partition: Seq[PartitionOffsetConfig])


object StreamType extends Enumeration {
  type StreamType = Value
  val BATCHFLOW = Value("batchflow")
  val HDFSLOG = Value("hdfslog")
  val ROUTER = Value("router")
  def streamType(s: String) = StreamType.withName(s.toLowerCase)

}


object InputDataRequirement extends Enumeration {
  type InputDataRequirement = Value
  val INITIAL = Value("initial")
  val INCREMENT = Value("increment")
  val BATCH = Value("batch")
  def inputDataRequirement(s: String) = InputDataRequirement.withName(s.toLowerCase)

}
