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


package edp.rider.wormhole

import edp.wormhole.common.{ConnectionConfig, KVConfig}

case class BatchJobConfig(sourceConfig: SourceConfig,
                          transformationConfig: Option[TransformationConfig],
                          sinkConfig: SinkConfig,
                          jobConfig: JobConfig)

case class SourceConfig(startTime: String,
                        endTime: String,
                        sourceNamespace: String,
                        connectionConfig: ConnectionConfig,
                        classFullName: String,
                        specialConfig: Option[String])

case class TransformationConfig(action: Option[String],
                                projection: Option[String],
                                specialConfig: Option[String])

case class SinkConfig(sinkNamespace: String,
                      connectionConfig: ConnectionConfig,
                      maxRecordPerPartitionProcessed: Int,
                      classFullName: Option[String],
                      specialConfig: Option[String],
                      tableKeys: Option[String])

case class JobConfig(appName: String,
                     master: String,
                     `spark.sql.shuffle.partitions`: Option[Int])


case class BatchFlowConfig(kafka_input: KafkaInputBaseConfig,
                           kafka_output: KafkaOutputConfig,
                           spark_config: SparkConfig,
                           rdd_partition_number: Int, //-1 do not repartition
                           zookeeper_path: String,
                           kafka_persistence_config_isvalid: Boolean,
                           stream_hdfs_address: Option[String])

//for parquet，data is main namespace or join namespace

case class SparkConfig(stream_id: Long,
                       stream_name: String,
                       master: String,
                       `spark.sql.shuffle.partitions`: Int)

case class KafkaOutputConfig(feedback_topic_name: String, brokers: String, config: Option[Seq[KVConfig]] = None)

case class KafkaInputConfig(kafka_base_config: KafkaInputBaseConfig,
                            kafka_topics: Seq[KafkaTopicConfig],
                            inWatch: Boolean)

case class KafkaInputBaseConfig(group_id: String,
                                batch_duration_seconds: Int,
                                brokers: String,
                                `max.partition.fetch.bytes`: Int = 10485760,
                                `key.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                                `value.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                                `session.timeout.ms`: Int = 30000
                               )


case class KafkaTopicConfig(topic_name: String,
                            topic_rate: Int,
                            topic_partition: Seq[PartitionOffsetConfig])

case class PartitionOffsetConfig(partition_num: Int, offset: Long)

