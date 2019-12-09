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

import edp.rider.rest.persistence.entities.{FlowUdfResponse, StreamSpecialConfig}
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}


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
                                specialConfig: Option[String])

case class SinkConfig(sinkNamespace: String,
                      connectionConfig: ConnectionConfig,
                      maxRecordPerPartitionProcessed: Int,
                      classFullName: Option[String],
                      specialConfig: Option[String],
                      tableKeys: Option[String],
                      projection: Option[String])

case class JobConfig(appName: String,
                     master: String,
                     `spark.sql.shuffle.partitions`: Option[Int])


case class BatchFlowConfig(kafka_input: KafkaInputBaseConfig,
                           kafka_output: KafkaOutputConfig,
                           spark_config: SparkConfig,
                           rdd_partition_number: Int, //-1 do not repartition
                           zookeeper_address: String,
                           zookeeper_path: String,
                           kafka_persistence_config_isvalid: Boolean,
                           special_config: Option[StreamSpecialConfig] = None,
                           stream_hdfs_address: Option[String],
                           kerberos: Boolean = false,
                           hdfs_namenode_hosts: Option[String] = None,
                           hdfs_namenode_ids: Option[String] = None,
                           hdfslog_server_kerberos: Option[Boolean] = None)

//for parquet，data is main namespace or join namespace

case class SparkConfig(stream_id: Long,
                       stream_name: String,
                       master: String,
                       `spark.sql.shuffle.partitions`: Int)

case class KafkaOutputConfig(feedback_topic_name: String,
                             brokers: String,
                             kerberos: Boolean,
                             config: Option[Seq[KVConfig]] = None)

case class KafkaInputBaseConfig(group_id: String,
                                batch_duration_seconds: Int,
                                brokers: String,
                                kerberos: Boolean,
                                `max.partition.fetch.bytes`: Int = 10485760,
                                `session.timeout.ms`: Int = 30000,
                                `group.max.session.timeout.ms`: Int = 60000,
                                `auto.offset.reset`: String = "earliest",
                                `key.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                                `value.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                                `enable.auto.commit`: Boolean = false)

case class KafkaBaseConfig(group_id: String,
                           brokers: String,
                           kerberos: Boolean,
                           `session.timeout.ms`: Int = 30000,
                           `group.max.session.timeout.ms`: Int = 60000,
                           `key.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                           `value.deserializer`: String = "org.apache.kafka.common.serialization.StringDeserializer",
                           `auto.offset.reset`: String = "earliest"
                          )

case class KafkaFlinkTopic(topic_name: String,
                           topic_partition: String)


case class KafkaInput(kafka_base_config: KafkaBaseConfig, kafka_topics: Seq[KafkaFlinkTopic])

case class WhFlinkConfig(flow_name: String, kafka_input: KafkaInput, kafka_output: KafkaOutputConfig, commonConfig: FlinkCommonConfig, zookeeper_address: String, udf_config: Seq[FlowUdfResponse], feedback_enabled: Boolean, feedback_state_count: Int, feedback_interval: Int, kerberos: Boolean = false)

case class FlinkCommonConfig(stateBackend: String)

case class FlinkCheckpoint(enable: Boolean = false, `checkpointInterval.ms`: Int = 60000, stateBackend: String)

