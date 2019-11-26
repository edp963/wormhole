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


package edp.wormhole.sparkxinterface.swifts

import edp.wormhole.util.config.KVConfig
import edp.wormhole.util.swifts.SwiftsSql

case class SwiftsProcessConfig(//projection:String,
                               swiftsSql: Option[Array[SwiftsSql]] = None,
                               validityConfig: Option[ValidityConfig] = None,
                               datasetShow: Option[Boolean] = None,
                               datasetShowNum: Option[Int] = None,
                               specialConfig: Option[String] = None)

case class SwiftsSpecialProcessConfig(`lookup_batch_size`: Option[Int] = None) {
  lazy val batchSize = `lookup_batch_size`.getOrElse(1000)
}

case class ValidityConfig(checkColumns: Array[String],
                          checkRule: String,
                          rule_mode: String,
                          ruleParams: String,
                          againstAction: String)

/*
case class WormholeConfig(kafka_input: KafkaInputBaseConfig,
                          kafka_output: KafkaOutputConfig,
                          //                          udf: Option[Seq[String]],
                          spark_config: SparkConfig,
                          rdd_partition_number: Int, //-1 do not repartition
                          zookeeper_address: String,
                          zookeeper_path: String,
                          kafka_persistence_config_isvalid: Boolean,
                          stream_hdfs_address: Option[String],
                          hdfs_namenode_hosts: Option[String],
                          hdfs_namenode_ids: Option[String],
                          kerberos: Boolean,
                          hdfslog_server_kerberos: Option[Boolean],
                          special_config: Option[StreamSpecialConfig])

case class KafkaInputBaseConfig(`max.partition.fetch.bytes`: Int,
                                `key.deserializer`: String,
                                `value.deserializer`: String,
                                `session.timeout.ms`: Int,
                                `group.max.session.timeout.ms`: Int,
                                `auto.offset.reset`: String,
                                group_id: String,
                                batch_duration_seconds: Int,
                                brokers: String)


case class KafkaOutputConfig(feedback_topic_name: String, brokers: String, config: Option[Seq[KVConfig]])

case class SparkConfig(//batch_duration_seconds: Int,
                       stream_id: Long,
                       stream_name: String,
                       master: String,
                       `spark.sql.shuffle.partitions`: Int)

case class StreamSpecialConfig(
                                useDefaultKey: Option[Boolean]
                              )*/
