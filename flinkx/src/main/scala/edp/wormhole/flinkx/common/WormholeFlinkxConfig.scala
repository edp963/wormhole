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

import edp.wormhole.flinkx.common.ExceptionProcessMethod.ExceptionProcessMethod
import edp.wormhole.util.config.KVConfig

case class WormholeFlinkxConfig(flow_name: String,
                                kafka_input: KafkaInputConfig,
                                kafka_output: KafkaOutputConfig,
                                commonConfig: CommonConfig,
                                zookeeper_address: String,
                                udf_config: Seq[UdfConfig],
                                feedback_enabled: Boolean,
                                feedback_state_count: Int,
                                feedback_interval: Int,
                                kerberos: Boolean)

case class UdfConfig(id: Long, functionName: String, fullClassName: String, jarName: String, mapOrAgg: String)

case class FlinkConfig(parallelism: Int, checkpoint: FlinkCheckpoint)

case class FlinkCheckpoint(isEnable: Boolean = false, `checkpointInterval.ms`: Int = 60000, stateBackend: String)

case class CommonConfig(stateBackend: String)

case class KafkaInputConfig(kafka_base_config: KafkaInputBaseConfig,
                            kafka_topics: Seq[KafkaTopicConfig]) {
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


case class KafkaInputBaseConfig(`key.deserializer`: String,
                                `value.deserializer`: String,
                                `session.timeout.ms`: Int,
                                `group.max.session.timeout.ms`: Int,
                                `auto.offset.reset`: String,
                                group_id: String,
                                brokers: String,
                                kerberos: Boolean)


case class KafkaOutputConfig(feedback_topic_name: String,
                             brokers: String,
                             kerberos: Boolean,
                             config: Option[Seq[KVConfig]])

case class KafkaTopicConfig(topic_name: String,
                            topic_partition: String)

case class PartitionOffsetConfig(partition_num: Int, offset: Long)


case class ExceptionConfig(streamId: Long,
                           flowId: Long,
                           sourceNamespace: String,
                           sinkNamespace: String,
                           exceptionProcessMethod: ExceptionProcessMethod)

object ExceptionProcessMethod extends Enumeration with Serializable {
  type ExceptionProcessMethod = Value

  val INTERRUPT = Value("interrupt")
  val FEEDBACK = Value("feedback")
  val UNHANDLE = Value("unhandle")

  def exceptionProcessMethod(s: String) = {
    if (s == null)
      UNHANDLE
    else
      ExceptionProcessMethod.withName(s.toLowerCase)
  }
}

object BuiltInFunctions extends Enumeration with Serializable {
  type BuiltInFunctions = Value

  val FIRSTVALUE = Value("firstvalue")
  val LASTVALUE = Value("lastvalue")
  val ADJACENTSUB = Value("adjacentsub")
  val MAPTOSTRING = Value("WhMapToString")

  def builtInFunctions(s: String): BuiltInFunctions = {
    BuiltInFunctions.withName(s)
  }
}
