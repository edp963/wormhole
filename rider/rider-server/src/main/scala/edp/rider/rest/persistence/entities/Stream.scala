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


package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}

case class Stream(id: Long,
                  name: String,
                  desc: Option[String] = None,
                  projectId: Long,
                  instanceId: Long,
                  streamType: String,
                  sparkConfig: Option[String] = None,
                  startConfig: String,
                  launchConfig: String,
                  sparkAppid: Option[String] = None,
                  logPath: Option[String] = None,
                  status: String,
                  startedTime: Option[String] = None,
                  stoppedTime: Option[String] = None,
                  active: Boolean,
                  createTime: String,
                  createBy: Long,
                  updateTime: String,
                  updateBy: Long) extends BaseEntity


case class StreamAdmin(stream: Stream,
                       disableActions: String,
                       projectName: String,
                       kafkaName: String,
                       kafkaConnection: String,
                       topicInfo: Seq[SimpleTopic])

case class StreamTopic(id: Long,
                       name: String,
                       desc: Option[String] = None,
                       projectId: Long,
                       instanceId: Long,
                       streamType: String,
                       sparkConfig: Option[String] = None,
                       startConfig: String,
                       launchConfig: String,
                       sparkAppid: Option[String] = None,
                       logPath: Option[String] = None,
                       status: String,
                       startedTime: Option[String] = None,
                       stoppedTime: Option[String] = None,
                       active: Boolean,
                       createTime: String,
                       createBy: Long,
                       updateTime: String,
                       updateBy: Long,
                       topic: String) extends BaseEntity

case class StreamReturn(stream: Stream,
                        disableActions: String)

case class StreamWithBrokers(stream: Stream,
                             brokers: String)

case class StreamProject(stream: Stream,
                         projectName: String)

case class StreamTopicTem(stream: Stream,
                          kafkaName: String,
                          kafkaConnection: String)

case class StreamSeqTopic(stream: Stream,
                          kafkaName: String,
                          kafkaConnection: String,
                          topicInfo: Seq[SimpleTopic])

case class StreamSeqTopicActions(stream: Stream,
                                 kafkaName: String,
                                 kafkaConnection: String,
                                 topicInfo: Seq[SimpleTopic],
                                 disableActions: String)

case class SimpleTopic(id: Long,
                       name: String,
                       partitionOffsets: String,
                       rate: Int)

case class FeedbackOffsetInfo(streamId: Long,
                              topicName: String,
                              partitionId: Int,
                              offset: Long)


case class SimpleStream(name: String,
                        desc: Option[String] = None,
                        projectId: Long,
                        instanceId: Long,
                        streamType: String,
                        sparkConfig: Option[String] = None,
                        startConfig: String,
                        launchConfig: String,
                        topics: String) extends SimpleBaseEntity

case class Kafka(id: Long, name: String)

case class TopicSimple(id: Long, name: String, partitions: Int)

case class TopicDetail(id: Long,
                       streamId: Long,
                       nsInstanceId: Long,
                       nsDatabaseId: Long,
                       partition: Option[Int],
                       partitionOffsets: String,
                       rate: Int,
                       zookeeper: String,
                       active: Boolean,
                       createTime: String,
                       createBy: Long,
                       updateTime: String,
                       updateBy: Long,
                       name: String)

case class StartConfig(driverCores: Int,
                       driverMemory: Int,
                       executorNums: Int,
                       perExecutorMemory: Int,
                       perExecutorCores: Int)

case class StreamResource(name: String,
                          driverCores: Int,
                          driverMemory: Int,
                          executorNums: Int,
                          perExecutorMemory: Int,
                          perExecutorCores: Int)

case class Resource(totalCores: Int,
                    totalMemory: Int,
                    remainCores: Int,
                    remainMemory: Int,
                    stream: Seq[StreamResource])

case class LaunchConfig(maxRecords: String,
                        partitions: String,
                        durations: String)


case class StreamKafkaTopic(id: Long,
                            name: String,
                            streamType: String,
                            kafka: String,
                            topics: String)

case class StreamCacheMap(streamId: Long, streamName: String, projectId: Long)

case class TopicOffset(topicName: String, partition: Int, offset: Long)

case class StreamHealth(streamStatus: String,
                        sparkApplicationId: String,
                        latestSinkWaterMark: String,
                        batchThreshold: Int,
                        batchDurationSecond: Int,
                        topics: Seq[TopicOffset])

class StreamTable(_tableTag: Tag) extends BaseTable[Stream](_tableTag, "stream") {
  def * = (id, name, desc, projectId, instanceId, streamType, sparkConfig, startConfig, launchConfig, sparkAppid, logPath, status, startedTime, stoppedTime, active, createTime, createBy, updateTime, updateBy) <> (Stream.tupled, Stream.unapply)


  val name: Rep[String] = column[String]("name", O.Length(200, varying = true))
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column mq_instance_id SqlType(BIGINT) */
  val instanceId: Rep[Long] = column[Long]("instance_id")
  /** Database column stream_type SqlType(VARCHAR), Length(100,true) */
  val streamType: Rep[String] = column[String]("stream_type", O.Length(100, varying = true))
  /** Database column spark_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  val sparkConfig: Rep[Option[String]] = column[Option[String]]("spark_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column start_config SqlType(VARCHAR), Length(1000,true) */
  val startConfig: Rep[String] = column[String]("start_config", O.Length(1000, varying = true))
  /** Database column launch_config SqlType(VARCHAR), Length(1000,true) */
  val launchConfig: Rep[String] = column[String]("launch_config", O.Length(1000, varying = true))
  /** Database column spark_appid SqlType(VARCHAR), Length(200,true) */
  val sparkAppid: Rep[Option[String]] = column[Option[String]]("spark_appid", O.Length(200, varying = true), O.Default(None))
  /** Database column logPath SqlType(VARCHAR), Length(200,true) */
  val logPath: Rep[Option[String]] = column[Option[String]]("log_path", O.Length(200, varying = true), O.Default(None))
  /** Database column status SqlType(VARCHAR), Length(200,true) */
  val status: Rep[String] = column[String]("status", O.Length(200, varying = true))

  /** Database column update_time SqlType(TIMESTAMP) */
  val startedTime: Rep[Option[String]] = column[Option[String]]("started_time", O.Length(1000, varying = true), O.Default(None))
  /** Database column update_time SqlType(TIMESTAMP) */
  val stoppedTime: Rep[Option[String]] = column[Option[String]]("stopped_time", O.Length(1000, varying = true), O.Default(None))

  val index1 = index("name_UNIQUE", name, unique = true)
}




