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

import edp.rider.common.AppInfo
import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}

case class Stream(id: Long,
                  name: String,
                  desc: Option[String] = None,
                  projectId: Long,
                  instanceId: Long,
                  streamType: String,
                  functionType: String,
                  JVMDriverConfig: Option[String] = None,
                  JVMExecutorConfig: Option[String] = None,
                  othersConfig: Option[String] = None,
                  startConfig: String,
                  launchConfig: String,
                  specialConfig: Option[String] = None,
                  sparkAppid: Option[String] = None,
                  logPath: Option[String] = None,
                  status: String,
                  startedTime: Option[String] = None,
                  stoppedTime: Option[String] = None,
                  active: Boolean,
                  userTimeInfo: UserTimeInfo) extends BaseEntity {

  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }

  def updateFromSpark(appInfo: AppInfo) = {
    Stream(this.id, this.name, this.desc, this.projectId, this.instanceId, this.streamType, this.functionType, this.JVMDriverConfig,
      this.JVMExecutorConfig, this.othersConfig, this.startConfig, this.launchConfig, this.specialConfig,
      Option(appInfo.appId), this.logPath, appInfo.appState, Option(appInfo.startedTime), Option(appInfo.finishedTime),
      this.active, this.userTimeInfo)
  }
}

case class StreamDetail(stream: Stream,
                        projectName: String,
                        kafkaInfo: StreamKafka,
                        topicInfo: Option[GetTopicsResponse],
                        currentUdf: Seq[StreamUdf],
                        disableActions: String,
                        hideActions: String)


case class StreamSpecialConfig(
                                useDefaultKey: Option[Boolean]
                              )

case class StreamKafka(instance: String, connUrl: String)

case class StreamUdfResponse(id: Long, streamId: Long, functionName: String, fullClassName: String, jarName: String)

case class StreamUdf(id: Long, functionName: String, fullClassName: String, jarName: String)

case class StreamZkUdfTemp(streamId: Long, functionName: String, fullClassName: String, jarName: String)

case class StreamZkUdf(functionName: String, fullClassName: String, jarName: String)

case class PutTopicDirective(name: String, partitionOffsets: String, rate: Int, action: Option[Int])

case class PutStreamTopic(autoRegisteredTopics: Seq[PutTopicDirective], userDefinedTopics: Seq[PutTopicDirective])

case class GetTopicsOffsetRequest(autoRegisteredTopics: Seq[String], userDefinedTopics: Seq[String])

case class StreamDirective(udfInfo: Seq[Long], topicInfo: Option[PutStreamTopic])

case class ConsumedLatestOffset(id: Long, name: String, rate: Int, partitionOffsets: String)

case class KafkaLatestOffset(id: Long, name: String, partitionOffsets: String)

case class TopicLatestOffset(consumedLatestOffset: Seq[ConsumedLatestOffset], kafkaLatestOffset: Seq[KafkaLatestOffset])


case class TopicAllOffsets(id: Long,
                           name: String,
                           rate: Int,
                           consumedLatestOffset: String,
                           kafkaEarliestOffset: String,
                           kafkaLatestOffset: String)

case class SimpleTopicAllOffsets(name: String,
                                 rate: Int,
                                 consumedLatestOffset: String,
                                 kafkaEarliestOffset: String,
                                 kafkaLatestOffset: String)

case class GetTopicsOffsetResponse(autoRegisteredTopics: Seq[SimpleTopicAllOffsets], userDefinedTopics: Seq[SimpleTopicAllOffsets])

case class GetTopicsResponse(autoRegisteredTopics: Seq[TopicAllOffsets], userDefinedTopics: Seq[TopicAllOffsets])

case class StreamTopicTemp(id: Long,
                           streamId: Long,
                           name: String,
                           partitionOffsets: String,
                           rate: Int)

case class UpdateTopicOffset(id: Long, offset: String)

case class StreamTopic(id: Long,
                       name: String,
                       partitionOffsets: String,
                       rate: Int)

case class FeedbackOffsetInfo(streamId: Long,
                              topicName: String,
                              partitionId: Int,
                              offset: Long)

case class StreamIdKafkaUrl(streamId: Long, kafkaUrl: String)

case class SimpleStream(name: String,
                        desc: Option[String] = None,
                        instanceId: Long,
                        streamType: String,
                        functionType: String,
                        JVMDriverConfig: Option[String] = None,
                        JVMExecutorConfig: Option[String] = None,
                        othersConfig: Option[String] = None,
                        startConfig: String,
                        launchConfig: String,
                        specialConfig: Option[String] = None) extends SimpleBaseEntity

case class PutStream(id: Long,
                     desc: Option[String] = None,
                     JVMDriverConfig: Option[String] = None,
                     JVMExecutorConfig: Option[String] = None,
                     othersConfig: Option[String] = None,
                     startConfig: String,
                     launchConfig: String,
                     specialConfig: Option[String] = None)

case class StartConfig(driverCores: Int,
                       driverMemory: Int,
                       executorNums: Int,
                       perExecutorMemory: Int,
                       perExecutorCores: Int)


case class LaunchConfig(maxRecords: String,
                        partitions: String,
                        durations: String)

case class StreamCacheMap(streamId: Long, streamName: String, projectId: Long)

case class TopicOffset(topicName: String,
                       consumedLatestOffset: String,
                       kafkaEarliestOffset: String,
                       kafkaLatestOffset: String)

case class StreamTopicOffset(streamId: Long, topicName: String, partitionOffsets: String)

case class StreamHealth(streamStatus: String,
                        sparkApplicationId: String,
                        latestSinkWaterMark: String,
                        batchThreshold: Int,
                        batchDurationSecond: Int,
                        topics: Seq[TopicOffset])

case class StreamInfo(name: String, appId: Option[String], streamType: String, functionType: String, status: String)

case class StreamSelect(id: Long,
                        name: String,
                        kafkaInstance: String,
                        topicInfo: Seq[String])

case class SimpleStreamInfo(id: Long,
                            name: String,
                            maxParallelism: Int,
                            kafkaInstance: String,
                            topicInfo: Seq[String]
                           )

case class StartResponse(id: Long,
                         status: String,
                         disableActions: String,
                         hideActions: String,
                         appId: Option[String] = None,
                         startedTime: Option[String] = None,
                         stoppedTime: Option[String] = None
                        )


//   "startConfig" : "{\"driverCores\":1,\"driverMemory\":1,\"executorNums\":1,\"perExecutorMemory\":1,\"perExecutorCores\":1}",
//"launchConfig" : "{\"durations\":30,\"partitions\":1,\"maxRecords\":10}",
//get default config response
case class SparkResourceConfig(driverCores: Int,
                               driverMemory: Int,
                               executorNums: Int,
                               perExecutorCores: Int,
                               perExecutorMemory: Int,
                               durations: Int,
                               partitions: Int,
                               maxRecords: Int)

case class SparkDefaultConfig(JVMDriverConfig: String, JVMExecutorConfig: String, spark: SparkResourceConfig, othersConfig: String)

case class FlinkResourceConfig(jobManagerMemoryGB: Int,
                               taskManagersNumber: Int,
                               perTaskManagerSlots: Int,
                               perTaskManagerMemoryGB: Int)

case class FlinkDefaultConfig(jvm: String, flink: FlinkResourceConfig, others: String)

case class RiderJVMConfig(JVMDriverConfig: String,
                          JVMExecutorConfig: String)

class StreamTable(_tableTag: Tag) extends BaseTable[Stream](_tableTag, "stream") {
  def * = (id, name, desc, projectId, instanceId, streamType, functionType,jvmDriverConfig , jvmExecutorConfig, othersConfig, startConfig, launchConfig, specialConfig,
    sparkAppid, logPath, status, startedTime, stoppedTime, active, (createTime, createBy, updateTime, updateBy)).shaped <> ({
    case (id, name, desc, projectId, instanceId, streamType, functionType, jvmDriverConfig, jvmExecutorConfig, othersConfig, startConfig, launchConfig, specialConfig,
  sparkAppid, logPath, status, startedTime, stoppedTime, active, userTimeInfo) =>
      Stream(id, name, desc, projectId, instanceId, streamType, functionType, jvmDriverConfig, jvmExecutorConfig, othersConfig, startConfig, launchConfig, specialConfig,
    sparkAppid, logPath, status, startedTime, stoppedTime, active, UserTimeInfo.tupled.apply(userTimeInfo))},
    { s: Stream => def f1(c: UserTimeInfo) = UserTimeInfo.unapply(c).get
    Some((s.id, s.name, s.desc, s.projectId, s.instanceId, s.streamType, s.functionType, s.JVMDriverConfig, s.JVMExecutorConfig, s.othersConfig,
      s.startConfig, s.launchConfig, s.specialConfig, s.sparkAppid, s.logPath, s.status, s.startedTime, s.stoppedTime, s.active, f1(s.userTimeInfo)))
  })


  val name: Rep[String] = column[String]("name", O.Length(200, varying = true))
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column mq_instance_id SqlType(BIGINT) */
  val instanceId: Rep[Long] = column[Long]("instance_id")
  /** Database column stream_type SqlType(VARCHAR), Length(100,true) */
  val streamType: Rep[String] = column[String]("stream_type", O.Length(100, varying = true))
  /** Database column function_type SqlType(VARCHAR), Length(100,true) */
  val functionType: Rep[String] = column[String]("function_type", O.Length(100, varying = true))
  /** Database column jvm_driver_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  val jvmDriverConfig: Rep[Option[String]] = column[Option[String]]("jvm_driver_config", O.Length(1000, varying = true), O.Default(None))
  /** Database column jvm_executor_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  val jvmExecutorConfig: Rep[Option[String]] = column[Option[String]]("jvm_executor_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column others_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  val othersConfig: Rep[Option[String]] = column[Option[String]]("others_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column start_config SqlType(VARCHAR), Length(1000,true) */
  val startConfig: Rep[String] = column[String]("start_config", O.Length(1000, varying = true))
  /** Database column launch_config SqlType(VARCHAR), Length(1000,true) */
  val launchConfig: Rep[String] = column[String]("launch_config", O.Length(1000, varying = true))
  /** Database column special_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  val specialConfig: Rep[Option[String]] = column[Option[String]]("special_config", O.Length(1000, varying = true), O.Default(None))
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




