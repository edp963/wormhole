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


package edp.rider.common

object UserRoleType extends Enumeration {
  type UserRoleType = Value

  val ADMIN = Value("admin")
  val USER = Value("user")
  val APP = Value("app")

  def userRowType(s: String) = UserRoleType.withName(s.toLowerCase)
}


object FlowStatus extends Enumeration {
  type FlowStatus = Value

  val NEW = Value("new")
  val STARTING = Value("starting")
  val RUNNING = Value("running")
  val UPDATING = Value("updating")
  val STOPPING = Value("stopping")
  val STOPPED = Value("stopped")
  val FAILED = Value("failed")
  val SUSPENDING = Value("suspending")

  def flowStatus(s: String) = FlowStatus.withName(s.toLowerCase)
}

object Action extends Enumeration {
  type Action = Value

  val REFRESH = Value("refresh")
  val START = Value("start")
  val STOP = Value("stop")
  val MODIFY = Value("modify")
  val RENEW = Value("renew")
  val DELETE = Value("delete")
  val BATCHSELECT = Value("batchSelect")
  val DRIFT = Value("drift")


  def action(s: String) = Action.withName(s.toLowerCase)
}

object StreamRefresh extends Enumeration {
  type StreamRefresh = Value

  val REFRESH = Value("refresh")
  val REFRESHLOG = Value("refresh_log")
  val REFRESHSPARK = Value("refresh_spark")

}

object StreamStatus extends Enumeration {
  type StreamStatus = Value

  val NEW = Value("new")
  val STARTING = Value("starting")
  val WAITING = Value("waiting")
  val RUNNING = Value("running")
  val STOPPING = Value("stopping")
  val STOPPED = Value("stopped")
  val FAILED = Value("failed")
  val FINISHED = Value("finished")
  val DONE = Value("done")

  def streamStatus(s: String) = StreamStatus.withName(s.toLowerCase)
}

object DbPermission extends Enumeration {
  type DbPermission = Value

  val READONLY = Value("ReadOnly")
  val READWRITE = Value("ReadWrite")

  def dbPermission(s: String) = DbPermission.withName(s)
}

object YarnAppStatus extends Enumeration {
  type YarnAppStatus = Value

  val STARTING = Value("STARTING")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val SUCCEEDED = Value("SUCCEEDED")
  val KILLED = Value("KILLED")
  val FINISHED = Value("FINISHED")
  val FAILED = Value("FAILED")
  val CANCELED = Value("CANCELED")

  def yarnAppStatus(s: String) = YarnAppStatus.withName(s.toUpperCase)
}


object JobStatus extends Enumeration {
  type JobStatus = Value

  val NEW = Value("new")
  val STARTING = Value("starting")
  val WAITING = Value("waiting")
  val RUNNING = Value("running")
  val STOPPING = Value("stopping")
  val STOPPED = Value("stopped")
  val FAILED = Value("failed")
  val DONE = Value("done")

  def jobStatus(s: String) = JobStatus.withName(s.toLowerCase)
}

object JobType extends Enumeration {
  type JobType = Value

  val DEFAULT = Value("1")
  val BACKFILL = Value("2")

  def getJobType(s: String) = JobType.withName(s.toLowerCase)
}

object ProtocolType extends Enumeration {
  type JobType = Value

  val INCREMENT = Value("1")
  val INITIAL = Value("2")
  val BATCH = Value("3")

  def getProtocolType(s: String) = ProtocolType.withName(s.toLowerCase)
}

object JobSinkProtocol extends Enumeration {
  type JobSinkProtocol = Value

  val SNAPSHOT = Value("snapshot")

  def getJobSinkProtocol(s: String) = JobSinkProtocol.withName(s.toLowerCase)
}

object FunctionType extends Enumeration {
  type FunctionType = Value
  val DEFAULT = Value("default")
  val HDFSLOG = Value("hdfslog")
  val ROUTIING = Value("routing")
  val HDFSCSV = Value("hdfscsv")

  def functionType(s: String) = FunctionType.withName(s.toLowerCase)

}

object StreamType extends Enumeration {
  type StreamType = Value
  val SPARK = Value("spark")
  val FLINK = Value("flink")

  def streamType(s: String) = StreamType.withName(s.toLowerCase)

}

object KafkaVersion extends Enumeration{
  type KafkaVersion = Value

  val KAFKA_MIN = Value("0.10.0.0")
  val KAFKA_010 = Value("0.10.0.0")
  val KAFKA_0102= Value("0.10.2.2")
  val KAFKA_UNKOWN= Value("")

  def kafkaVersion(s: String) = KafkaVersion.withName(s.toLowerCase)

}

object FeedbackDirectiveType extends Enumeration {
  type FeedbackDirectiveType = Value
  val FLOW = Value("flow")
  val UDF = Value("udf")
  val TOPIC = Value("topic")

  def feedbackDirectiveType(s: String) = FeedbackDirectiveType.withName(s.toLowerCase)
}