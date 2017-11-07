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

object SparkAppStatus extends Enumeration {
  type SparkAppStatus = Value

  val STARTING = Value("STARTING")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val SUCCEEDED = Value("SUCCEEDED")
  val KILLED = Value("KILLED")
  val FINISHED = Value("FINISHED")

  def sparkAppStatus(s: String) = StreamStatus.withName(s.toUpperCase)
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
