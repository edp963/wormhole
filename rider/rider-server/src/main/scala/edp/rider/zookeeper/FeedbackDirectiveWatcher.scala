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


package edp.rider.zookeeper

import akka.actor.Actor
import edp.rider.RiderStarter.modules._
import edp.rider.common.FeedbackDirectiveType._
import edp.rider.common._
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.ums.{UmsFieldType, UmsSchemaUtils}


class FeedbackDirectiveWatcher extends Actor with RiderLogger {
  val zkAddress = RiderConfig.zk.address

  val feedbackDirectivePath = RiderConfig.zk.path + "/feedback/directive"

  override def receive: Receive = {
    case FeedbackWatch =>
      watch
    case Stop =>
      super.postStop()
  }


  def watch {
    try {
      val lock = WormholeZkClient.getInterProcessMutexLock(zkAddress, feedbackDirectivePath)
      lock.acquire()
      WormholeZkClient.setTreeCacheListener(zkAddress, feedbackDirectivePath, add, remove, update)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"lock zookeeper feedback directive path $feedbackDirectivePath failed", ex)
    }
  }

  // feedback flow directive path: /rootPath/clusterId/feedback/directive/flow/streamId/batchflow->flowId->sourceNs->sinkNs
  def add(path: String, data: String, time: Long): Unit = {
    try {
      if ((path.contains("flow") || path.contains("udf") || path.contains("topic")) && data.nonEmpty) {
        riderLogger.info(s"zk node_added, path: $path, data: $data, time: $time")
        val pathSplit = path.split("/")
        val feedbackType = pathSplit(pathSplit.size - 3)
        feedbackDirectiveType(feedbackType) match {
          case FLOW =>
            processFlowFeedback(path, data)
          case _ =>
            riderLogger.warn(s"$feedbackType feedback directive type can't support now.")
        }
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"process zk feedback directive message(path: $path, data: $data) failed", ex)
    }

  }

  def update(path: String, data: String, time: Long): Unit = {
    riderLogger.info(s"zk node_updated, path: $path, data: $data, time: $time")
    add(path, data, time)
  }

  def remove(path: String): Unit = {

  }

  def processFlowFeedback(path: String, data: String): Unit = {
    val flowId = path.split("/").last.split("->")(1).toLong
    val ums = UmsSchemaUtils.toUms(data)
    val status = UmsFieldType.umsFieldValue(ums.payload_get.head.tuple, ums.schema.fields_get, "status").toString
    if (status.trim.toLowerCase == "success")
      flowDal.updateStatusByFeedback(flowId, FlowStatus.RUNNING.toString)
    else
      flowDal.updateStatusByFeedback(flowId, FlowStatus.FAILED.toString)
    WormholeZkClient.delete(zkAddress, path)
  }

}
