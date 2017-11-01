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

import edp.rider.common.{GetZookeeperDataException, RiderConfig, RiderLogger}
import edp.wormhole.common.zookeeper.WormholeZkClient
import edp.rider.RiderStarter.modules
import kafka.utils.ZkUtils

case class SendDirectiveException(message: String) extends Exception(message)

object PushDirective extends RiderLogger {

  val rootPath = "/wormhole/"
  val flowDir = "/flow"
  val topicDir = "/offset/watch"
  val udfDir = "/udf"


  def sendFlowStartDirective(streamId: Long, sourceNamespace: String, sinkNamespace: String, flowStartJson: String, zkUrl: String = RiderConfig.zk): Boolean = {
    val path = s"$rootPath$streamId${flowDir}/batchflow->$sourceNamespace->$sinkNamespace"
    setDataToPath(zkUrl, path, flowStartJson)
  }

  def sendFlowStopDirective(streamId: Long, sourceNamespace: String, sinkNamespace: String, zkUrl: String = RiderConfig.zk): Unit = {
    val path = s"$rootPath$streamId${flowDir}/batchflow->$sourceNamespace->$sinkNamespace"
    deleteData(zkUrl, path)
  }

  def sendHdfsLogFlowStartDirective(streamId: Long, sourceNamespace: String, flowStartJson: String, zkUrl: String = RiderConfig.zk): Boolean = {
    val path = s"$rootPath$streamId${flowDir}/hdfslog->$sourceNamespace->$sourceNamespace"
    setDataToPath(zkUrl, path, flowStartJson)
  }

  def sendHdfsLogFlowStopDirective(streamId: Long, sourceNamespace: String, zkUrl: String = RiderConfig.zk): Unit = {
    val path = s"$rootPath$streamId${flowDir}/hdfslog->$sourceNamespace->$sourceNamespace"
    deleteData(zkUrl, path)
  }

  def sendRouterFlowStartDirective(streamId: Long, sourceNamespace: String, sinkNamespace: String, flowStartJson: String, zkUrl: String = RiderConfig.zk): Boolean = {
    val path = s"$rootPath$streamId${flowDir}/router->$sourceNamespace->$sinkNamespace"
    setDataToPath(zkUrl, path, flowStartJson)
  }

  def sendRouterFlowStopDirective(streamId: Long, sourceNamespace: String, sinkNamespace: String, zkUrl: String = RiderConfig.zk): Unit = {
    val path = s"$rootPath$streamId${flowDir}/router->$sourceNamespace->$sinkNamespace"
    deleteData(zkUrl, path)
  }

  def sendTopicDirective(streamId: Long, directiveList: String, zkUrl: String = RiderConfig.zk): Boolean = {
    val path = s"$rootPath$streamId$topicDir"
    //    riderLogger.info(s"topic zk path: $path")
    setDataToPath(zkUrl, path, directiveList)
  }

  def removeTopicDirective(streamId: Long, zkUrl: String = RiderConfig.zk): Unit = {
    val path = s"$rootPath$streamId$topicDir"
    deleteData(zkUrl, path)
  }

  def sendUdfDirective(streamId: Long, functionName: String, directive: String, zkUrl: String = RiderConfig.zk): Boolean = {
    val path = s"$rootPath$streamId$udfDir/$functionName"
    //    riderLogger.info(s"topic zk path: $path")
    setDataToPath(zkUrl, path, directive)
  }

  def removeUdfDirective(streamId: Long, functionName: Option[String], zkUrl: String = RiderConfig.zk): Unit = {
    val path =
      functionName match {
        case Some(function) => s"$rootPath$streamId$udfDir/$function"
        case None => s"$rootPath$streamId$udfDir"
      }
    deleteData(zkUrl, path)
  }

  def getUdfDirective(streamId: Long, zkUrl: String = RiderConfig.zk): Seq[String] = {
    val path = s"$rootPath$streamId$udfDir"
    getData(zkUrl, path)
  }

  private def getData(zkUrl: String = RiderConfig.zk, path: String): Seq[String] = {
    try {
      if (WormholeZkClient.checkExist(zkUrl, path)) {
        val childSeq = WormholeZkClient.getChildren(zkUrl, path)
        childSeq.map(child => new String(WormholeZkClient.getData(zkUrl, s"$path/$child")))
      }
      else throw GetZookeeperDataException(s"zk path $path didn't exist")
    } catch {
      case e: Exception =>
        riderLogger.error(s"get zk $zkUrl path $path data failed", e)
        throw GetZookeeperDataException(e.getMessage, e.getCause)
    }
  }

  private def deleteData(zkUrl: String = RiderConfig.zk, path: String): Unit = {
    try {
      WormholeZkClient.delete(zkUrl, path)
    } catch {
      case e: Exception =>
        riderLogger.error(s"delete zk $zkUrl path $path failed", e)
        throw SendDirectiveException(e.getMessage)
    }
  }

  private def setDataToPath(zkUrl: String = RiderConfig.zk, path: String, context: String): Boolean = {
    var rc = false
    try {
      if (!WormholeZkClient.checkExist(zkUrl, path))
        WormholeZkClient.createPath(zkUrl, path)
    } catch {
      case e: Exception =>
        riderLogger.error(s"create zk $zkUrl path $path failed", e)
        throw SendDirectiveException(e.getMessage)
    }
    try {
      if (WormholeZkClient.setData(zkUrl, path, context.getBytes()).getDataLength > 0)
        rc = true
    } catch {
      case e: Exception =>
        riderLogger.error(s"send flow start directive to zk $zkUrl failed", e)
        throw SendDirectiveException(e.getMessage)
    }
    rc
  }

}
