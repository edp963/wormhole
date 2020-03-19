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


package edp.wormhole.sparkx.directive

import edp.wormhole.common.StreamType
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx.batchflow.BatchflowDirective
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.externalclient.zookeeper.WormholeZkClient._
import edp.wormhole.sparkx.hdfs.HdfsDirective
import edp.wormhole.sparkx.router.RouterDirective
import edp.wormhole.sparkxinterface.swifts.WormholeConfig
import edp.wormhole.ums.{UmsProtocolType, UmsSchemaUtils}

object DirectiveFlowWatch extends EdpLogging {

  val flowRelativePath = "/flow"

  val flowFeedbackRelativePath = "/feedback/directive/flow"

  def initFlow(config: WormholeConfig, appId: String): Unit = {
    logInfo("init flow,appId=" + appId)
    val watchPath = config.zookeeper_path + "/" + config.spark_config.stream_id + flowRelativePath
    if (!WormholeZkClient.checkExist(config.zookeeper_address, watchPath)) WormholeZkClient.createPath(config.zookeeper_address, watchPath)
    val flowList = WormholeZkClient.getChildren(config.zookeeper_address, watchPath)
    flowList.toArray.foreach(flow => {
      val flowContent = WormholeZkClient.getData(config.zookeeper_address, watchPath + "/" + flow)
      add(config.zookeeper_address, config.zookeeper_path)(watchPath + "/" + flow, new String(flowContent))
    })

    WormholeZkClient.setPathChildrenCacheListener(config.zookeeper_address, watchPath, add(config.zookeeper_address, config.zookeeper_path), remove(config.zookeeper_address), update(config.zookeeper_address, config.zookeeper_path))
  }

  def add(zkUrl: String, zkRootPath: String)(path: String, data: String, time: Long = 1): Unit = {
    try {
      logInfo("add" + data)
      if (path.contains("flow")) {
        val dataSplit = path.split("/")
        val streamId = dataSplit(dataSplit.length - 3)
        val flowInfo = dataSplit.last
        val flowDirectivePath = s"$zkRootPath$flowFeedbackRelativePath/$streamId/$flowInfo"
        if (!data.startsWith("{")) {
          logWarning("data is " + data + ", not in ums")
        } else {
          val ums = UmsSchemaUtils.toUms(data)
          ums.protocol.`type` match {
            case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_FLOW_STOP =>
              val feedbackMes = BatchflowDirective.flowStartProcess(ums)
              createAndSetData(zkUrl, flowDirectivePath, feedbackMes)
            case UmsProtocolType.DIRECTIVE_ROUTER_FLOW_START | UmsProtocolType.DIRECTIVE_ROUTER_FLOW_STOP =>
              val feedbackMes = RouterDirective.flowStartProcess(ums)
              createAndSetData(zkUrl, flowDirectivePath, feedbackMes)
            case UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_STOP =>
              val feedbackMes = HdfsDirective.flowStartProcess(ums) //todo change name uniform, take directiveflowwatch and directiveoffsetwatch out of core, because hdfs also use them
              createAndSetData(zkUrl, flowDirectivePath, feedbackMes)
            case UmsProtocolType.DIRECTIVE_HDFSCSV_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSCSV_FLOW_STOP =>
              val feedbackMes = HdfsDirective.flowStartProcess(ums) //todo change name uniform, take directiveflowwatch and directiveoffsetwatch out of core, because hdfs also use them
              createAndSetData(zkUrl, flowDirectivePath, feedbackMes)
            case _ => logWarning("ums type: " + ums.protocol.`type` + " is not supported")
          }
        }
      }
    } catch {
      case e: Throwable => logAlert("flow add error:" + data, e)
    }
  }

  def remove(zkUrl: String)(path: String): Unit = {
    try {
      val (streamType, sourceNamespace, sinkNamespace) = getNamespaces(path)
      StreamType.streamType(streamType) match {
        case StreamType.BATCHFLOW => ConfMemoryStorage.cleanDataStorage(sourceNamespace, sinkNamespace)
        case StreamType.HDFSLOG => ConfMemoryStorage.removeFromHdfslogMap(sourceNamespace)
        case StreamType.ROUTER => ConfMemoryStorage.removeFromRouterMap(sourceNamespace, sinkNamespace)
      }
    } catch {
      case e: Throwable => logAlert("flow remove error:", e)
    }
  }

  def update(zkUrl: String, zkRootPath: String)(path: String, data: String, time: Long): Unit = {
    try {
      logInfo("update" + data)
      add(zkUrl, zkRootPath)(path, data, time)
    } catch {
      case e: Throwable => logAlert("flow update error:" + data, e)
    }
  }

  private def getNamespaces(path: String): (String, String, String) = {
    val result = path.substring(path.lastIndexOf("/") + 1).split("->")
    (result(0).toLowerCase, result(2).toLowerCase, result(3))
  }
}
