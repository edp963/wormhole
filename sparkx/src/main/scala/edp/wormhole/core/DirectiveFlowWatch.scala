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


package edp.wormhole.core

import edp.wormhole.batchflow.BatchflowDirective
import edp.wormhole.common.WormholeConstants
import edp.wormhole.common.zookeeper.WormholeZkClient
import edp.wormhole.hdfslog.{HdfsDirective, HdfsMainProcess}
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.router.{RouterDirective, RouterMainProcess}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsProtocolType, UmsSchemaUtils}

object DirectiveFlowWatch extends EdpLogging {

  val flowRelativePath = "/flow"

  def initFlow(config: WormholeConfig, appId: String): Unit = {
    logInfo("init flow,appId=" + appId)

    val watchPath = WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + flowRelativePath
    if(!WormholeZkClient.checkExist(config.zookeeper_path, watchPath))WormholeZkClient.createPath(config.zookeeper_path, watchPath)
    val flowList = WormholeZkClient.getChildren(config.zookeeper_path, watchPath)
    flowList.toArray.foreach(flow => {
      val flowContent = WormholeZkClient.getData(config.zookeeper_path, watchPath + "/" + flow)
      add(config.kafka_output.feedback_topic_name,config.kafka_output.brokers)(watchPath + "/" + flow, new String(flowContent))
    })

    WormholeZkClient.setPathChildrenCacheListener(config.zookeeper_path, watchPath, add(config.kafka_output.feedback_topic_name,config.kafka_output.brokers), remove(config.kafka_output.brokers), update(config.kafka_output.feedback_topic_name,config.kafka_output.brokers))
  }

  def add(feedbackTopicName: String,brokers:String)(path: String, data: String, time: Long = 1): Unit = {
    try {
      logInfo("add"+data)
      val ums = UmsSchemaUtils.toUms(data)
      ums.protocol.`type` match {
        case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_FLOW_STOP =>
          BatchflowDirective.flowStartProcess(ums, feedbackTopicName,brokers)
        case UmsProtocolType.DIRECTIVE_ROUTER_FLOW_START | UmsProtocolType.DIRECTIVE_ROUTER_FLOW_STOP =>
          RouterDirective.flowStartProcess(ums, feedbackTopicName,brokers)
        case UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_STOP =>
          HdfsDirective.flowStartProcess(ums, feedbackTopicName,brokers)  //TOdo change name uniform, take directiveflowwatch and directiveoffsetwatch out of core, because hdfs also use them
        case _ => logWarning("ums type: " + ums.protocol.`type` + " is not supported")
      }
    } catch {
      case e: Throwable => logAlert("flow add error:" + data, e)
    }
  }

  def remove(brokers:String)(path: String): Unit = {
    try {
      val (streamType,sourceNamespace, sinkNamespace) = getNamespaces(path)
      StreamType.streamType(streamType) match {
        case StreamType.BATCHFLOW => ConfMemoryStorage.cleanDataStorage(sourceNamespace, sinkNamespace)
        case StreamType.HDFSLOG => HdfsMainProcess.directiveNamespaceRule.remove(sourceNamespace)
        case StreamType.ROUTER => RouterMainProcess.removeFromRouterMap(sourceNamespace,sinkNamespace)
        //case _=> logAlert("unsolve stream type:" + StreamType.ROUTER.toString)
      }
    } catch {
      case e: Throwable => logAlert("flow remove error:", e)
    }
  }

  def update(feedbackTopicName: String,brokers:String)(path: String, data: String, time: Long): Unit = {
    try {
      logInfo("update"+data)
      add(feedbackTopicName,brokers)(path, data, time)
    } catch {
      case e: Throwable => logAlert("flow update error:" + data, e)
    }
  }

  private def getNamespaces(path: String): (String, String,String) = {
    val result = path.substring(path.lastIndexOf("/")+1).split("->")
    (result(0).toLowerCase, result(1).toLowerCase,result(2))
  }
}
