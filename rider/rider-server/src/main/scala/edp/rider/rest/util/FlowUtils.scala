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


package edp.rider.rest.util

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.kafka.KafkaUtils
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.StreamUtils._
import edp.rider.zookeeper.PushDirective
import edp.wormhole.common.util.CommonUtils._
import edp.wormhole.ums.UmsProtocolType._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await

object FlowUtils extends RiderLogger {

  def getConsumptionType(consType: String): String = {
    consType match {
      case "increment" => "{\"initial\": false, \"increment\": true, \"batch\": false}"
      case "initial" => "{\"initial\": true, \"increment\": false, \"batch\": false}"
      case "all" => "{\"initial\": true, \"increment\": true, \"batch\": false}"
    }
  }


  def getSinkConfig(sinkNs: String, sinkConfig: String): String = {
    try {
      val (instance, db, ns) = modules.namespaceDal.getNsDetail(sinkNs)
      val specialConfig =
        if (sinkConfig != "" && JSON.parseObject(sinkConfig).containsKey("sink_specific_config"))
          JSON.parseObject(sinkConfig).getString("sink_specific_config")
        else "{}"
      val dbConfig = "\"\""
      //      val dbConfig = if (db.config.getOrElse("") == "") "\"\"" else db.config.get
      s"""
         |{
         |"sink_connection_url": "${getConnUrl(instance, db)}",
         |"sink_connection_username": "${db.user.getOrElse("")}",
         |"sink_connection_password": "${db.pwd.getOrElse("")}",
         |"sink_table_keys": "${ns.keys.getOrElse("")}",
         |"sink_connection_config": $dbConfig,
         |"sink_process_class_fullname": "${getSinkProcessClass(ns.nsSys)}",
         |"sink_specific_config": $specialConfig,
         |"sink_retry_times": "3",
         |"sink_retry_seconds": "300"
         |}
       """.stripMargin.replaceAll("\n", "")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get sinkConfig failed", ex)
        throw ex
    }
  }

  def getSinkProcessClass(nsSys: String) =
    nsSys match {
      case "cassandra" => "edp.wormhole.sinks.cassandrasink.Data2CassandraSink"
      case "mysql" | "oracle" | "postgresql" => "edp.wormhole.sinks.dbsink.Data2DbSink"
      case "es" => "edp.wormhole.sinks.elasticsearchsink.Data2EsSink"
      case "hbase" => "edp.wormhole.sinks.hbasesink.Data2HbaseSink"
      case "kafka" => "edp.wormhole.sinks.kafkasink.Data2KafkaSink"
      case "mongodb" => "edp.wormhole.sinks.mongosink.Data2MongoSink"
      case "phoenix" => "edp.wormhole.sinks.phoenixsink.Data2PhoenixSink"
    }

  def actionRule(flowStream: FlowStream, action: String): FlowInfo = {
    if (flowStream.disableActions.split(",").contains(action)) {
      FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, s"$action operation is refused.")
    }
    else (flowStream.streamStatus, flowStream.status, action) match {
      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "starting" | "updating" | "suspending" | "running", "refresh" | "modify") =>
        FlowInfo(flowStream.id, "suspending", "start", s"$action success.")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "failed", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew", s"$action success.")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "new" | "stopped", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew,stop", s"$action success.")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "stopping", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew,start", s"$action success.")

      case ("running", "starting" | "updating" | "running" | "suspending", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "start", s"$action success.")

      case ("running", "failed", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew", s"$action success.")

      case ("running", "stopping", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew,start", s"$action success.")

      case ("running", "new" | "stopped", "refresh" | "modify") =>
        FlowInfo(flowStream.id, flowStream.status, "renew,stop", s"$action success.")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "suspending" | "failed" | "stopping", "stop") =>
        if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.streamType, flowStream.sourceNs, flowStream.sinkNs))
          FlowInfo(flowStream.id, "stopped", "renew,stop", "stop is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "stop failed")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "suspending", "renew") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "updating", "start", "renew is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "renew failed")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "new" | "stopped" | "failed", "start") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "starting", "start", "start is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "start failed")

      case ("running", "starting" | "updating" | "stopping" | "suspending" | "running" | "failed", "stop") =>
        if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.streamType, flowStream.sourceNs, flowStream.sinkNs))
          FlowInfo(flowStream.id, "stopped", "renew,stop", "stop is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "stop failed")

      case ("running", "starting" | "updating" | "running" | "suspending", "renew") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "updating", "start", "renew is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "renew failed")

      case ("running", "new" | "stopped" | "failed", "start") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "starting", "start", "start is processing")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "start failed")

      case (_, _, _) =>
        FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, s"$action isn't supported.")
    }
  }

  def startFlow(streamId: Long, streamType: String, flowId: Long, sourceNs: String, sinkNs: String, consumedProtocol: String, sinkConfig: String, tranConfig: String, userId: Long): Boolean = {
    try {
      if (streamType == "default") {
        val consumedProtocolSet = getConsumptionType(consumedProtocol)
        val sinkConfigSet = getSinkConfig(sinkNs, sinkConfig)
        val tranConfigFinal =
          if (tranConfig == "") "{}"
          else {
            if (JSON.parseObject(tranConfig).containsKey("action"))
              JSON.parseObject(tranConfig).fluentPut("action", base64byte2s(JSON.parseObject(tranConfig).getString("action").trim.getBytes)).toString
            else tranConfig
          }
        val tuple = Seq(streamId, currentMicroSec, sourceNs, sinkNs, consumedProtocolSet, sinkConfigSet, tranConfigFinal)
        val base64Tuple = Seq(streamId, currentMicroSec, sinkNs, base64byte2s(consumedProtocolSet.trim.getBytes),
          base64byte2s(sinkConfigSet.trim.getBytes), base64byte2s(tranConfigFinal.trim.getBytes))
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, streamId, flowId, tuple.mkString(","), RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_START.toString} success.")
        val flow_start_ums =
          s"""
             |{
             |"protocol": {
             |"type": "${DIRECTIVE_FLOW_START.toString}"
             |},
             |"schema": {
             |"namespace": "$sourceNs",
             |"fields": [
             |{
             |"name": "directive_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "stream_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "ums_ts_",
             |"type": "datetime",
             |"nullable": false
             |},
             |{
             |"name": "sink_namespace",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "consumption_data_type",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "sinks",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "swifts",
             |"type": "string",
             |"nullable": true
             |}
             |]
             |},
             |"payload": [
             |{
             |"tuple": [${directive.id}, ${base64Tuple.head}, "${base64Tuple(1)}", "${base64Tuple(2)}", "${base64Tuple(3)}", "${base64Tuple(4)}", "${base64Tuple(5)}"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${directive.createBy} send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendFlowStartDirective(streamId, sourceNs, sinkNs, flow_start_ums)
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_FLOW_START.toString} directive to ${RiderConfig.zk} success.")
      } else if (streamType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs, "24")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, streamId, flowId, tuple.mkString(","), RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_HDFSLOG_FLOW_START.toString} success.")
        val flow_start_ums =
          s"""
             |{
             |"protocol": {
             |"type": "${DIRECTIVE_HDFSLOG_FLOW_START.toString}"
             |},
             |"schema": {
             |"namespace": "$sourceNs",
             |"fields": [
             |{
             |"name": "directive_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "stream_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "ums_ts_",
             |"type": "datetime",
             |"nullable": false
             |},
             |{
             |"name": "namespace_rule",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "hour_duration",
             |"type": "string",
             |"nullable": false
             |}
             |]
             |},
             |"payload": [
             |{
             |"tuple": [${directive.id}, ${tuple.head}, "${tuple(1)}", "${tuple(2)}", "${tuple(3)}"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${directive.createBy} send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendHdfsLogFlowStartDirective(streamId, sourceNs, flow_start_ums)
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_HDFSLOG_FLOW_START.toString} directive to ${RiderConfig.zk} success.")
      }
      autoRegisterTopic(streamId, sourceNs, userId)
      true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId start directive failed", ex)
        false
    }

  }

  def stopFlow(streamId: Long, flowId: Long, userId: Long, streamType: String, sourceNs: String, sinkNs: String): Boolean = {
    try {
      val topicInfo = checkDeleteTopic(streamId, flowId, sourceNs)
      if (topicInfo._1) {
        StreamUtils.sendUnsubscribeTopicDirective(streamId, topicInfo._3, userId)
        Await.result(modules.inTopicDal.deleteByFilter(topic => topic.streamId === streamId && topic.nsDatabaseId === topicInfo._2), minTimeOut)
        riderLogger.info(s"drop topic ${topicInfo._3} directive")
      }
      if (streamType == "default") {
        val tuple = Seq(streamId, currentMicroSec, sourceNs).mkString(",")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_STOP.toString, streamId, flowId, tuple, RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_STOP.toString} success.")
        riderLogger.info(s"user ${directive.createBy} send flow $flowId stop directive")
        PushDirective.sendFlowStopDirective(streamId, sourceNs, sinkNs)
      } else if (streamType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs).mkString(",")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_HDFSLOG_FLOW_STOP.toString, streamId, flowId, tuple, RiderConfig.zk, currentSec, userId)), minTimeOut)
        riderLogger.info(s"user ${directive.createBy} send flow $flowId stop directive")
        PushDirective.sendHdfsLogFlowStopDirective(streamId, sourceNs)
      }
      true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId stop directive failed", ex)
        false
    }
  }

  def autoRegisterTopic(streamId: Long, sourceNs: String, userId: Long) = {
    try {
      val ns = modules.namespaceDal.getNamespaceByNs(sourceNs)
      val topicSearch = Await.result(modules.inTopicDal.findByFilter(rel => rel.streamId === streamId && rel.nsDatabaseId === ns.nsDatabaseId), minTimeOut)
      if (topicSearch.isEmpty) {
        val instance = Await.result(modules.instanceDal.findByFilter(_.id === ns.nsInstanceId), minTimeOut).head
        val database = Await.result(modules.databaseDal.findByFilter(_.id === ns.nsDatabaseId), minTimeOut).head
        val inTopicInsert = StreamInTopic(0, streamId, ns.nsInstanceId, ns.nsDatabaseId, KafkaUtils.getKafkaLatestOffset(instance.connUrl, database.nsDatabase), RiderConfig.spark.topicDefaultRate,
          active = true, currentSec, userId, currentSec, userId)
        val inTopic = Await.result(modules.inTopicDal.insert(inTopicInsert), minTimeOut)
        sendTopicDirective(streamId, Seq(StreamTopicTemp(inTopic.id, streamId, database.nsDatabase, inTopic.partitionOffsets, inTopic.rate)), userId)
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId auto register topic to stream $streamId failed", ex)
    }
  }

  def flowMatch(projectId: Long, streamId: Long, sourceNs: String): Seq[String] = {
    val nsSplit = sourceNs.split("\\.")
    if (nsSplit(1).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(2).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.filter(ns => ns.nsInstance == nsSplit(1)).map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(3).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.filter(ns => ns.nsInstance == nsSplit(1) && ns.nsDatabase == nsSplit(2)).map(ns => NamespaceUtils.generateStandardNs(ns))

    } else Seq(sourceNs)
  }

  def checkConfigFormat(sinkConfig: String, tranConfig: String) = {
    (isJson(sinkConfig), isJson(tranConfig)) match {
      case (true, true) => (true, "success")
      case (true, false) => (false, s"tranConfig $tranConfig is not json type")
      case (false, true) => (false, s"sinkConfig $sinkConfig is not json type")
      case (false, false) => (false, s"sinkConfig $sinkConfig, tranConfig $tranConfig both are not json type")
    }
  }

  def checkDeleteTopic(streamId: Long, flowId: Long, sourceNs: String) = {
    val flows = Await.result(modules.flowDal.findByFilter(_.streamId === streamId), minTimeOut)
    val ns = modules.namespaceDal.getNamespaceByNs(sourceNs)
    val topicName = Await.result(modules.databaseDal.findById(ns.nsDatabaseId), minTimeOut).get.nsDatabase
    val ids = flows.map(flow => modules.namespaceDal.getNamespaceByNs(flow.sourceNs)).filter(_.nsDatabaseId == ns.nsDatabaseId)
    if (ids.size > 1) (false, ns.nsDatabaseId, topicName)
    else (true, ns.nsDatabaseId, topicName)
  }
}
