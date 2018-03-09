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

import akka.http.scaladsl.model.StatusCodes.OK
import com.alibaba.fastjson.{JSON, JSONArray}
import edp.rider.RiderStarter.modules
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.kafka.{GetLatestOffsetException, KafkaUtils}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.NsDatabaseUtils._
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.rest.util.StreamUtils._
import edp.rider.zookeeper.PushDirective
import edp.wormhole.common.KVConfig
import edp.wormhole.common.util.CommonUtils._
import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.ums.UmsProtocolType._
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
      val sink_output =
        if (sinkConfig != "" && JSON.parseObject(sinkConfig).containsKey("sink_output"))
          JSON.parseObject(sinkConfig).getString("sink_output")
        else ""
      val dbConfig = getDbConfig(ns.nsSys, db.config.getOrElse(""))
      val sinkConnectionConfig =
        if (dbConfig.nonEmpty && dbConfig.get.nonEmpty)
          caseClass2json[Seq[KVConfig]](dbConfig.get)
        else "\"\""

      val sinkKeys = if (ns.nsSys == "hbase") getRowKey(specialConfig) else ns.keys.getOrElse("")

      if (ns.sinkSchema.nonEmpty && ns.sinkSchema.get != "") {
        val schema = caseClass2json[Object](json2caseClass[SinkSchema](ns.sinkSchema.get).schema)
        val base64 = base64byte2s(schema.trim.getBytes)

        s"""
           |{
           |"sink_connection_url": "${getConnUrl(instance, db)}",
           |"sink_connection_username": "${db.user.getOrElse("")}",
           |"sink_connection_password": "${db.pwd.getOrElse("")}",
           |"sink_table_keys": "$sinkKeys",
           |"sink_output": "$sink_output",
           |"sink_connection_config": $sinkConnectionConfig,
           |"sink_process_class_fullname": "${getSinkProcessClass(ns.nsSys, ns.sinkSchema)}",
           |"sink_specific_config": $specialConfig,
           |"sink_retry_times": "3",
           |"sink_retry_seconds": "300",
           |"sink_schema": "$base64"
           |}
       """.stripMargin.replaceAll("\n", "")
      } else {
        s"""
           |{
           |"sink_connection_url": "${getConnUrl(instance, db)}",
           |"sink_connection_username": "${db.user.getOrElse("")}",
           |"sink_connection_password": "${db.pwd.getOrElse("")}",
           |"sink_table_keys": "$sinkKeys",
           |"sink_output": "$sink_output",
           |"sink_connection_config": $sinkConnectionConfig,
           |"sink_process_class_fullname": "${getSinkProcessClass(ns.nsSys, ns.sinkSchema)}",
           |"sink_specific_config": $specialConfig,
           |"sink_retry_times": "3",
           |"sink_retry_seconds": "300"
           |}
       """.stripMargin.replaceAll("\n", "")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get sinkConfig failed", ex)
        throw ex
    }
  }

  def getTranConfig(tranConfig: String) = {
    if (tranConfig == "") "{}"
    else {
      val json = JSON.parseObject(tranConfig)
      if (json.containsKey("action")) {
        json.fluentPut("action", base64byte2s(JSON.parseObject(tranConfig).getString("action").trim.getBytes)).toString
        if (json.containsKey("pushdown_connection")) {
          json.fluentRemove("pushdown_connection")
          val seq = getPushDownConfig(tranConfig)
          val jsonArray = new JSONArray()
          seq.foreach(config => jsonArray.add(JSON.parseObject(caseClass2json[PushDownConnection](config))))
          json.fluentPut("pushdown_connection", jsonArray)
        }
        if (json.containsKey("swifts_specific_config")) {
          val swiftsSpecificConfig = json.get("swifts_specific_config")
          json.fluentPut("swifts_specific_config", swiftsSpecificConfig.toString)
        }
        json.toString
      } else tranConfig
    }
  }

  def getSinkProcessClass(nsSys: String, sinkSchema: Option[String]) = {
    nsSys match {
      case "cassandra" => "edp.wormhole.sinks.cassandrasink.Data2CassandraSink"
      case "mysql" | "oracle" | "postgresql" | "vertica" => "edp.wormhole.sinks.dbsink.Data2DbSink"
      case "es" =>
        if (sinkSchema.nonEmpty && sinkSchema.get != "") "edp.wormhole.sinks.elasticsearchsink.DataJson2EsSink"
        else "edp.wormhole.sinks.elasticsearchsink.Data2EsSink"
      case "hbase" => "edp.wormhole.sinks.hbasesink.Data2HbaseSink"
      case "kafka" =>
        if (sinkSchema.nonEmpty && sinkSchema.get != "") "edp.wormhole.sinks.kafkasink.DataJson2KafkaSink"
        else "edp.wormhole.sinks.kafkasink.Data2KafkaSink"
      case "mongodb" =>
        if (sinkSchema.nonEmpty && sinkSchema.get != "") "edp.wormhole.sinks.mongosink.DataJson2MongoSink"
        else "edp.wormhole.sinks.mongosink.Data2MongoSink"
      case "phoenix" => "edp.wormhole.sinks.phoenixsink.Data2PhoenixSink"
      case "parquet" => ""
    }
  }

  def actionRule(flowStream: FlowStream, action: String): FlowInfo = {
    if (flowStream.disableActions.contains("modify") && action == "refresh")
      FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, s"$action success.")
    else if (flowStream.disableActions.contains(action)) {
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
        if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.streamType, flowStream.sourceNs, flowStream.sinkNs, flowStream.tranConfig.getOrElse("")))
          FlowInfo(flowStream.id, "stopped", "renew,stop", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "stop failed")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "suspending", "renew") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "updating", "start", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "renew failed")

      case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "new" | "stopped" | "failed", "start") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "starting", "start", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "start failed")

      case ("running", "starting" | "updating" | "stopping" | "suspending" | "running" | "failed", "stop") =>
        if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.streamType, flowStream.sourceNs, flowStream.sinkNs, flowStream.tranConfig.getOrElse("")))
          FlowInfo(flowStream.id, "stopped", "renew,stop", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "stop failed")

      case ("running", "starting" | "updating" | "running" | "suspending", "renew") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "updating", "start", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "renew failed")

      case ("running", "new" | "stopped" | "failed", "start") =>
        if (startFlow(flowStream.streamId, flowStream.streamType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.updateBy))
          FlowInfo(flowStream.id, "starting", "start", s"$action success")
        else
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, "start failed")

      case (_, _, _) =>
        FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, s"$action isn't supported.")
    }
  }

  def getDisableActions(flowSeq: Seq[FlowStream]): mutable.HashMap[Long, String] = {
    val map = new mutable.HashMap[Long, String]()
    val projectNsMap = new mutable.HashMap[Long, Seq[String]]
    flowSeq.map(_.projectId).distinct.foreach(projectId =>
      projectNsMap(projectId) = modules.relProjectNsDal.getNsByProjectId(projectId)
    )
    flowSeq.foreach(flow =>
      map(flow.id) = getDisableActions(flow, projectNsMap(flow.projectId)))
    map
  }

  def getDisableActions(flow: FlowStream, projectNsSeq: Seq[String]): String = {

    val nsSeq = new ListBuffer[String]
    nsSeq += flow.sourceNs
    nsSeq += flow.sinkNs
    nsSeq ++= getDbFromTrans(flow.tranConfig).distinct
    nsSeq ++= getStreamJoinNamespaces(flow.tranConfig.getOrElse(""))
    var flag = true
    for (i <- nsSeq.indices) {
      if (!projectNsSeq.exists(_.startsWith(nsSeq(i))))
        flag = false
    }
    if (!flag) {
      if (flow.status == "stopped") "modify,start,renew,stopped"
      else "modify,start,renew"
    } else {
      flow.status match {
        case "new" => "renew,stop"
        case "starting" => "start,stop"
        case "running" => "start"
        case "updating" => "start,stop"
        case "suspending" => "start"
        case "stopping" => "start,renew,stop"
        case "stopped" => "renew,stop"
        case "failed" => ""
      }
    }
  }


  def startFlow(streamId: Long, streamType: String, flowId: Long, sourceNs: String, sinkNs: String, consumedProtocol: String, sinkConfig: String, tranConfig: String, userId: Long): Boolean = {
    try {
      autoDeleteTopic(userId, streamId)
      autoRegisterTopic(streamId, sourceNs, tranConfig, userId)
      val sourceNsObj = modules.namespaceDal.getNamespaceByNs(sourceNs).get
      val umsInfoOpt =
        if (sourceNsObj.sourceSchema.nonEmpty)
          json2caseClass[Option[SourceSchema]](modules.namespaceDal.getNamespaceByNs(sourceNs).get.sourceSchema.get)
        else None
      val umsType = umsInfoOpt match {
        case Some(umsInfo) => umsInfo.umsType.getOrElse("ums")
        case None => "ums"
      }
      val umsSchema = umsInfoOpt match {
        case Some(umsInfo) => umsInfo.umsSchema match {
          case Some(schema) => caseClass2json[Object](schema)
          case None => ""
        }
        case None => ""
      }
      if (streamType == "default") {
        val consumedProtocolSet = getConsumptionType(consumedProtocol)
        val sinkConfigSet = getSinkConfig(sinkNs, sinkConfig)
        val tranConfigFinal = getTranConfig(tranConfig)
        val tuple = Seq(streamId, currentMicroSec, umsType, umsSchema, sourceNs, sinkNs, consumedProtocolSet, sinkConfigSet, tranConfigFinal)
        val base64Tuple = Seq(streamId, currentMicroSec, umsType, base64byte2s(umsSchema.toString.trim.getBytes), sinkNs, base64byte2s(consumedProtocolSet.trim.getBytes),
          base64byte2s(sinkConfigSet.trim.getBytes), base64byte2s(tranConfigFinal.trim.getBytes))
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, streamId, flowId, "", RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_START.toString} success.")
        val flow_start_ums =
          s"""
             |{
             |"protocol": {
             |"type": "${
            DIRECTIVE_FLOW_START.toString
          }"
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
             |"name": "data_type",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "data_parse",
             |"type": "string",
             |"nullable": true
             |},
             |{
             |"name": "sink_namespace",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "consumption_protocol",
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
             |"tuple": [${
            directive.id
          }, ${
            base64Tuple.head
          }, "${
            base64Tuple(1)
          }", "${
            base64Tuple(2)
          }", "${
            base64Tuple(3)
          }", "${
            base64Tuple(4)
          }", "${
            base64Tuple(5)
          }", "${
            base64Tuple(6)
          }", "${
            base64Tuple(7)
          }"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendFlowStartDirective(streamId, sourceNs, sinkNs, jsonCompact(flow_start_ums))
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_FLOW_START.toString} directive to ${RiderConfig.zk} success.")
      } else if (streamType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs, "24", umsType, umsSchema)
        val base64Tuple = Seq(streamId, currentMillSec, sourceNs, "24", umsType, base64byte2s(umsSchema.toString.trim.getBytes))
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_HDFSLOG_FLOW_START.toString, streamId, flowId, tuple.mkString(","), RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_HDFSLOG_FLOW_START.toString} success.")
        val flow_start_ums =
          s"""
             |{
             |"protocol": {
             |"type": "${
            DIRECTIVE_HDFSLOG_FLOW_START.toString
          }"
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
             |},
             |{
             |"name": "data_type",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "data_parse",
             |"type": "string",
             |"nullable": true
             |}
             |]
             |},
             |"payload": [
             |{
             |"tuple": [${
            directive.id
          }, ${
            base64Tuple.head
          }, "${
            base64Tuple(1)
          }", "${
            base64Tuple(2)
          }", "${
            base64Tuple(3)
          }", "${
            base64Tuple(4)
          }", "${
            base64Tuple(5)
          }"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendHdfsLogFlowStartDirective(streamId, sourceNs, jsonCompact(flow_start_ums))
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_HDFSLOG_FLOW_START.toString} directive to ${RiderConfig.zk} success.")
      } else if (streamType == "routing") {
        val (instance, db, _) = modules.namespaceDal.getNsDetail(sinkNs)
        val tuple = Seq(streamId, currentMillSec, umsType, sinkNs, instance.connUrl, db.nsDatabase)
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_ROUTER_FLOW_START.toString, streamId, flowId, tuple.mkString(","), RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_HDFSLOG_FLOW_START.toString} success.")
        val flow_start_ums =
          s"""
             |{
             |  "protocol": {
             |    "type": "${DIRECTIVE_ROUTER_FLOW_START.toString}"
             |  },
             |  "schema": {
             |    "namespace": "$sourceNs",
             |    "fields": [
             |      {
             |        "name": "directive_id",
             |        "type": "long",
             |        "nullable": false
             |      },
             |      {
             |        "name": "stream_id",
             |        "type": "long",
             |        "nullable": false
             |      },
             |      {
             |        "name": "ums_ts_",
             |        "type": "datetime",
             |        "nullable": false
             |      },
             |      {
             |        "name": "data_type",
             |        "type": "string",
             |        "nullable": false
             |      },
             |      {
             |        "name": "sink_namespace",
             |        "type": "string",
             |        "nullable": false
             |      },
             |      {
             |        "name": "kafka_broker",
             |        "type": "string",
             |        "nullable": true
             |      },
             |      {
             |        "name": "kafka_topic",
             |        "type": "string",
             |        "nullable": false
             |      }
             |    ]
             |  },
             |  "payload": [
             |    {
             |      "tuple": [${directive.id}, ${tuple.head}, "${tuple(1)}", "${tuple(2)}", "${tuple(3)}","${tuple(4)}", "${tuple(5)}"]
             |    }
             |  ]
             |}
             |
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendRouterFlowStartDirective(streamId, sourceNs, sinkNs, jsonCompact(flow_start_ums))
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_HDFSLOG_FLOW_START.toString} directive to ${RiderConfig.zk} success.")
      }
      true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId start directive failed", ex)
        false
    }

  }

  def stopFlow(streamId: Long, flowId: Long, userId: Long, streamType: String, sourceNs: String, sinkNs: String, tranConfig: String): Boolean = {
    try {
      autoDeleteTopic(userId, streamId, Some(flowId))
      if (streamType == "default") {
        val tuple = Seq(streamId, currentMicroSec, sourceNs).mkString(",")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_STOP.toString, streamId, flowId, tuple, RiderConfig.zk, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_STOP.toString} success.")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendFlowStopDirective(streamId, sourceNs, sinkNs)
      } else if (streamType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs).mkString(",")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_HDFSLOG_FLOW_STOP.toString, streamId, flowId, tuple, RiderConfig.zk, currentSec, userId)), minTimeOut)
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendHdfsLogFlowStopDirective(streamId, sourceNs)
      } else if (streamType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs).mkString(",")
        val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_ROUTER_FLOW_STOP.toString, streamId, flowId, tuple, RiderConfig.zk, currentSec, userId)), minTimeOut)
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendRouterFlowStopDirective(streamId, sourceNs, sinkNs)
      }
      true
    } catch {
      case kafkaEx: GetLatestOffsetException => throw kafkaEx
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId stop directive failed", ex)
        false
    }
  }

  def autoRegisterTopic(streamId: Long, sourceNs: String, tranConfig: String, userId: Long) = {
    try {
      val streamJoinNs = getStreamJoinNamespaces(tranConfig)
      val nsSeq = (streamJoinNs += sourceNs).map(ns => modules.namespaceDal.getNamespaceByNs(ns).get)
      nsSeq.distinct.foreach(ns => {
        val topicSearch = Await.result(modules.inTopicDal.findByFilter(rel => rel.streamId === streamId && rel.nsDatabaseId === ns.nsDatabaseId), minTimeOut)
        if (topicSearch.isEmpty) {
          val instance = Await.result(modules.instanceDal.findByFilter(_.id === ns.nsInstanceId), minTimeOut).head
          val database = Await.result(modules.databaseDal.findByFilter(_.id === ns.nsDatabaseId), minTimeOut).head
          val lastConsumedOffset = Await.result(modules.feedbackOffsetDal.getLatestOffset(streamId, database.nsDatabase), minTimeOut)
          val offset =
            if (lastConsumedOffset.nonEmpty) lastConsumedOffset.get.partitionOffsets
            else KafkaUtils.getKafkaLatestOffset(instance.connUrl, database.nsDatabase)
          val inTopicInsert = StreamInTopic(0, streamId, ns.nsInstanceId, ns.nsDatabaseId, offset, RiderConfig.spark.topicDefaultRate,
            active = true, currentSec, userId, currentSec, userId)
          val inTopic = Await.result(modules.inTopicDal.insert(inTopicInsert), minTimeOut)
          sendTopicDirective(streamId, Seq(StreamTopicTemp(inTopic.id, streamId, database.nsDatabase, inTopic.partitionOffsets, inTopic.rate)), userId)
        }
      })
    }
    catch {
      case kafkaEx: GetLatestOffsetException =>
        throw kafkaEx
      case ex: Exception =>
        riderLogger.error(s"user $userId auto register topic to stream $streamId failed", ex)
        throw new Exception(ex)
    }
  }

  def flowMatch(projectId: Long, streamId: Long, sourceNs: String): Seq[String] = {
    val nsSplit = sourceNs.split("\\.")
    if (nsSplit(1).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(2).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.filter(ns => ns.nsInstance == nsSplit(1)).map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(3).trim == "*") {
      val nsSelect = Await.result(modules.relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
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

  def autoDeleteTopic(userId: Long, streamId: Long, flowId: Option[Long] = None) = {
    val flows = Await.result(modules.flowDal
      .findByFilter(flow => flow.streamId === streamId && flow.status =!= "new" && flow.status =!= "stopping" && flow.status =!= "stopped")
      , minTimeOut)
    val streamTopicIds = Await.result(modules.inTopicDal.findByFilter(_.streamId === streamId), minTimeOut).map(_.nsDatabaseId)
    val ids = new ListBuffer[Long]
    val flowSearch = if (flowId.isEmpty) flows else flows.filter(_.id != flowId.get)
    flowSearch.foreach(flow =>
      ids ++= (getStreamJoinNamespaces(flow.tranConfig.getOrElse("")) += flow.sourceNs)
        .map(ns => modules.namespaceDal.getNamespaceByNs(ns).get)
        .map(_.nsDatabaseId))
    val deleteIds = streamTopicIds.filterNot(ids.contains(_))
    if (deleteIds.nonEmpty) {
      val topicMap = Await.result(modules.databaseDal.findByFilter(_.id inSet deleteIds), minTimeOut).map(db => (db.id, db.nsDatabase)).toMap[Long, String]
      deleteIds.foreach(id => StreamUtils.sendUnsubscribeTopicDirective(streamId, topicMap(id), userId))
      Await.result(modules.inTopicDal.deleteByFilter(topic => (topic.nsDatabaseId inSet deleteIds) && (topic.streamId === streamId)), minTimeOut)
      riderLogger.info(s"drop topic ${
        topicMap.values.mkString(",")
      } directive")
    }
  }

  def getFlowsAndJobsByNsIds(projectId: Long, deleteNsIds: Seq[Long], inputNsIds: Seq[Long]): (mutable.HashMap[Long, Seq[String]], mutable.HashMap[Long, Seq[String]], Seq[Long]) = {
    val nsDeleteSearch = Await.result(modules.namespaceDal.findByFilter(_.id inSet deleteNsIds).mapTo[Seq[Namespace]], minTimeOut)
      .map(ns => (generateStandardNs(ns), ns.id)).toMap[String, Long]
    val nsInputSearch = Await.result(modules.namespaceDal.findByFilter(_.id inSet inputNsIds).mapTo[Seq[Namespace]], minTimeOut)
      .map(ns => generateStandardNs(ns))
    val nsDeleteSeq = nsDeleteSearch.keySet
    val notDeleteNsIds = new ListBuffer[Long]
    val flows = Await.result(modules.flowDal.findByFilter(flow => flow.projectId === projectId && flow.status =!= "stopped" && flow.status =!= "new" && flow.status =!= "failed"), minTimeOut)
    val jobs = Await.result(modules.jobDal.findByFilter(job => job.projectId === projectId && job.status =!= "stopped" && job.status =!= "new" && job.status =!= "failed"), minTimeOut)
    val flowNsMap = mutable.HashMap.empty[Long, Seq[String]]
    val jobNsMap = mutable.HashMap.empty[Long, Seq[String]]
    flows.foreach(flow => {
      val lookupDbs = NsDatabaseUtils.getDbFromTrans(flow.tranConfig)
      val streamJoinNs = getStreamJoinNamespaces(flow.tranConfig.getOrElse(""))
      val notDeleteNsSeq = new ListBuffer[String]
      if (nsDeleteSeq.contains(flow.sourceNs)) {
        notDeleteNsSeq += flow.sourceNs
        notDeleteNsIds += nsDeleteSearch(flow.sourceNs)
      }
      if (nsDeleteSeq.contains(flow.sinkNs)) {
        notDeleteNsSeq += flow.sinkNs
        notDeleteNsIds += nsDeleteSearch(flow.sinkNs)
      }
      lookupDbs.foreach(db => {
        if (!notDeleteNsSeq.exists(ns => ns.startsWith(db))) {
          if (!nsInputSearch.exists(ns => ns.startsWith(db))) {
            val lookupNsFind = nsDeleteSeq.filter(_.startsWith(db))
            if (lookupNsFind.nonEmpty) {
              notDeleteNsSeq += lookupNsFind.head
              notDeleteNsIds += nsDeleteSearch(lookupNsFind.head)
            }
          }
        }
      })
      streamJoinNs.foreach(streamJoin => {
        if (!notDeleteNsSeq.exists(ns => ns.startsWith(streamJoinNs))) {
          if (!nsInputSearch.exists(ns => ns.startsWith(streamJoinNs))) {
            val streamJoinNsFind = nsDeleteSeq.filter(_.startsWith(streamJoin))
            if (streamJoinNsFind.nonEmpty) {
              notDeleteNsSeq += streamJoinNsFind.head
              notDeleteNsIds += nsDeleteSearch(streamJoinNsFind.head)
            }
          }
        }
      })
      if (notDeleteNsSeq.nonEmpty)
        flowNsMap(flow.id) = notDeleteNsSeq.distinct
    })

    jobs.foreach(job => {
      val notDeleteNsSeq = new ListBuffer[String]
      if (nsDeleteSeq.contains(job.sourceNs)) {
        notDeleteNsSeq += job.sourceNs
        notDeleteNsIds += nsDeleteSearch(job.sourceNs)
      }
      if (nsDeleteSeq.contains(job.sinkNs)) {
        notDeleteNsSeq += job.sinkNs
        notDeleteNsIds += nsDeleteSearch(job.sinkNs)
      }
      if (notDeleteNsSeq.nonEmpty)
        jobNsMap(job.id) = notDeleteNsSeq.distinct
    })
    (flowNsMap, jobNsMap, notDeleteNsIds.distinct)
  }

  def getRowKey(sinkConfig: String): String = {
    val joinGrp = sinkConfig.split("\\+").map(_.trim)
    val rowKey = ListBuffer.empty[String]
    joinGrp.foreach(oneFieldPattern => {
      var subPatternContent = oneFieldPattern
      val keyOpts = ListBuffer.empty[(String, String)]
      if (subPatternContent.contains("(")) {
        while (subPatternContent.contains("(")) {
          val firstIndex = subPatternContent.indexOf("(")
          val keyOpt = subPatternContent.substring(0, firstIndex).trim
          val lastIndex = subPatternContent.lastIndexOf(")")
          subPatternContent = subPatternContent.substring(firstIndex + 1, lastIndex)
          val param = if (subPatternContent.trim.endsWith(")")) null.asInstanceOf[String]
          else {
            if (subPatternContent.contains("(")) {
              val subLastIndex = subPatternContent.lastIndexOf(")", lastIndex)
              val part = subPatternContent.substring(subLastIndex + 1)
              subPatternContent = subPatternContent.substring(0, subLastIndex + 1)
              if (part.contains(",")) part.trim.substring(1)
              else null.asInstanceOf[String]
            } else if (subPatternContent.contains(",")) {
              val tmpIndex = subPatternContent.indexOf(",")
              val tmp = subPatternContent.substring(tmpIndex + 1)
              subPatternContent = subPatternContent.substring(0, tmpIndex)
              tmp
            } else null.asInstanceOf[String]
          }
          keyOpts += ((keyOpt.toLowerCase, param))
        }
        rowKey += subPatternContent
      } else {
        rowKey += subPatternContent.replace("'", "").trim
      }
    })
    rowKey.distinct.filter(_ != "_").mkString(",")
  }


  //  def getNsSeqByTranConfig(tranConfig: String): Seq[String] = {
  //    val tableSeq = new ListBuffer[String]
  //    val sqls = if (tranConfig != "" && tranConfig != null) {
  //      val json = JSON.parseObject(tranConfig)
  //      if (json.containsKey("action")) {
  //        json.getString("action").split(";").filter(_.contains("pushdown_sql")).toList
  //      } else List()
  //    } else List()
  //    sqls.foreach(sql => tableSeq ++= getNsSeqByLookupSql(sql))
  //    tableSeq
  //  }

  //  def getNsSeqByLookupSql(sql: String): List[String] = {
  //    if (sql.contains("pushdown_sql")) {
  //      val sqlSplit = sql.split("with")(1).split("=")
  //      val db = sqlSplit(0).trim
  //      val pureSql = sqlSplit(1).trim
  //      getTables(pureSql).map(table => db + "." + table + ".*.*.*")
  //    } else List()
  //  }

  //  def getTables(sql: String): List[String] = {
  //    val regex = "([A-Za-z]+[A-Za-z0-9_-]*\\.){4,7}[A-Za-z0-9_-]+(,([A-Za-z]+[A-Za-z0-9_-]*\\.){4,7}[A-Za-z0-9_-]+)*".r.pattern
  //    val verify = sqlVerify(sql)
  //    if (verify._1) {
  //      val statement = CCJSqlParserUtil.parse(regex.matcher(sql).replaceAll("aaaaaaaaaaaaaaa"))
  //      val tablesNamesFinder = new TablesNamesFinder
  //      tablesNamesFinder.getTableList(statement).toList.distinct
  //    } else List()
  //  }

  //  def sqlVerify(sql: String): (Boolean, String) = {
  //    val regex = "([A-Za-z]+[A-Za-z0-9_-]*\\.){4,7}[A-Za-z0-9_-]+(,([A-Za-z]+[A-Za-z0-9_-]*\\.){4,7}[A-Za-z0-9_-]+)*".r.pattern
  //    try {
  //      val finalSql =
  //        if (sql.contains("pushdown_sql"))
  //          sql.split("=")(1).trim
  //        else sql
  //      CCJSqlParserUtil.parse(regex.matcher(finalSql).replaceAll("aaaaaaaaaaaaaaa"))
  //      (true, "")
  //    } catch {
  //      case sqlEx: JSQLParserException =>
  //        riderLogger.error(s"sql $sql is not regular, ${sqlEx.getMessage}")
  //        (false, sqlEx.getMessage)
  //      case ex: Exception =>
  //        riderLogger.error("sql verify failed", ex)
  //        (false, ex.getMessage)
  //    }
  //  }

  def getStreamJoinNamespaces(tranConfig: String): ListBuffer[String] = {
    val nsSeq = new ListBuffer[String]
    if (tranConfig != "" && tranConfig != null) {
      val json = JSON.parseObject(tranConfig)
      if (json.containsKey("action")) {
        val action = json.getString("action")
        val sqls = action.split(";").filter(_.trim.startsWith("parquet_sql"))
        if (sqls.nonEmpty) {
          val regex = "([A-Za-z]+[A-Za-z0-9_-]*\\.){3,6}[A-Za-z]+[A-Za-z0-9_-]*".r
          sqls.foreach(sql => nsSeq ++= regex.findAllIn(sql.split("=")(0).trim).toList)
        }
      }
    }
    nsSeq
  }
}
