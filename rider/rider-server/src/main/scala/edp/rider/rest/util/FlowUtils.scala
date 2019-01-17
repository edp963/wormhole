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

import scala.concurrent.ExecutionContext.Implicits.global
import com.alibaba.fastjson.{JSON, JSONArray}
import edp.rider.RiderStarter.modules._
import edp.rider.common._
import edp.rider.kafka.KafkaUtils
import edp.rider.kafka.KafkaUtils.{getKafkaEarliestOffset, getKafkaLatestOffset, getKafkaOffsetByGroupId}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.NsDatabaseUtils._
import edp.rider.rest.util.StreamUtils._
import edp.rider.yarn.YarnClientLog
import edp.rider.yarn.YarnStatusQuery._
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.wormhole._
import edp.rider.zookeeper.PushDirective
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.util.JsonUtils._
import edp.wormhole.util.CommonUtils._
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.config.KVConfig
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.util.{Failure, Success}
import scalaj.http.{Http, HttpResponse}

object FlowUtils extends RiderLogger {

  def getConsumptionType(consType: String): String = {
    val consumedTypeFormat = consType.split(",").map(_.trim).sorted.mkString(",")
    consumedTypeFormat match {
      case "increment" => "{\"initial\": false, \"increment\": true, \"batch\": false}"
      case "initial" => "{\"initial\": true, \"increment\": false, \"batch\": false}"
      case "backfill" => "{\"initial\": false, \"increment\": false, \"batch\": true}"
      case "increment,initial" => "{\"initial\": true, \"increment\": true, \"batch\": false}"
      case "backfill,increment" => "{\"initial\": false, \"increment\": true, \"batch\": true}"
      case "backfill,initial" => "{\"initial\": true, \"increment\": false, \"batch\": true}"
      case "backfill,increment,initial" => "{\"initial\": true, \"increment\": true, \"batch\": true}"
    }
  }

  def getSinkConfig(sinkNs: String, sinkConfig: String, tableKeys: String): String = {
    try {
      val (instance, db, ns) = namespaceDal.getNsDetail(sinkNs)
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

      //val sinkKeys = if (ns.nsSys == "hbase") getRowKey(specialConfig) else ns.keys.getOrElse("")
      val sinkKeys = if (ns.nsSys == "hbase") getRowKey(specialConfig) else tableKeys

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
      case "mysql" | "oracle" | "postgresql" | "vertica" | "greenplum" => "edp.wormhole.sinks.dbsink.Data2DbSink"
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
      case "kudu" => "edp.wormhole.sinks.kudusink.Data2KuduSink"
    }
  }

  def actionRule(flowStream: FlowStream, action: String): FlowInfo = {
    if (flowStream.disableActions.contains("modify") && action == "refresh")
      FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, s"$action success.")
    else if (flowStream.disableActions.contains(action)) {
      FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, s"$action operation is refused.")
    }
    else if (flowStream.streamType == StreamType.SPARK.toString) {
      (flowStream.streamStatus, flowStream.status, action) match {
        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "starting" | "updating" | "suspending" | "running", "refresh" | "modify") =>
          FlowInfo(flowStream.id, "suspending", "start", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "failed", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "new" | "stopped", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,start,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "starting", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,stop,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "updating" | "running" | "suspending", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "failed", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,start,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "new" | "stopped", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "suspending" | "failed" | "stopping", "stop") =>
          if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.functionType, flowStream.sourceNs, flowStream.sinkNs, flowStream.tranConfig.getOrElse("")))
            FlowInfo(flowStream.id, "stopped", "renew,stop", flowStream.startedTime, Option(currentSec), s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "stop failed")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "suspending", "renew") =>
          if (startFlow(flowStream.streamId, flowStream.functionType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.tableKeys.getOrElse(""), flowStream.updateBy))
            FlowInfo(flowStream.id, "updating", "start", Option(currentSec), None, s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "renew failed")

        case ("new" | "starting" | "waiting" | "failed" | "stopped" | "stopping", "new" | "stopped" | "failed", "start") =>
          if (startFlow(flowStream.streamId, flowStream.functionType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.tableKeys.getOrElse(""), flowStream.updateBy))
            FlowInfo(flowStream.id, "starting", "start,delete", Option(currentSec), None, s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "start failed")

        case ("running", "starting" | "updating" | "stopping" | "suspending" | "running" | "failed", "stop") =>
          if (stopFlow(flowStream.streamId, flowStream.id, flowStream.updateBy, flowStream.functionType, flowStream.sourceNs, flowStream.sinkNs, flowStream.tranConfig.getOrElse("")))
            FlowInfo(flowStream.id, "stopped", "renew,stop", flowStream.startedTime, Option(currentSec), s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "stop failed")

        case ("running", "starting" | "updating" | "running" | "suspending", "renew") =>
          if (startFlow(flowStream.streamId, flowStream.functionType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.tableKeys.getOrElse(""), flowStream.updateBy))
            FlowInfo(flowStream.id, "updating", "start", Option(currentSec), None, s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "renew failed")

        case ("running", "new" | "stopped" | "failed", "start") =>
          if (startFlow(flowStream.streamId, flowStream.functionType, flowStream.id, flowStream.sourceNs, flowStream.sinkNs, flowStream.consumedProtocol, flowStream.sinkConfig.getOrElse(""), flowStream.tranConfig.getOrElse(""), flowStream.tableKeys.getOrElse(""), flowStream.updateBy))
            FlowInfo(flowStream.id, "starting", "start,delete", Option(currentSec), None, s"$action success")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "start failed")

        case (_, _, _) =>
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, s"$action isn't supported.")
      }
    } else {
      (flowStream.streamStatus, flowStream.status, action) match {
        case ("new" | "starting" | "waiting", "starting", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew,stop,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting", "new" | "running" | "stopped", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting", "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("new" | "starting" | "waiting", "failed", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("stopping" | "stopped" | "failed", "new" | "stopped", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("stopping" | "stopped" | "failed", "failed", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("stopping", "starting" | "running" | "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, "stopping", "start,renew,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("failed", "starting" | "running" | "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, "failed", "start,renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("stopped", "starting" | "running" | "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, "stopped", "start,renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "new", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "starting", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew,stop,delete", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "running" | "stopping", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "start,renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "stopped", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew,stop", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "failed", "refresh" | "modify") =>
          FlowInfo(flowStream.id, flowStream.status, "renew", flowStream.startedTime, flowStream.stoppedTime, s"$action success.")

        case ("running", "new" | "stopped" | "failed", "start") =>
          if (startFlinkFlow(flowStream.streamAppId.get, getFlowByFlowStream(flowStream)))
            FlowInfo(flowStream.id, "starting", "start,renew,stop,delete", Option(currentSec), None, s"$action success.")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "start failed")

        case (_, "failed", "stop") =>
          FlowInfo(flowStream.id, "stopped", "renew,stop", flowStream.startedTime, Option(currentSec), s"$action success.")

        case ("running", "running" | "stopping", "stop") =>
          if (stopFlinkFlow(flowStream.streamAppId.get, getFlowName(flowStream.id, flowStream.sourceNs, flowStream.sinkNs)))
            FlowInfo(flowStream.id, "stopping", "start,renew,delete", flowStream.startedTime, Option(currentSec), s"$action success.")
          else
            FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, "stop failed")
        case (_, _, _) =>
          FlowInfo(flowStream.id, flowStream.status, flowStream.disableActions, flowStream.startedTime, flowStream.stoppedTime, s"$action isn't supported.")
      }
    }
  }

  def getDisableActions(flow: Flow): String = {
    getDisableActions(Seq(flow))(flow.id)
  }

  def getDisableActions(flowSeq: Seq[Flow]): mutable.HashMap[Long, String] = {
    val map = new mutable.HashMap[Long, String]()
    val projectNsMap = new mutable.HashMap[Long, Seq[String]]
    flowSeq.map(_.projectId).distinct.foreach(projectId =>
      projectNsMap(projectId) = relProjectNsDal.getNsByProjectId(projectId)
    )
    flowSeq.foreach(flow =>
      map(flow.id) = getDisableActions(flow, projectNsMap(flow.projectId)))
    map
  }

  def getHideActions(streamType: String, functionType: String): String = {
    StreamType.withName(streamType) match {
      case StreamType.FLINK => s"${Action.RENEW},${Action.BATCHSELECT},${Action.DRIFT}"
      case StreamType.SPARK =>
        FunctionType.withName(functionType) match {
          case FunctionType.HDFSLOG | FunctionType.ROUTIING =>
            s"${Action.DRIFT}"
          case FunctionType.DEFAULT => ""
        }
    }
  }

  def getDisableActions(flow: Flow, projectNsSeq: Seq[String]): String = {

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
        case "starting" => "start,delete,drift"
        case "running" => "start"
        case "updating" => "start,drift"
        case "suspending" => "start"
        case "stopping" => "start,renew,delete,drift"
        case "stopped" => "renew,stop"
        case "failed" => ""
      }
    }
  }


  def startFlow(streamId: Long, functionType: String, flowId: Long, sourceNs: String, sinkNs: String, consumedProtocol: String, sinkConfig: String, tranConfig: String, tableKeys: String, userId: Long): Boolean = {
    try {
      autoDeleteTopic(userId, streamId)
      autoRegisterTopic(streamId, sourceNs, tranConfig, userId)
      val sourceNsObj = namespaceDal.getNamespaceByNs(sourceNs).get
      val umsInfoOpt =
        if (sourceNsObj.sourceSchema.nonEmpty)
          json2caseClass[Option[SourceSchema]](namespaceDal.getNamespaceByNs(sourceNs).get.sourceSchema.get)
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
      if (functionType == "default") {
        val consumedProtocolSet = getConsumptionType(consumedProtocol)
        val sinkConfigSet = getSinkConfig(sinkNs, sinkConfig, tableKeys)
        val tranConfigFinal = getTranConfig(tranConfig)
        //        val tuple = Seq(streamId, currentMicroSec, umsType, umsSchema, sourceNs, sinkNs, consumedProtocolSet, sinkConfigSet, tranConfigFinal)
        val base64Tuple = Seq(streamId, flowId, currentMicroSec, umsType, base64byte2s(umsSchema.toString.trim.getBytes), sinkNs, base64byte2s(consumedProtocolSet.trim.getBytes),
          base64byte2s(sinkConfigSet.trim.getBytes), base64byte2s(tranConfigFinal.trim.getBytes), RiderConfig.kerberos.enabled)
        val directiveFuture = directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId))
        directiveFuture onComplete {
          case Success(directive) =>
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
                 |"name": "flow_id",
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
                 |},
                 |{
                 |"name": "kerberos",
                 |"type": "boolean",
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
              }","${
                base64Tuple(8)
              }","${
                base64Tuple(9)
              }"]
                 |}
                 |]
                 |}
        """.stripMargin.replaceAll("\n", "")
            riderLogger.info(s"user ${
              directive.createBy
            } send flow $flowId start directive: $flow_start_ums")
            PushDirective.sendFlowStartDirective(flowId, streamId, sourceNs, sinkNs, jsonCompact(flow_start_ums))
          //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_FLOW_START.toString} directive to ${RiderConfig.zk.address} success.")
          case Failure(ex) =>
            riderLogger.error(s"send ${DIRECTIVE_FLOW_START.toString} directive to ${RiderConfig.zk.address} failed", ex)
            false
        }
      }
      else if (functionType == "hdfslog") {
        //        val tuple = Seq(streamId, currentMillSec, sourceNs, "24", umsType, umsSchema)
        val base64Tuple = Seq(streamId, flowId, currentMillSec, sourceNs, "24", umsType, base64byte2s(umsSchema.toString.trim.getBytes))
        val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_HDFSLOG_FLOW_START.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId)), minTimeOut)
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
             |"name": "flow_id",
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
          }", "${
            base64Tuple(6)
          }"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendHdfsLogFlowStartDirective(flowId, streamId, sourceNs, jsonCompact(flow_start_ums))
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_HDFSLOG_FLOW_START.toString} directive to ${RiderConfig.zk.address} success.")
      } else if (functionType == "routing") {
        val (instance, db, _) = namespaceDal.getNsDetail(sinkNs)
        val tuple = Seq(streamId, flowId, currentMillSec, umsType, sinkNs, instance.connUrl, db.nsDatabase)
        val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_ROUTER_FLOW_START.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId)), minTimeOut)
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
             |        "name": "flow_id",
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
             |      "tuple": [${directive.id}, ${tuple.head}, "${tuple(1)}", "${tuple(2)}", "${tuple(3)}","${tuple(4)}", "${tuple(5)}", "${tuple(6)}"]
             |    }
             |  ]
             |}
             |
        """.stripMargin.replaceAll("\n", "")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId start directive: $flow_start_ums")
        PushDirective.sendRouterFlowStartDirective(flowId, streamId, sourceNs, sinkNs, jsonCompact(flow_start_ums))
        //        riderLogger.info(s"user ${directive.createBy} send ${DIRECTIVE_HDFSLOG_FLOW_START.toString} directive to ${RiderConfig.zk.address} success.")
      }
      true
    }
    catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId start directive failed", ex)
        false
    }

  }

  def stopFlow(streamId: Long, flowId: Long, userId: Long, functionType: String, sourceNs: String, sinkNs: String, tranConfig: String): Boolean = {
    try {
      autoDeleteTopic(userId, streamId, Some(flowId))
      if (functionType == "default") {
        val tuple = Seq(streamId, currentMicroSec, sourceNs).mkString(",")
        val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_FLOW_STOP.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId)), minTimeOut)
        //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_STOP.toString} success.")
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendFlowStopDirective(flowId, streamId, sourceNs, sinkNs)
      } else if (functionType == "hdfslog") {
        val tuple = Seq(streamId, currentMillSec, sourceNs).mkString(",")
        val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_HDFSLOG_FLOW_STOP.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId)), minTimeOut)
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendHdfsLogFlowStopDirective(flowId, streamId, sourceNs)
      } else if (functionType == "routing") {
        val tuple = Seq(streamId, currentMillSec, sourceNs).mkString(",")
        val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_ROUTER_FLOW_STOP.toString, streamId, flowId, "", RiderConfig.zk.address, currentSec, userId)), minTimeOut)
        riderLogger.info(s"user ${
          directive.createBy
        } send flow $flowId stop directive")
        PushDirective.sendRouterFlowStopDirective(flowId, streamId, sourceNs, sinkNs)
      }
      true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId send flow $flowId stop directive failed", ex)
        false
    }
  }

  def autoRegisterTopic(streamId: Long, sourceNs: String, tranConfig: String, userId: Long) = {
    try {
      val streamJoinNs = getStreamJoinNamespaces(tranConfig)
      val nsSeq = (streamJoinNs += sourceNs).map(ns => namespaceDal.getNamespaceByNs(ns).get)
      nsSeq.distinct.foreach(ns => {
        val topicSearch = Await.result(streamInTopicDal.findByFilter(rel => rel.streamId === streamId && rel.nsDatabaseId === ns.nsDatabaseId), minTimeOut)
        if (topicSearch.isEmpty) {
          val instance = Await.result(instanceDal.findByFilter(_.id === ns.nsInstanceId), minTimeOut).head
          val database = Await.result(databaseDal.findByFilter(_.id === ns.nsDatabaseId), minTimeOut).head
          val lastConsumedOffset = Await.result(feedbackOffsetDal.getLatestOffset(streamId, database.nsDatabase), minTimeOut)
          val offset =
            if (lastConsumedOffset.nonEmpty) lastConsumedOffset.get.partitionOffsets
            else KafkaUtils.getKafkaLatestOffset(instance.connUrl, database.nsDatabase, RiderConfig.kerberos.enabled)
          val inTopicInsert = StreamInTopic(0, streamId, ns.nsDatabaseId, offset, RiderConfig.spark.topicDefaultRate,
            active = true, currentSec, userId, currentSec, userId)
          val inTopic = Await.result(streamInTopicDal.insert(inTopicInsert), minTimeOut)
          sendTopicDirective(streamId, Seq(PutTopicDirective(database.nsDatabase, inTopic.partitionOffsets, inTopic.rate, None)), userId, false)
        }
      })
    }
    catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId auto register topic to stream $streamId failed", ex)
        throw new Exception(ex)
    }
  }

  def flowMatch(projectId: Long, streamId: Long, sourceNs: String): Seq[String] = {
    val nsSplit = sourceNs.split("\\.")
    if (nsSplit(1).trim == "*") {
      val nsSelect = Await.result(relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(2).trim == "*") {
      val nsSelect = Await.result(relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
      nsSelect.filter(ns => ns.nsInstance == nsSplit(1)).map(ns => NamespaceUtils.generateStandardNs(ns))
    } else if (nsSplit(3).trim == "*") {
      val nsSelect = Await.result(relProjectNsDal.getFlowSourceNamespaceByProjectId(projectId, streamId, nsSplit(0)), minTimeOut)
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
    val flows = Await.result(flowDal
      .findByFilter(flow => flow.streamId === streamId && flow.status =!= "new" && flow.status =!= "stopping" && flow.status =!= "stopped")
      , minTimeOut)
    val streamTopicIds = Await.result(streamInTopicDal.findByFilter(_.streamId === streamId), minTimeOut).map(_.nsDatabaseId)
    val ids = new ListBuffer[Long]
    val flowSearch = if (flowId.isEmpty) flows else flows.filter(_.id != flowId.get)
    flowSearch.foreach(flow =>
      ids ++= (getStreamJoinNamespaces(flow.tranConfig.getOrElse("")) += flow.sourceNs)
        .map(ns => namespaceDal.getNamespaceByNs(ns).get)
        .map(_.nsDatabaseId))
    val deleteIds = streamTopicIds.filterNot(ids.contains(_))
    if (deleteIds.nonEmpty) {
      val topicMap = Await.result(databaseDal.findByFilter(_.id inSet deleteIds), minTimeOut).map(db => (db.id, db.nsDatabase)).toMap[Long, String]
      deleteIds.foreach(id => StreamUtils.sendUnsubscribeTopicDirective(streamId, topicMap(id), userId))
      Await.result(streamInTopicDal.deleteByFilter(topic => (topic.nsDatabaseId inSet deleteIds) && (topic.streamId === streamId)), minTimeOut)
      riderLogger.info(s"drop topic ${
        topicMap.values.mkString(",")
      } directive")
    }
  }

  def getFlowsAndJobsByNsIds(projectId: Long, deleteNsIds: Seq[Long], inputNsIds: Seq[Long]): (mutable.HashMap[Long, Seq[String]], mutable.HashMap[Long, Seq[String]], Seq[Long]) = {
    val nsDeleteSearch = Await.result(namespaceDal.findByFilter(_.id inSet deleteNsIds).mapTo[Seq[Namespace]], minTimeOut)
      .map(ns => (generateStandardNs(ns), ns.id)).toMap[String, Long]
    val nsInputSearch = Await.result(namespaceDal.findByFilter(_.id inSet inputNsIds).mapTo[Seq[Namespace]], minTimeOut)
      .map(ns => generateStandardNs(ns))
    val nsDeleteSeq = nsDeleteSearch.keySet
    val notDeleteNsIds = new ListBuffer[Long]
    val flows = Await.result(flowDal.findByFilter(flow => flow.projectId === projectId && flow.status =!= "stopped" && flow.status =!= "new" && flow.status =!= "failed"), minTimeOut)
    val jobs = Await.result(jobDal.findByFilter(job => job.projectId === projectId && job.status =!= "stopped" && job.status =!= "new" && job.status =!= "failed"), minTimeOut)
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

  def startFlinkFlow(appId: String, flow: Flow) = {
    try {
      val commandSh = generateFlinkFlowStartSh(appId, flow)
      riderLogger.info(s"start flow ${flow.id} command: $commandSh")
      runShellCommand(commandSh)
      true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"flow ${flow.id} start failed", ex)
        false
    }
  }

  def generateFlinkFlowStartSh(appId: String, flow: Flow): String = {
    val address = getJobManagerAddressOnYarn(appId)
    riderLogger.info(s"Flow ${flow.id} JobManager address: $address")
    val config1 = getWhFlinkConfig(flow)
    val config2 = getFlinkFlowConfig(flow)
    val logPath = getLogPath(getFlowName(flow.id, flow.sourceNs, flow.sinkNs))
    flowDal.updateLogPath(flow.id, logPath)
    s"""
       |ssh -p${RiderConfig.spark.sshPort} ${RiderConfig.spark.user}@${RiderConfig.riderServer.host}
       |${RiderConfig.flink.homePath}/bin/flink run
       |-m $address ${RiderConfig.flink.jarPath} '${config1}' '${config2}'
       |> $logPath 2>&1
     """.stripMargin.replaceAll("\n", " ").trim
  }

  def getWhFlinkConfig(flow: Flow) = {
    val kafkaUrl = StreamUtils.getKafkaByStreamId(flow.streamId)
    val baseConfig = KafkaBaseConfig(getFlowName(flow.id, flow.sourceNs, flow.sinkNs), kafkaUrl, RiderConfig.flink.kafkaSessionTimeOut, RiderConfig.flink.kafkaGroupMaxSessionTimeOut)
    val outputConfig = KafkaOutputConfig(RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.brokers)
    val autoRegisteredTopics = flowInTopicDal.getAutoRegisteredTopics(Seq(flow.id)).map(topic => KafkaFlinkTopic(topic.topicName, topic.partitionOffsets))
    val userDefinedTopics = flowUdfTopicDal.getUdfTopics(Seq(flow.id)).map(topic => KafkaFlinkTopic(topic.topicName, topic.partitionOffsets))
    val flinkTopic = autoRegisteredTopics ++ userDefinedTopics
    val udfConfig: Seq[FlowUdfResponse] = flowUdfDal.getFlowUdf(Seq(flow.id))
    val config = WhFlinkConfig(getFlowName(flow.id, flow.sourceNs, flow.sinkNs), KafkaInput(baseConfig, flinkTopic), outputConfig, flow.parallelism.getOrElse(RiderConfig.flink.defaultParallelism), RiderConfig.zk.address, udfConfig, RiderConfig.flink.feedbackEnabled, RiderConfig.flink.feedbackStateCount, RiderConfig.flink.feedbackInterval, FlinkConfig(RiderConfig.flinkCheckpoint), RiderConfig.kerberos.enabled)
    caseClass2json[WhFlinkConfig](config)
  }

  def getFlinkFlowConfig(flow: Flow): String = {
    val consumedProtocol = getConsumptionType(flow.consumedProtocol)
    val sinkConfig = getSinkConfig(flow.sinkNs, flow.sinkConfig.get, flow.tableKeys.getOrElse(""))
    val tranConfigFinal = getTranConfig(flow.tranConfig.getOrElse(""))

    val sourceNsObj = namespaceDal.getNamespaceByNs(flow.sourceNs).get
    val umsInfoOpt =
      if (sourceNsObj.sourceSchema.nonEmpty)
        json2caseClass[Option[SourceSchema]](namespaceDal.getNamespaceByNs(flow.sourceNs).get.sourceSchema.get)
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

    val base64Tuple = Seq(flow.streamId, flow.id, currentNodMicroSec, umsType, base64byte2s(umsSchema.toString.trim.getBytes), flow.sinkNs, base64byte2s(consumedProtocol.trim.getBytes),
      base64byte2s(sinkConfig.trim.getBytes), base64byte2s(tranConfigFinal.trim.getBytes))
    val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, flow.streamId, flow.id, "", RiderConfig.zk.address, currentSec, flow.updateBy)), minTimeOut)
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
         |"namespace": "${flow.sourceNs}",
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
         |"name": "job_id",
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
      }", "${
        base64Tuple(8)
      }"]
         |}
         |]
         |}
        """.stripMargin.replaceAll("\n", "")
    jsonCompact(flow_start_ums)
  }

  def getFlowName(flowId: Long, sourceNs: String, sinkNs: String): String =
    if (RiderConfig.riderServer.clusterId != "") s"${RiderConfig.riderServer.clusterId}-$sourceNs-$sinkNs".toLowerCase
    else s"$sourceNs-$sinkNs".toLowerCase

  def updateUdfsByStart(flowId: Long, udfIds: Seq[Long], userId: Long): Unit = {
    if (udfIds.nonEmpty) {
      val deleteUdfIds = flowUdfDal.getDeleteUdfIds(flowId, udfIds)
      Await.result(flowUdfDal.deleteByFilter(udf => udf.flowId === flowId && udf.udfId.inSet(deleteUdfIds)), minTimeOut)
      val insertUdfs = udfIds.map(
        id => FlowUdf(0, flowId, id, currentSec, userId, currentSec, userId)
      )
      Await.result(flowUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
    } else {
      Await.result(flowUdfDal.deleteByFilter(_.flowId === flowId), minTimeOut)
    }
  }

  def updateTopicsByStart(flowId: Long, putTopic: PutFlowTopic, userId: Long): Unit = {
    val autoRegisteredTopics = putTopic.autoRegisteredTopics
    val userDefinedTopics = putTopic.userDefinedTopics
    // update auto registered topics
    flowInTopicDal.updateByStart(flowId, autoRegisteredTopics, userId)
    // delete user defined topics by start
    flowUdfTopicDal.deleteByStart(flowId, userDefinedTopics)
    // insert or update user defined topics by start
    flowUdfTopicDal.insertUpdateByStart(flowId, userDefinedTopics, userId)
  }

  def stopFlinkFlow(appId: String, flowName: String): Boolean = {
    try {
      val jobId = getFlinkJobStatusOnYarn(Seq(appId))(flowName).jobId
      val activeRm = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
      val url = s"http://$activeRm/proxy/$appId/jobs/$jobId/yarn-cancel"
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      if (response.isSuccess) {
        riderLogger.info(s"stop flink flow $flowName success.")
        true
      } else {
        riderLogger.error(s"stop flink flow $flowName by request url $url failed", response.body)
        false
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stop flink flow $flowName by request failed", ex)
        false
    }
  }

  private def getFlowByFlowStream(flowStream: FlowStream): Flow

  = {
    Flow(flowStream.id, flowStream.flowName, flowStream.projectId, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
      flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.status, flowStream.startedTime, flowStream.stoppedTime,
      flowStream.logPath, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
      flowStream.updateBy)
  }

  def getFlowStatusByYarn(flowStreams: Seq[FlowStream]): Seq[FlowStream] = {
    try {
      val flowYarnMap = getFlinkJobStatusOnYarn(
        flowStreams.filter(stream => stream.streamType == StreamType.FLINK.toString && stream.streamStatus == StreamStatus.RUNNING.toString)
          .map(_.streamAppId.get).distinct)
      flowStreams.map {
        flowStream =>
          if (flowStream.streamType == StreamType.FLINK.toString && flowStream.streamStatus == StreamStatus.RUNNING.toString) {
            val flowName = getFlowName(flowStream.id, flowStream.sourceNs, flowStream.sinkNs)
            val logStatus =
              if (FlowStatus.withName(flowStream.status) == FlowStatus.STARTING)
                getFlowStatusByLog(flowName, flowStream.logPath.getOrElse(""), flowStream.status)
              else flowStream.status
            val yarnFlow = if (!flowYarnMap.contains(flowName) && FlowStatus.withName(logStatus) == FlowStatus.STOPPING) {
              FlinkFlowStatus(FlowStatus.STOPPED.toString, flowStream.startedTime, flowStream.stoppedTime)
            } else if (flowYarnMap.contains(flowName) && flowStream.startedTime.orNull != null && yyyyMMddHHmmss(flowYarnMap(flowName).startTime) > yyyyMMddHHmmss(flowStream.startedTime.get)) {
              getFlowStatusByYarnAndLog(FlinkFlowStatus(logStatus, flowStream.startedTime, flowStream.stoppedTime), flowYarnMap(flowName))
            } else FlinkFlowStatus(logStatus, flowStream.startedTime, flowStream.stoppedTime)
            FlowStream(flowStream.id, flowStream.flowName, flowStream.projectId, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
              flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, yarnFlow.status, yarnFlow.startTime, yarnFlow.stopTime,
              flowStream.logPath, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
              flowStream.updateBy, flowStream.streamName, flowStream.streamAppId, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowStream.disableActions, flowStream.hideActions,
              flowStream.topicInfo, flowStream.currentUdf, flowStream.msg)
          } else flowStream
      }
    } catch {
      case ex: Exception =>
        riderLogger.error("refresh flow on yarn failed", ex)
        flowStreams
    }
  }

  private def getFlowStatusByYarnAndLog(dbInfo: FlinkFlowStatus, yarnInfo: FlinkJobStatus): FlinkFlowStatus

  = {
    YarnAppStatus.withName(yarnInfo.state) match {
      case YarnAppStatus.ACCEPTED =>
        FlinkFlowStatus(FlowStatus.STARTING.toString, dbInfo.startTime, dbInfo.stopTime)
      case YarnAppStatus.RUNNING =>
        FlinkFlowStatus(FlowStatus.RUNNING.toString, dbInfo.startTime, dbInfo.stopTime)
      case YarnAppStatus.CANCELED | YarnAppStatus.KILLED | YarnAppStatus.FINISHED | YarnAppStatus.FAILED =>
        if (FlowStatus.withName(dbInfo.status) == FlowStatus.RUNNING || FlowStatus.withName(dbInfo.status) == FlowStatus.STARTING) {
          FlinkFlowStatus(FlowStatus.FAILED.toString, dbInfo.startTime, Option(yarnInfo.stopTime))
        } else if (FlowStatus.withName(dbInfo.status) == FlowStatus.STOPPING) {
          FlinkFlowStatus(FlowStatus.STOPPED.toString, dbInfo.startTime, Option(yarnInfo.stopTime))
        } else {
          dbInfo
        }
      case _ => dbInfo
    }
  }

  def getLogPath(flowName: String) = s"${RiderConfig.flink.clientLogPath}/$flowName-$currentNodSec.log"

  def getFlowStatusByLog(flowName: String, logPath: String, preStatus: String): String = {
    val failedPattern = "The program finished with the following exception".r
    try {
      val fileLines = YarnClientLog.getLogByAppName(flowName, logPath)
      if (failedPattern.findFirstIn(fileLines).nonEmpty)
        FlowStatus.FAILED.toString
      else preStatus
    }
    catch {
      case ex: Exception =>
        riderLogger.warn(s"Refresh flow $flowName status from client log failed", ex)
        preStatus
    }
  }

  def updateStatusByStreamStop(streamId: Long, streamType: String, streamStatus: String): Int = {
    val flows = Await.result(flowDal.findByFilter(_.streamId === streamId), minTimeOut)
    StreamType.withName(streamType) match {
      case StreamType.SPARK =>
        val flowIds = flows.filter(flow =>
          flow.status == FlowStatus.RUNNING.toString || flow.status == FlowStatus.STARTING.toString || flow.status == FlowStatus.UPDATING.toString)
          .map(_.id)
        Await.result(flowDal.updateStatusByStreamStop(flowIds, FlowStatus.SUSPENDING.toString), minTimeOut)
      case StreamType.FLINK =>
        if (streamStatus == StreamStatus.STOPPING.toString) {
          val flowIds = flows.filter(flow =>
            flow.status == FlowStatus.RUNNING.toString || flow.status == FlowStatus.STARTING.toString)
            .map(_.id)
          Await.result(flowDal.updateStatusByStreamStop(flowIds, FlowStatus.STOPPING.toString), minTimeOut)
        } else if (streamStatus == StreamStatus.STOPPED.toString) {
          val flowIds = flows.filter(flow =>
            flow.status == FlowStatus.RUNNING.toString || flow.status == FlowStatus.STARTING.toString || flow.status == FlowStatus.STOPPING.toString)
            .map(_.id)
          Await.result(flowDal.updateStatusByStreamStop(flowIds, FlowStatus.STOPPED.toString), minTimeOut)
        } else {
          val flowIds = flows.filter(flow =>
            flow.status == FlowStatus.RUNNING.toString || flow.status == FlowStatus.STARTING.toString || flow.status == FlowStatus.STOPPING.toString)
            .map(_.id)
          Await.result(flowDal.updateStatusByStreamStop(flowIds, FlowStatus.FAILED.toString), minTimeOut)
        }
    }
  }

  def formatConsumedOffsetByGroup(offset: String): String = {
    offset.split(",").map(partOffset => partOffset.split(":")(0) + ":").mkString(",")
  }

  def getLog(flowId: Long): String = {
    val flow = Await.result(flowDal.findById(flowId), minTimeOut).get
    val flowName = getFlowName(flow.id, flow.sourceNs, flow.sinkNs)
    YarnClientLog.getLogByAppName(flowName, flow.logPath.getOrElse(""))
  }

  def getFlowTopicsMap(flowIds: Seq[Long]): Map[Long, GetTopicsResponse] = {
    val autoRegisteredTopics = flowInTopicDal.getAutoRegisteredTopics(flowIds)
    val udfTopics = flowUdfTopicDal.getUdfTopics(flowIds)
    val kafkaMap = flowDal.getFlowKafkaMap(flowIds)
    flowIds.map(id => {
      //      val topics = autoRegisteredTopics.filter(_.flowId == id) ++: udfTopics.filter(_.flowId == id)
      //val feedbackOffsetMap = getConsumedMaxOffset(id, topics)

      val autoTopicsResponse = genFlowAllOffsets(autoRegisteredTopics, kafkaMap)
      val udfTopicsResponse = genFlowAllOffsets(udfTopics, kafkaMap)

      (id, GetTopicsResponse(autoTopicsResponse, udfTopicsResponse))
    }).toMap
  }

  def genFlowAllOffsets(topics: Seq[FlowTopicTemp], kafkaMap: Map[Long, String]): Seq[TopicAllOffsets] = {
    topics.map(topic => {
      val earliest = getKafkaEarliestOffset(kafkaMap(topic.flowId), topic.topicName, RiderConfig.kerberos.enabled)
      val latest = getKafkaLatestOffset(kafkaMap(topic.flowId), topic.topicName, RiderConfig.kerberos.enabled)
      val consumedLatestOffset =
        try {
          val flow = Await.result(flowDal.findById(topic.flowId), minTimeOut).get
          val flowName = FlowUtils.getFlowName(flow.id, flow.sourceNs, flow.sinkNs)
          getKafkaOffsetByGroupId(kafkaMap(topic.flowId), topic.topicName, flowName)
        } catch {
          case _: Exception =>
            formatConsumedOffsetByGroup(latest)
        }
      TopicAllOffsets(topic.id, topic.topicName, topic.rate,
        KafkaUtils.formatConsumedOffsetByLatestOffset(consumedLatestOffset, latest), earliest, latest)
    })
  }

  def getDriftTip(preFlowStream: FlowStream, streamId: Long): (Boolean, String) = {
    FlowStatus.withName(preFlowStream.status) match {
      case FlowStatus.NEW | FlowStatus.STOPPED | FlowStatus.FAILED =>
        (true, s"it's available to drift, flow status will be ${preFlowStream.status} after drift.")
      case FlowStatus.STARTING | FlowStatus.UPDATING | FlowStatus.STOPPING =>
        (false, s"staring/updating/stopping status flow is not allowed to drift.")
      case FlowStatus.SUSPENDING =>
        (true, s"it's available to drift, flow status will be stopped after drift, you need start it manually.")
      case FlowStatus.RUNNING =>
        (true, getRunningFlowDriftTip(preFlowStream, streamId))
    }
  }

  def driftFlow(preFlowStream: FlowStream, driftFlowRequest: DriftFlowRequest, userId: Long): (Boolean, String) = {
    FlowStatus.withName(preFlowStream.status) match {
      case FlowStatus.NEW | FlowStatus.STOPPED =>
        Await.result(flowDal.updateStreamId(preFlowStream.id, driftFlowRequest.streamId), minTimeOut)
        (true, s"success")
      case FlowStatus.STARTING | FlowStatus.UPDATING | FlowStatus.STOPPING =>
        (false, s"staring/updating/stopping status flow is not allowed to drift. The final offset depends on the actual operation time!!!")
      case FlowStatus.SUSPENDING | FlowStatus.FAILED =>
        flowDal.genFlowStreamByAction(preFlowStream, Action.STOP.toString)
        Await.result(flowDal.updateStreamId(preFlowStream.id, driftFlowRequest.streamId), minTimeOut)
        (true, s"success, you need start it manually.")
      case FlowStatus.RUNNING =>
        (true, driftRunningFlow(preFlowStream, driftFlowRequest, userId))
    }
  }

  def getRunningFlowDriftTip(preFlowStream: FlowStream, streamId: Long): String = {
    val driftStream = streamDal.refreshStreamStatus(streamId).get
    StreamStatus.withName(driftStream.status) match {
      case StreamStatus.NEW | StreamStatus.STOPPING | StreamStatus.STOPPED | StreamStatus.FAILED =>
        s"it's available to drift, flow status will be stopped after drift, you need start it manually."
      case StreamStatus.STARTING | StreamStatus.WAITING | StreamStatus.RUNNING =>
        getDriftFlowOffset(preFlowStream, driftStream)._2
    }
  }

  def driftRunningFlow(preFlowStream: FlowStream, driftFlowRequest: DriftFlowRequest, userId: Long): String = {
    val driftStream = streamDal.refreshStreamStatus(driftFlowRequest.streamId).get
    val nsDetail = namespaceDal.getNsDetail(preFlowStream.sourceNs)
    StreamStatus.withName(driftStream.status) match {
      case StreamStatus.NEW | StreamStatus.STOPPING | StreamStatus.STOPPED | StreamStatus.FAILED =>
        flowDal.genFlowStreamByAction(preFlowStream, Action.STOP.toString)
        Await.result(flowDal.updateStreamId(preFlowStream.id, driftFlowRequest.streamId), minTimeOut)
        s"success, you need start it manually."
      case StreamStatus.STARTING | StreamStatus.WAITING | StreamStatus.RUNNING =>
        val offset = getDriftFlowOffset(preFlowStream, driftStream)._1
        val rate = Await.result(streamInTopicDal.findByFilter(rel => rel.streamId === preFlowStream.streamId && rel.nsDatabaseId === nsDetail._2.id), minTimeOut).head.rate
        flowDal.genFlowStreamByAction(preFlowStream, Action.STOP.toString)
        Await.result(flowDal.updateStreamId(preFlowStream.id, driftFlowRequest.streamId), minTimeOut)
        Await.result(flowDal.defaultGetAll(_.id === preFlowStream.id, Action.START.toString), minTimeOut)
        topicOffsetDrift(driftStream.id, nsDetail._2, offset, rate, userId)
        s"success, ${nsDetail._2.nsDatabase} topic offset adjust to $offset. The final offset depends on the actual operation time!!!"
    }
  }

  def getDriftFlowOffset(preFlowStream: FlowStream, driftStream: Stream): (String, String) = {
    val nsDetail = namespaceDal.getNsDetail(preFlowStream.sourceNs)
    val db = nsDetail._2
    if (StreamUtils.containsTopic(driftStream.id, db.id)) {
      val preStreamOffset = getConsumedOffset(preFlowStream.streamId, db.id, db.nsDatabase)
      val driftStreamOffset = getConsumedOffset(driftStream.id, db.id, db.nsDatabase)
      val offset = if (preStreamOffset < driftStreamOffset) preStreamOffset
      else driftStreamOffset
      (offset,
        s"it's available to drift, ${preFlowStream.streamName} stream consumed topic ${db.nsDatabase} offset is $preStreamOffset, ${driftStream.name} stream consumed offset is $driftStreamOffset, ${driftStream.name} stream ${db.nsDatabase} offset will be update to $offset. The final offset depends on the actual operation time!!!")
    } else {
      val offset = StreamUtils.getConsumedOffset(preFlowStream.streamId, db.id, db.nsDatabase)
      (offset, s"it's available to drift, ${driftStream.name} stream will add new topic ${db.nsDatabase} with $offset offset. The final offset depends on the actual operation time!!!")
    }
  }

  def topicOffsetDrift(streamId: Long, db: NsDatabase, offset: String, rate: Int, userId: Long): Unit = {
    Await.result(streamInTopicDal.updateOffsetAndRate(streamId, db.id, offset, rate, userId), minTimeOut)
    sendTopicDirective(streamId, Seq(PutTopicDirective(db.nsDatabase, offset, rate, Option(1))), userId, false)
  }

}
