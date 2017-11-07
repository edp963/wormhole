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
import edp.rider.common.Action._
import edp.rider.common.StreamStatus._
import edp.rider.common._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.spark.SparkJobClientLog
import edp.rider.spark.SparkStatusQuery.{getAllYarnAppStatus, getAppStatusByRest}
import edp.rider.spark.SubmitSparkJob.{generateStreamStartSh, runShellCommand}
import edp.rider.wormhole.{BatchFlowConfig, KafkaInputBaseConfig, KafkaOutputConfig, SparkConfig}
import edp.rider.zookeeper.PushDirective
import edp.rider.zookeeper.PushDirective._
import edp.wormhole.common.util.JsonUtils.{caseClass2json, _}
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.UmsSchemaUtils.toUms

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.Await

object StreamUtils extends RiderLogger {

  def getDisableActions(status: String): String = {
    streamStatus(status) match {
      case NEW => s"$STOP, $RENEW"
      case STARTING => s"$START, $STOP, $DELETE"
      case WAITING => s"$START"
      case RUNNING => s"$START"
      case STOPPING => s"$START, $RENEW"
      case STOPPED => s"$STOP, $RENEW"
      case FAILED => s"$RENEW"
    }
  }

  def getStatus(action: String, streams: Seq[Stream]): Seq[Stream] = {
    val fromTime =
      if (streams.nonEmpty && streams.exists(_.startedTime.getOrElse("") != ""))
        streams.filter(_.startedTime.getOrElse("") != "").map(_.startedTime).min.getOrElse("")
      else ""
    //    riderLogger.info(s"fromTime: $fromTime")
    val appInfoList: List[AppResult] =
      if (fromTime == "") List() else getAllYarnAppStatus(fromTime).sortWith(_.appId < _.appId)
    //    riderLogger.info(s"app info size: ${appInfoList.size}")
    streams.map(
      stream => {
        val dbStatus = stream.status
        val dbUpdateTime = stream.updateTime
        val startedTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime.get
        val stoppedTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime.get
        val appInfo = {
          if (action == "start") AppInfo("", "starting", currentSec, null)
          else if (action == "stop") AppInfo("", "stopping", startedTime, stoppedTime)
          else {
            val endAction =
              if (dbStatus == STARTING.toString) "refresh_log"
              else "refresh_spark"
//            val endAction = "refresh_spark"
            val sparkStatus: AppInfo = endAction match {
              case "refresh_spark" =>
                getAppStatusByRest(appInfoList, stream.name, stream.status, startedTime, stoppedTime)
              case "refresh_log" =>
                val logInfo = SparkJobClientLog.getAppStatusByLog(stream.name, dbStatus)
                logInfo._2 match {
                  case "running" =>
                    getAppStatusByRest(appInfoList, stream.name, logInfo._2, startedTime, stoppedTime)
                  case "waiting" =>
                    val curInfo = getAppStatusByRest(appInfoList, stream.name, logInfo._2, startedTime, stoppedTime)
                    AppInfo(curInfo.appId, curInfo.appState, startedTime, curInfo.finishedTime)
                  case "starting" => getAppStatusByRest(appInfoList, stream.name, logInfo._2, startedTime, stoppedTime)
                  case "failed" => AppInfo(logInfo._1, "failed", startedTime, currentSec)
                }
              case _ => AppInfo("", stream.status, startedTime, null)
            }
            if (sparkStatus == null) AppInfo(stream.sparkAppid.getOrElse(""), "failed", startedTime, stoppedTime)
            else {
              val resStatus = dbStatus match {
                case "starting" =>
                  sparkStatus.appState.toUpperCase match {
                    case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "KILLED" | "FINISHED" | "FAILED" => AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case _ => AppInfo("", "starting", startedTime, stoppedTime)
                  }
                case "waiting" => sparkStatus.appState.toUpperCase match {
                  case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case "KILLED" | "FINISHED" | "FAILED" => AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case _ => AppInfo(sparkStatus.appId, "waiting", startedTime, stoppedTime)
                }
                case "running" =>
                  if (List("FAILED", "KILLED", "FINISHED").contains(sparkStatus.appState.toUpperCase)) {
                    AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                  }
                  else {
                    AppInfo(sparkStatus.appId, "running", startedTime, stoppedTime)
                  }
                case "stopping" =>
                  if (sparkStatus.appState == "KILLED") {
                    AppInfo(sparkStatus.appId, "stopped", sparkStatus.startedTime, sparkStatus.finishedTime)
                  }
                  else {
                    AppInfo(sparkStatus.appId, "stopping", startedTime, stoppedTime)
                  }
                case "new" =>
                  AppInfo("", "new", startedTime, stoppedTime)
                case "stopped" =>
                  AppInfo(sparkStatus.appId, "stopped", startedTime, stoppedTime)
                case "failed" =>
                  sparkStatus.appState.toUpperCase match {
                    case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "KILLED" | "FINISHED" | "FAILED" | _ => AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)

                  }
                case _ => AppInfo(sparkStatus.appId, dbStatus, startedTime, stoppedTime)
              }
              resStatus
            }
          }
        }
        stream.updateFromSpark(appInfo)
      })
  }

  def getProjectNameByStreamName(streamName: String) = {
    streamName.split("_")(1)
  }

  def genStreamNameByProjectName(projectName: String, name: String): String = s"wormhole_${projectName}_$name"

  def getBatchFlowConfig(streamDetail: StreamDetail) = {
    val launchConfig = json2caseClass[LaunchConfig](streamDetail.stream.launchConfig)
    val config = BatchFlowConfig(KafkaInputBaseConfig(streamDetail.stream.name, launchConfig.durations.toInt, streamDetail.kafkaInfo.connUrl, launchConfig.maxRecords.toInt),
      KafkaOutputConfig(RiderConfig.consumer.topic, RiderConfig.consumer.brokers),
      SparkConfig(streamDetail.stream.id, streamDetail.stream.name, "yarn-cluster", launchConfig.partitions.toInt),
      launchConfig.partitions.toInt, RiderConfig.zk, false, Some(RiderConfig.spark.hdfs_root))
    caseClass2json[BatchFlowConfig](config)
  }

  def startStream(streamDetail: StreamDetail) = {
    val args = getBatchFlowConfig(streamDetail)
    val startConfig = json2caseClass[StartConfig](streamDetail.stream.startConfig)
    val commandSh = generateStreamStartSh(s"'''$args'''", streamDetail.stream.name, startConfig, streamDetail.stream.sparkConfig.get, streamDetail.stream.streamType)
    riderLogger.info(s"start stream command: $commandSh")
    runShellCommand(commandSh)
  }

  def sendTopicDirective(streamId: Long, topicSeq: Seq[StreamTopicTemp], userId: Long) = {
    try {
      val directiveSeq = new ArrayBuffer[Directive]
      val zkConURL: String = RiderConfig.zk
      topicSeq.foreach({
        topic =>
          val tuple = Seq(streamId, currentMicroSec, topic.name, topic.rate, topic.partitionOffsets).mkString("#")
          directiveSeq += Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, tuple, zkConURL, currentSec, userId)
      })
      val directives: Seq[Directive] =
        if (directiveSeq.isEmpty) directiveSeq
        else {
          Await.result(modules.directiveDal.insert(directiveSeq), minTimeOut)
        }
      val blankTopic = Directive(0, null, streamId, 0, Seq(streamId, currentMicroSec, RiderConfig.spark.wormholeHeartBeatTopic, RiderConfig.spark.topicDefaultRate, "0:0").mkString("#"), zkConURL, currentSec, userId)
      val directiveNew = directives.to[mutable.ArrayBuffer] += blankTopic
      val topicUms = directiveNew.map({
        directive =>
          val topicInfo = directive.directive.split("#")
          s"""
             |{
             |"protocol": {
             |"type": "${DIRECTIVE_TOPIC_SUBSCRIBE.toString}"
             |},
             |"schema": {
             |"namespace": "",
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
             |"name": "topic_name",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "topic_rate",
             |"type": "int",
             |"nullable": false
             |},
             |{
             |"name": "partitions_offset",
             |"type": "string",
             |"nullable": false
             |}
             |]
             |},
             |"payload": [
             |{
             |"tuple": [${directive.id}, ${topicInfo(0)}, "${topicInfo(1)}", "${topicInfo(2)}", ${topicInfo(3)}, "${topicInfo(4)}"]
             |}
             |]
             |}
          """.stripMargin.replaceAll("[\\n\\t\\r]+", "")
      }).mkString("\n")
      PushDirective.sendTopicDirective(streamId, topicUms, directiveNew.head.zkPath)
      riderLogger.info(s"user $userId send ${DIRECTIVE_TOPIC_SUBSCRIBE.toString} directives success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def sendUnsubscribeTopicDirective(streamId: Long, topicName: String, userId: Long) = {
    try {
      val zkConURL: String = RiderConfig.zk
      val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, topicName, zkConURL, currentSec, userId)
      ), minTimeOut)
      val topicUms =
        s"""
            {
           |  "protocol": {
           |    "type": "${DIRECTIVE_TOPIC_UNSUBSCRIBE.toString}"
           |  },
           |  "schema": {
           |    "namespace": "",
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
           |        "name": "topic_name",
           |        "type": "string",
           |        "nullable": false
           |      }
           |    ]
           |  },
           |  "payload": [
           |    {
           |      "tuple": [${directive.id}, $streamId, "$currentSec", "$topicName"]
           |    }
           |  ]
           |}
          """.stripMargin.replaceAll("[\\n\\t\\r]+", "")
      PushDirective.sendTopicDirective(streamId, topicUms)
      riderLogger.info(s"user $userId send ${DIRECTIVE_TOPIC_SUBSCRIBE.toString} directives success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def removeAndSendDirective(streamId: Long, topicSeq: Seq[StreamTopicTemp], userId: Long) = {
    try {
      if (topicSeq.nonEmpty) {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      } else {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      }
      sendTopicDirective(streamId, topicSeq, userId)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"remove and send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def removeStreamDirective(streamId: Long, topicSeq: Seq[StreamTopic], userId: Long) = {
    try {
      if (topicSeq.nonEmpty) {
        PushDirective.removeTopicDirective(streamId, RiderConfig.zk)
        riderLogger.info(s"user $userId remove topic directive success.")
      } else {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"remove and send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def getDuration(launchConfig: String): Int = {
    if (launchConfig != null && launchConfig != "") {
      if (JSON.parseObject(launchConfig).containsKey("durations"))
        JSON.parseObject(launchConfig).getIntValue("durations")
      else 10
    } else 10
  }

  def checkConfigFormat(startConfig: String, launchConfig: String, sparkConfig: String) = {
    (isJson(startConfig), isJson(launchConfig), isStreamSparkConfig(sparkConfig)) match {
      case (true, true, true) => (true, "success")
      case (true, true, false) => (false, s"sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (true, false, true) => (false, s"launchConfig $launchConfig is not json type")
      case (true, false, false) => (false, s"launchConfig $launchConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (false, true, true) => (false, s"startConfig $startConfig is not json type")
      case (false, true, false) => (false, s"startConfig $startConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (false, false, true) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type")
      case (false, false, false) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
    }
  }

  def getZkStreamUdf(streamIds: Seq[Long]): Seq[StreamZkUdfTemp] = {
    val seq = new ListBuffer[StreamZkUdfTemp]
    try {
      streamIds.foreach(id => {
        seq ++= zkUdf2StreamUdf(getUdfDirective(id))
      })
      seq
    } catch {
      case _: GetZookeeperDataException =>
        riderLogger.info(s"streams ${
          streamIds.mkString(",")
        } zk udf didn't exist")
        seq
      case ex: Exception =>
        riderLogger.error(s"get stream ${
          streamIds.mkString(",")
        } zk udf failed", ex)
        seq
    }
  }

  def zkUdf2StreamUdf(udfSeq: Seq[String]): Seq[StreamZkUdfTemp] = {
    val seq = new ListBuffer[StreamZkUdfTemp]
    udfSeq.foreach(
      udf => {
        if (udf != "" && udf != null) {
          if (isJson(udf)) {
            toUms(udf).payload match {
              case Some(payloadSeq) =>
                seq ++= payloadSeq.map(
                  payload => {
                    StreamZkUdfTemp(payload.tuple(1).toLong, payload.tuple(3), payload.tuple(4), payload.tuple(5).split("/").last)
                  })
              case None =>
            }

          }
        }
      }
    )
    seq
  }

  def stopStream(sparkAppid: Option[String], status: String): String = {
    if (status == RUNNING.toString || status == WAITING.toString) {
      if (sparkAppid.getOrElse("") != "") {
        val cmdStr = "yarn application -kill " + sparkAppid.get
        riderLogger.info(s"stop stream command: $cmdStr")
        runShellCommand(cmdStr)
        STOPPING.toString
      } else STOPPED.toString
    } else STOPPED.toString
  }
}
