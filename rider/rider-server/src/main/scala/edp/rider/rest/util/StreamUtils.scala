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
import edp.rider.RiderStarter.modules._
import edp.rider.common.Action._
import edp.rider.common.StreamStatus._
import edp.rider.common.StreamType._
import edp.rider.common.{StreamType, _}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.UdfUtils.sendUdfDirective
import edp.rider.wormhole.{BatchFlowConfig, KafkaInputBaseConfig, KafkaOutputConfig, SparkConfig}
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.yarn.YarnStatusQuery.{getActiveResourceManager, getAllYarnAppStatus, getAppStatusByRest}
import edp.rider.yarn.{ShellUtils, SubmitYarnJob, YarnClientLog}
import edp.rider.zookeeper.PushDirective
import edp.rider.zookeeper.PushDirective._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.UmsSchemaUtils.toUms
import edp.wormhole.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import edp.rider.kafka.WormholeGetOffsetUtils._
import edp.wormhole.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.Await

object StreamUtils extends RiderLogger {

  def getDisableActions(streamType: String, status: String): String = {
    StreamType.withName(streamType) match {
      case SPARK =>
        StreamStatus.withName(status) match {
          case NEW => s"$STOP, $RENEW"
          case STARTING => s"$START, $STOP, $DELETE"
          case WAITING => s"$START"
          case RUNNING => s"$START"
          case STOPPING => s"$START, $RENEW, $DELETE"
          case STOPPED => s"$STOP, $RENEW"
          case FAILED => s"$RENEW"
        }
      case FLINK =>
        StreamStatus.withName(status) match {
          case NEW => s"$STOP, $RENEW"
          case STARTING => s"$START, $STOP, $DELETE，$RENEW"
          case WAITING => s"$START，$RENEW"
          case RUNNING => s"$START，$RENEW"
          case STOPPING => s"$START, $RENEW, $DELETE"
          case STOPPED => s"$STOP, $RENEW"
          case FAILED => s"$RENEW"
        }
    }
  }

  def getHideActions(streamType: String): String = {
    StreamType.withName(streamType) match {
      case FLINK => s"$RENEW"
      case _ => ""
    }
  }

  def getAppInfo(fromTime: String, streamName: String): Option[AppResult] = {
    val appInfoMap: Map[String, AppResult] = if (fromTime == "") Map.empty[String, AppResult] else getAllYarnAppStatus(fromTime, Seq(streamName))
    if (appInfoMap.contains(streamName)) Some(appInfoMap(streamName))
    else None
  }

  def getYarnUri(status: String, appId: String): String = {
    val rmUrl = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    if (status.toLowerCase == "running")
      s"http://${rmUrl.stripPrefix("http://").stripSuffix("/")}/proxy/$appId/"
    else
      s"http://${rmUrl.stripPrefix("http://").stripSuffix("/")}/cluster/app/$appId/"
  }

  def getStreamYarnAppStatus(streams: Seq[Stream], appInfoMap: Map[String, AppResult], userId: Long) = {
    streams.map(
      stream => {
        val dbStatus = stream.status
        val startedTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime.get
        val stoppedTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime.get
        val appInfo = {
          val endAction =
            if (dbStatus == STARTING.toString) "refresh_log"
            else "refresh_spark"
          val sparkStatus: AppInfo = endAction match {
            case "refresh_spark" =>
              getAppStatusByRest(appInfoMap, stream.sparkAppid.getOrElse(""), stream.name, stream.status, startedTime, stoppedTime)
            case "refresh_log" =>
              val logInfo = StreamType.withName(stream.streamType) match {
                case StreamType.SPARK => YarnClientLog.getAppStatusByLog(stream.name, dbStatus, stream.logPath.getOrElse(""), stream.sparkAppid.getOrElse(""))
                case _ => YarnClientLog.getFlinkAppStatusByLog(stream.name, dbStatus, stream.logPath.getOrElse(""), stream.sparkAppid.getOrElse(""))
              }
              logInfo._2 match {
                case "running" =>
                  getAppStatusByRest(appInfoMap, logInfo._1, stream.name, logInfo._2, startedTime, stoppedTime)
                case "waiting" =>
                  val curInfo = getAppStatusByRest(appInfoMap, logInfo._1, stream.name, logInfo._2, startedTime, stoppedTime)
                  AppInfo(curInfo.appId, curInfo.appState, startedTime, curInfo.finishedTime)
                case "starting" => getAppStatusByRest(appInfoMap, logInfo._1, stream.name, logInfo._2, startedTime, stoppedTime)
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
                  case _ => AppInfo(stream.sparkAppid.getOrElse(""), "starting", startedTime, stoppedTime)
                }
              case "waiting" => sparkStatus.appState.toUpperCase match {
                case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                case "KILLED" | "FINISHED" | "FAILED" => AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                case _ => AppInfo(sparkStatus.appId, "waiting", startedTime, stoppedTime)
              }
              case "running" =>
                if (List("FAILED", "KILLED", "FINISHED").contains(sparkStatus.appState.toUpperCase)) {
                  FlowUtils.updateStatusByStreamStop(stream.id, stream.streamType, "failed", userId)
                  AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                } else {
                  AppInfo(sparkStatus.appId, "running", startedTime, stoppedTime)
                }
              case "stopping" =>
                if (sparkStatus.appState == "KILLED" || sparkStatus.appState == "FAILED" || sparkStatus.appState == "FINISHED") {
                  FlowUtils.updateStatusByStreamStop(stream.id, stream.streamType, "stopped", userId)
                  AppInfo(sparkStatus.appId, "stopped", sparkStatus.startedTime, sparkStatus.finishedTime)
                }
                else {
                  AppInfo(sparkStatus.appId, "stopping", startedTime, stoppedTime)
                }
              case "new" =>
                AppInfo("", "new", startedTime, stoppedTime)
              case "stopped" =>
                sparkStatus.appState.toUpperCase match {
                  case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case "KILLED" | "FINISHED" | "FAILED" => AppInfo(sparkStatus.appId, "stopped", sparkStatus.startedTime, sparkStatus.finishedTime)
                  case _ => AppInfo(sparkStatus.appId, "stopped", startedTime, stoppedTime)
                }
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
        stream.updateFromSpark(appInfo)
      })
  }


  def getStatus(action: String, streams: Seq[Stream]): Seq[Stream] = {
    streams.map(
      stream => {
        val startedTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime.get
        val stoppedTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime.get
        val appInfo = {
          if (action == "start") AppInfo("", "starting", currentSec, null)
          else if (action == "stop") AppInfo("", "stopping", startedTime, stoppedTime)
          else AppInfo(stream.sparkAppid.getOrElse(""), stream.status, startedTime, stoppedTime)
        }
        stream.updateFromSpark(appInfo)
      })
  }


  def genStreamNameByProjectName(projectName: String, name: String): String =
    if (RiderConfig.riderServer.clusterId != "") s"wormhole_${RiderConfig.riderServer.clusterId}_${projectName}_$name"
    else s"wormhole_${projectName}_$name"

  def getStreamConfig(stream: Stream) = {
    val startConfig = json2caseClass[StartConfig](stream.startConfig)
    val launchConfig = json2caseClass[LaunchConfig](stream.launchConfig)
    val inputKafkaInstance = getKafkaDetailByStreamId(stream.id)
    val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(inputKafkaInstance._2.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)

    val config =
      RiderConfig.spark.remoteHdfsRoot match {
        case Some(_) =>
          BatchFlowConfig(KafkaInputBaseConfig(stream.name, launchConfig.durations.toInt, inputKafkaInstance._1, inputKafkaKerberos, launchConfig.maxRecords.toInt * 1024 * 1024, RiderConfig.spark.kafkaSessionTimeOut, RiderConfig.spark.kafkaGroupMaxSessionTimeOut),
            KafkaOutputConfig(RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.brokers, RiderConfig.kerberos.kafkaEnabled),
            SparkConfig(stream.id, stream.name, "yarn", startConfig.executorNums * startConfig.perExecutorCores),
            launchConfig.partitions.toInt, RiderConfig.zk.address, RiderConfig.zk.path, false, getStreamSpecialConfig(stream.specialConfig),
            RiderConfig.spark.remoteHdfsRoot, RiderConfig.kerberos.kafkaEnabled, RiderConfig.spark.remoteHdfsNamenodeHosts,
            RiderConfig.spark.remoteHdfsNamenodeIds, Option(false))
        case None =>
         // val (hdfsNameNodeIds,hdfsNameNodeHosts)=getNameNodeInfoFromLocalHadoop()
          BatchFlowConfig(KafkaInputBaseConfig(stream.name, launchConfig.durations.toInt, inputKafkaInstance._1, inputKafkaKerberos, launchConfig.maxRecords.toInt * 1024 * 1024, RiderConfig.spark.kafkaSessionTimeOut, RiderConfig.spark.kafkaGroupMaxSessionTimeOut),
            KafkaOutputConfig(RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.brokers, RiderConfig.kerberos.kafkaEnabled),
            SparkConfig(stream.id, stream.name, "yarn", launchConfig.partitions.toInt),
            launchConfig.partitions.toInt, RiderConfig.zk.address, RiderConfig.zk.path, false, getStreamSpecialConfig(stream.specialConfig),
            Some(RiderConfig.spark.hdfsRoot), RiderConfig.kerberos.kafkaEnabled, None, None, None)
      }
    caseClass2json[BatchFlowConfig](config)
  }

  def getStreamSpecialConfig(config: Option[String]): Option[StreamSpecialConfig] = {
    config match {
      case Some(c) =>
        if(c.trim.isEmpty) None
        else Option(JsonUtils.json2caseClass[StreamSpecialConfig](c))
      case None =>
        None
    }
  }

/*
  def getNameNodeInfoFromLocalHadoop()={
    val hadoopHome = System.getenv("HADOOP_HOME")
    val configuration = new Configuration(false)
    configuration.addResource(new Path(s"$hadoopHome/conf/hdfs-site.xml"))
    val nameServiceNameInternal=configuration.get("dfs.internal.nameservices")
    //val nameServiceNameNoInternal=configuration.get("dfs.nameservices")
    val nameServiceName = if(null != nameServiceNameInternal && nameServiceNameInternal != "") nameServiceNameInternal else configuration.get("dfs.nameservices")

    val nameNodeIds = configuration.get(s"dfs.ha.namenodes.$nameServiceName")
    val nameNodeHosts = nameNodeIds.split(",").map(nodeId => configuration.get(s"dfs.namenode.rpc-address.$nameServiceName.$nodeId")).mkString(",")
    riderLogger.info(s"serviceName:$nameServiceName,nodeIds:$nameNodeIds,nodeHosts:$nameNodeHosts")
    (nameNodeIds , nameNodeHosts)
  }
*/

  def startStream(stream: Stream, logPath: String): (Boolean, Option[String]) = {
    StreamType.withName(stream.streamType) match {
      case StreamType.SPARK =>
        val args = getStreamConfig(stream)
        val startConfig = json2caseClass[StartConfig](stream.startConfig)
        val commandSh = generateSparkStreamStartSh(s"'''$args'''", stream.name, logPath, startConfig, stream.JVMDriverConfig.getOrElse(""), stream.JVMExecutorConfig.getOrElse(""), stream.othersConfig.getOrElse(""), stream.functionType)
        riderLogger.info(s"start stream ${stream.id} command: $commandSh")
        ShellUtils.runShellCommand(commandSh, logPath)
      case StreamType.FLINK =>
        val commandSh = SubmitYarnJob.generateFlinkStreamStartSh(stream)
        riderLogger.info(s"start stream ${stream.id} command: $commandSh")
        ShellUtils.runShellCommand(commandSh, logPath)
    }
  }

  def genUdfsStartDirective(streamId: Long, udfIds: Seq[Long], userId: Long): Unit = {
    if (udfIds.nonEmpty) {
      val deleteUdfIds = relStreamUdfDal.getDeleteUdfIds(streamId, udfIds)
      Await.result(relStreamUdfDal.deleteByFilter(udf => udf.streamId === streamId && udf.udfId.inSet(deleteUdfIds)), minTimeOut)
      val insertUdfs = udfIds.map(
        id => RelStreamUdf(0, streamId, id, currentSec, userId, currentSec, userId)
      )
      Await.result(relStreamUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
      sendUdfDirective(streamId, relStreamUdfDal.getStreamUdf(Seq(streamId)), userId)
    } else {
      Await.result(relStreamUdfDal.deleteByFilter(_.streamId === streamId), minTimeOut)
    }
  }

  def genUdfsRenewDirective(streamId: Long, udfIds: Seq[Long], userId: Long): Unit = {
    if (udfIds.nonEmpty) {
      val insertUdfs = udfIds.map(
        id => RelStreamUdf(0, streamId, id, currentSec, userId, currentSec, userId)
      )
      Await.result(relStreamUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
      sendUdfDirective(streamId,
        relStreamUdfDal.getStreamUdf(Seq(streamId)).filter(udf => udfIds.contains(udf.id)),
        userId)
    }
  }

  def genTopicsStartDirective(streamId: Long, putTopicOpt: Option[PutStreamTopic], userId: Long): Unit = {
    putTopicOpt match {
      case Some(putTopic) =>
        val autoRegisteredTopics = putTopic.autoRegisteredTopics
        val userdefinedTopics = putTopic.userDefinedTopics
        // update auto registered topics
        streamInTopicDal.updateByStartOrRenew(streamId, autoRegisteredTopics, userId)
        // delete user defined topics by start
        streamUdfTopicDal.deleteByStartOrRenew(streamId, userdefinedTopics)
        // insert or update user defined topics by start
        streamUdfTopicDal.insertUpdateByStartOrRenew(streamId, userdefinedTopics, userId)
        // send topics start directive
        //val topics = autoRegisteredTopics ++: userdefinedTopics
        //val addHeartbeatTopic = if (topics.isEmpty) true else false
        val addDefaultTopicFlag =
        if (autoRegisteredTopics.nonEmpty || userdefinedTopics.nonEmpty)
          false
        else
          true
        sendTopicDirective(streamId, autoRegisteredTopics, Some(userdefinedTopics), userId, addDefaultTopicFlag)
      case None =>
        // delete all user defined topics by stream id
        Await.result(streamUdfTopicDal.deleteByFilter(_.streamId === streamId), minTimeOut)
    }
  }

  def genTopicsRenewDirective(streamId: Long, putTopicOpt: Option[PutStreamTopic], userId: Long): Unit = {
    putTopicOpt match {
      case Some(putTopic) =>
        val autoRegisteredTopics = putTopic.autoRegisteredTopics
        val userdefinedTopics = putTopic.userDefinedTopics
        // update auto registered topics
        streamInTopicDal.updateByStartOrRenew(streamId, autoRegisteredTopics, userId)
        // delete user defined topics by start
        val deleteTopics = streamUdfTopicDal.deleteByStartOrRenew(streamId, userdefinedTopics)
        // delete topics directive in zookeeper
        sendUnsubscribeTopicDirective(streamId, deleteTopics, userId)
        // insert or update user defined topics by start
        streamUdfTopicDal.insertUpdateByStartOrRenew(streamId, userdefinedTopics, userId)
        // send topics renew directive which action is 1
        sendTopicDirective(streamId, autoRegisteredTopics.filter(_.action.getOrElse(0) == 1), Some(userdefinedTopics.filter(_.action.getOrElse(0) == 1)), userId, false)
      case None =>
        val deleteTopics = streamUdfTopicDal.deleteByStartOrRenew(streamId, Seq())
        // delete topics directive in zookeeper
        sendUnsubscribeTopicDirective(streamId, deleteTopics, userId)
    }
  }

  def sendTopicDirective(streamId: Long, incrementTopicSeq: Seq[PutTopicDirective], initialTopicSeq: Option[Seq[PutTopicDirective]], userId: Long, addDefaultTopic: Boolean = true) = {
    try {
      val directiveSeq = new ArrayBuffer[Directive]
      val zkConURL: String = RiderConfig.zk.address
      (incrementTopicSeq ++: initialTopicSeq.getOrElse(mutable.ArraySeq.empty[PutTopicDirective])).filter(_.rate == 0).map(
        topic => sendUnsubscribeTopicDirective(streamId, topic.name, userId)
      )
      if (initialTopicSeq.nonEmpty) initialTopicSeq.get.filter(_.rate != 0).foreach({
        topic =>
          val tuple = Seq(streamId, currentMicroSec, topic.name, topic.rate, topic.partitionOffsets, "initial").mkString("#")
          directiveSeq += Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, tuple, zkConURL, currentSec, userId)
      })
      incrementTopicSeq.filter(_.rate != 0).foreach({
        topic =>
          val tuple = Seq(streamId, currentMicroSec, topic.name, topic.rate, topic.partitionOffsets, "increment").mkString("#")
          directiveSeq += Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, tuple, zkConURL, currentSec, userId)
      })
      if (addDefaultTopic) {
        //val broker = getKafkaByStreamId(streamId)
        val inputKafkaInstance = getKafkaDetailByStreamId(streamId)
        val broker = inputKafkaInstance._1
        val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(inputKafkaInstance._2.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
        val blankTopicOffset = getLatestOffset(broker, RiderConfig.spark.wormholeHeartBeatTopic, inputKafkaKerberos)
        val blankTopic = Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, Seq(streamId, currentMicroSec, RiderConfig.spark.wormholeHeartBeatTopic, RiderConfig.spark.topicDefaultRate, blankTopicOffset, "initial").mkString("#"), zkConURL, currentSec, userId)
        directiveSeq += blankTopic
      }

      val directives = Await.result(directiveDal.insert(directiveSeq), minTimeOut)

      val topicUms = directives.map({
        directive =>
          val topicInfo = directive.directive.split("#")
          val ums =
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
               |"name": "topic_type",
               |"type": "string",
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
               |"tuple": [${directive.id}, ${topicInfo(0)}, "${topicInfo(1)}", "${topicInfo(2)}", ${topicInfo(3)}, "${topicInfo(5)}","${topicInfo(4)}"]
               |}
               |]
               |}
          """.stripMargin.replaceAll("[\\n\\t\\r]+", "")
          jsonCompact(ums)
      }).mkString("\n")
      if (topicUms.nonEmpty) {
        PushDirective.sendTopicDirective(streamId, topicUms)
        riderLogger.info(s"user $userId send topic directive $topicUms success.")
      } else {
        riderLogger.info(s"topic offsets don't need update.")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def sendUnsubscribeTopicDirective(streamId: Long, topicsName: Seq[String], userId: Long): Unit = {
    topicsName.foreach(topic => sendUnsubscribeTopicDirective(streamId, topic, userId))
  }

  def sendUnsubscribeTopicDirective(streamId: Long, topicName: String, userId: Long): Unit = {
    try {
      val zkConURL: String = RiderConfig.zk.address
      val directive = Await.result(directiveDal.insert(Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, "", zkConURL, currentSec, userId)
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
      PushDirective.sendTopicDirective(streamId, jsonCompact(topicUms))
      riderLogger.info(s"user $userId send topic directive $topicUms success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  //  def removeAndSendTopicDirective(streamId: Long, topicSeq: Seq[PutTopicDirective], userId: Long) = {
  //    try {
  //      if (topicSeq.nonEmpty) {
  //        PushDirective.removeTopicDirective(streamId)
  //        riderLogger.info(s"user $userId remove topic directive success.")
  //      } else {
  //        PushDirective.removeTopicDirective(streamId)
  //        riderLogger.info(s"user $userId remove topic directive success.")
  //      }
  //      sendTopicDirective(streamId, topicSeq, userId, true)
  //    } catch {
  //      case ex: Exception =>
  //        riderLogger.error(s"remove and send stream $streamId topic directive failed", ex)
  //        throw ex
  //    }
  //  }

  def removeStreamDirective(streamId: Long, userId: Long) = {
    try {
      PushDirective.removeStreamDirective(streamId)
      riderLogger.info(s"user $userId remove stream $streamId directive success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"remove and send stream $streamId directive failed", ex)
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

  def checkConfigFormat(startConfig: String, launchConfig: String, JVMDriverConfig: String, JVMExecutorConfig: String, othersConfig: String) = {
    val jvmConfig = JVMDriverConfig + JVMExecutorConfig
    (isJson(startConfig), isJson(launchConfig), isStreamConfig(jvmConfig), isStreamConfig(othersConfig)) match {
      case (true, true, true, true) => (true, "success")
      case (true, true, true, false) => (false, s"othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (true, true, false, true) => (false, s"jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format")
      case (true, true, false, false) => (false, s"jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (true, false, true, true) => (false, s"launchConfig $launchConfig is not json type")
      case (true, false, true, false) => (false, s"launchConfig $launchConfig is not json type, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (true, false, false, true) => (false, s"launchConfig $launchConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format")
      case (true, false, false, false) => (false, s"launchConfig $launchConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (false, true, true, true) => (false, s"startConfig $startConfig is not json type")
      case (false, true, true, false) => (false, s"startConfig $startConfig is not json type, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (false, true, false, true) => (false, s"startConfig $startConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format")
      case (false, true, false, false) => (false, s"startConfig $startConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (false, false, true, true) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type")
      case (false, false, true, false) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")
      case (false, false, false, true) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format")
      case (false, false, false, false) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type, jvmConfig $jvmConfig doesn't meet key=value,key1=value1 format, othersConfig $othersConfig doesn't meet key=value,key1=value1 format")

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

  def stopStream(streamId: Long, streamType: String, sparkAppid: Option[String], status: String, userId: Long): (String, Boolean) = {
    if (status == RUNNING.toString || status == WAITING.toString || status == STOPPING.toString) {
      if (sparkAppid.getOrElse("") != "") {
        val cmdStr = "yarn application -kill " + sparkAppid.get
        riderLogger.info(s"stop stream command: $cmdStr")
        val stopSuccess = runYarnKillCommand(cmdStr)
        if (stopSuccess) {
          FlowUtils.updateStatusByStreamStop(streamId, streamType, STOPPING.toString, userId)
          (STOPPING.toString, true)
        } else {
          FlowUtils.updateStatusByStreamStop(streamId, streamType, status, userId)
          (status, false)
        }
      } else {
        FlowUtils.updateStatusByStreamStop(streamId, streamType, STOPPED.toString, userId)
        (STOPPED.toString, true)
      }
    } else {
      FlowUtils.updateStatusByStreamStop(streamId, streamType, STOPPED.toString, userId)
      (STOPPED.toString, true)
    }
  }

  def checkAdminRemoveUdfs(projectId: Long, ids: Seq[Long]): (mutable.HashMap[Long, Seq[String]], ListBuffer[Long]) = {
    val deleteUdfMap = Await.result(udfDal.findByFilter(_.id inSet ids).mapTo[Seq[Udf]], minTimeOut)
      .map(udf => (udf.id, udf.functionName)).toMap[Long, String]
    val notDeleteMap = new mutable.HashMap[Long, Seq[String]]
    val deleteUdfSeq = deleteUdfMap.keySet
    val notDeleteUdfIds = new ListBuffer[Long]
    val streamIds = Await.result(streamDal.findByFilter(stream => stream.projectId === projectId && stream.status =!= "new" && stream.status =!= "stopped" && stream.status =!= "failed"), minTimeOut).map(_.id)
    val streamUdfs = Await.result(relStreamUdfDal.findByFilter(_.streamId inSet streamIds), minTimeOut)
    streamUdfs.foreach(stream => {
      val notDeleteUdfSeq = new ListBuffer[String]
      if (deleteUdfSeq.contains(stream.udfId)) {
        notDeleteUdfIds += stream.udfId
        notDeleteUdfSeq += deleteUdfMap(stream.udfId)
      }
      if (notDeleteUdfSeq.nonEmpty)
        notDeleteMap(stream.streamId) = notDeleteUdfSeq.distinct
    })
    (notDeleteMap, notDeleteUdfIds)
  }

  def getProjectIdsByUdf(udf: Long): Seq[Long] = {
    val streamIds = Await.result(relStreamUdfDal.findByFilter(_.udfId === udf), minTimeOut).map(_.streamId).distinct
    Await.result(streamDal.findByFilter(_.id inSet (streamIds)), minTimeOut).map(_.projectId).distinct
  }

  def getKafkaByStreamId(id: Long): String = {
    val kakfaId = Await.result(streamDal.findById(id), minTimeOut).get.instanceId
    Await.result(instanceDal.findById(kakfaId), minTimeOut).get.connUrl
  }

  def getKafkaDetailByStreamId(id: Long): (String, Option[String]) = {
    val kakfaId = Await.result(streamDal.findById(id), minTimeOut).get.instanceId
    val instance = Await.result(instanceDal.findById(kakfaId), minTimeOut)
    (instance.get.connUrl, instance.get.connConfig)
  }

  def getLogPath(appName: String) = s"${RiderConfig.spark.clientLogRootPath}/streams/$appName-${CommonUtils.currentNodSec}.log"

  def getStreamTime(time: Option[String]) = {
    val timeValue = time.getOrElse("")
    if (timeValue.nonEmpty) timeValue.split("\\.")(0) else null
  }

  def getDefaultJvmConf: RiderJVMConfig = {
    lazy val driverConf = RiderConfig.spark.driverExtraConf
    lazy val executorConf = RiderConfig.spark.executorExtraConf
    RiderJVMConfig(driverConf, executorConf)
  }

  def getDefaultSparkConf = {
    RiderConfig.spark.sparkConfig
  }

  def checkYarnAppNameUnique(userDefinedName: String, projectId: Long): Boolean = {
    val projectName = Await.result(projectDal.getById(projectId), minTimeOut).get.name
    val realName = genStreamNameByProjectName(projectName, userDefinedName)
    if (Await.result(streamDal.findByFilter(_.name === realName), minTimeOut).nonEmpty) {
      false
    } else {
      if (Await.result(jobDal.findByFilter(_.name === realName), minTimeOut).nonEmpty) false
      else true
    }
  }

  def formatOffset(offset: String): String = {
    offset.split(",").sortBy(partOffset => partOffset.split(":")(0).toLong).mkString(",")
  }

  def getDriftStreamsByStreamId(streamId: Long): Seq[SimpleStreamInfo] = {
    val preStream = Await.result(streamDal.findById(streamId), minTimeOut).head
    val streamKafkaUrl = streamDal.getStreamKafkaMap(Seq(streamId))(streamId)
    val sameInstanceIds = Await.result(instanceDal.findByFilter(_.connUrl === streamKafkaUrl), minTimeOut).map(_.id)
    val driftStreams = Await.result(streamDal.findByFilter(stream =>
      stream.streamType === preStream.streamType &&
        stream.functionType === preStream.functionType &&
        stream.projectId === preStream.projectId &&
        stream.id =!= streamId), minTimeOut).filter(stream => sameInstanceIds.contains(stream.instanceId))
    val driftStreamsKafkaMap = instanceDal.getStreamKafka(driftStreams.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
    driftStreams.map(
      stream => {
        SimpleStreamInfo(stream.id, stream.name, 0, driftStreamsKafkaMap(stream.id).instance, streamDal.getStreamTopicsName(stream.id)._2)
      }
    )
  }

  def containsTopic(streamId: Long, dbId: Long): Boolean = {
    val streamTopicRel = Await.result(streamInTopicDal.findByFilter(rel => rel.streamId === streamId && rel.nsDatabaseId === dbId), minTimeOut)
    if (streamTopicRel.nonEmpty) true
    else false
  }


  def hidePid(stream: Stream): Stream = {
    if (stream != null && stream.status == "starting") {
      Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.functionType,
        stream.JVMDriverConfig, stream.JVMExecutorConfig, stream.othersConfig, stream.startConfig,
        stream.launchConfig, stream.specialConfig, None, stream.logPath, stream.status, stream.startedTime, stream.stoppedTime,
        stream.active, stream.userTimeInfo)
    } else stream
  }

  def hidePid(streams: Seq[Stream]): Seq[Stream] = {
    if (streams != null && streams.nonEmpty) {
      streams.map(stream => {
        hidePid(stream)
      })
    } else streams
  }

  //  def getConsumedOffset(streamId: Long, dbId: Long, topic: String): String = {
  //    val stream = Await.result(streamDal.findById(streamId), minTimeOut).head
  //    val feedbackOffsetOpt = Await.result(feedbackOffsetDal.getLatestOffset(streamId, topic), minTimeOut)
  //    val startOffsetOpt = Await.result(streamInTopicDal.findByFilter(rel => rel.streamId === streamId && rel.nsDatabaseId === dbId), minTimeOut)
  //
  //    val offset =
  //      if (startOffsetOpt.nonEmpty) {
  //        if (feedbackOffsetOpt.nonEmpty) {
  //          if (stream.startedTime.nonEmpty && stream.startedTime != null &&
  //            DateUtils.yyyyMMddHHmmss(feedbackOffsetOpt.get.umsTs) > DateUtils.yyyyMMddHHmmss(stream.startedTime.get))
  //            feedbackOffsetOpt.get.partitionOffsets
  //          else startOffsetOpt.head.partitionOffsets
  //        } else startOffsetOpt.head.partitionOffsets
  //      } else throw new Exception("get consumed offset failed.")
  //
  //    formatOffset(offset)
  //  }

}