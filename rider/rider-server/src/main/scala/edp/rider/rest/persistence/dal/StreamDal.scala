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


package edp.rider.rest.persistence.dal

import edp.rider.common.StreamRefresh._
import edp.rider.common.{AppInfo, AppResult, RiderConfig, RiderLogger}
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.{BaseDal, BaseDalImpl}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.spark.SparkJobClientLog
import edp.rider.spark.SparkStatusQuery._
import edp.rider.service.util._
import edp.wormhole.common.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class StreamDal(streamTable: TableQuery[StreamTable], projectTable: TableQuery[ProjectTable], feedbackOffsetTable: TableQuery[FeedbackOffsetTable], instanceTable: TableQuery[InstanceTable], nsDatabaseTable: TableQuery[NsDatabaseTable], relProjectNsTable: TableQuery[RelProjectNsTable], streamInTopicTable: TableQuery[StreamInTopicTable], namespaceTable: TableQuery[NamespaceTable], directiveDal: BaseDal[DirectiveTable, Directive]) extends BaseDalImpl[StreamTable, Stream](streamTable) with RiderLogger {

  def adminGetAll: Future[Seq[StreamAdmin]] = {
    try {
      val streamTemp = refreshStreamsByProjectId()
      val projectMap = mutable.HashMap.empty[Long, String]
      val projectIds: List[Long] = streamTemp.map(_.stream.projectId).distinct.toList
      val projectSeq = Await.result(db.run(projectTable.filter(_.id inSet projectIds).result).mapTo[Seq[Project]], minTimeOut)
      projectSeq.foreach(project => projectMap(project.id) = project.name)
      Future(streamTemp.map(stream => StreamAdmin(stream.stream, stream.disableActions, projectMap(stream.stream.projectId), stream.kafkaName, stream.kafkaConnection, stream.topicInfo)))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"admin get all streams failed", ex)
        throw ex
    }
  }

  def refreshStreamsByProjectId(id: Option[Long] = None, streamId: Option[Long] = None): Seq[StreamSeqTopicActions] = {
    try {
      val streams = getUpdateStream(Await.result(getStreamsByProjectId(id, streamId), maxTimeOut))
      Await.result(super.update(streams.map(_._1)), maxTimeOut)
      streams.map(stream => {
        //        val startTime = if (stream._1.startedTime.getOrElse("") == "") Some("") else stream._1.startedTime
        //        val stopTime = if (stream._1.stoppedTime.getOrElse("") == "") Some("") else stream._1.stoppedTime
        //        val newStream = Stream(stream._1.id, stream._1.name, stream._1.desc, stream._1.projectId, stream._1.instanceId, stream._1.streamType,
        //          stream._1.sparkConfig, stream._1.startConfig, stream._1.launchConfig, stream._1.sparkAppid, stream._1.logpath, stream._1.status,
        //          startTime, stopTime, stream._1.active, stream._1.createTime, stream._1.createBy, stream._1.updateTime, stream._1.updateBy)

        getReturnRes(stream._2)
      })
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get streams by project id $id, stream id $streamId failed", ex)
        throw ex
    }
  }

  def getStreamsByProjectId(idOpt: Option[Long] = None, streamIdOpt: Option[Long] = None): Future[Seq[StreamSeqTopic]] = {
    try {
      val realStream: Query[StreamTable, Stream, Seq] = (idOpt, streamIdOpt) match {
        case (Some(id), Some(streamId)) => streamTable.filter(stream => stream.active === true && stream.id === streamId && stream.projectId === id)
        case (Some(id), None) => streamTable.filter(stream => stream.active === true && stream.projectId === id)
        case (None, Some(streamId)) => streamTable.filter(stream => stream.active === true && stream.id === streamId)
        case (None, None) => streamTable.filter(_.active === true)
      }
      db.run((realStream join instanceTable.filter(_.active === true) on (_.instanceId === _.id)).map {
        case (stream, instance) => (stream, instance.nsInstance, instance.connUrl) <> (StreamTopicTem.tupled, StreamTopicTem.unapply)
      }.result).map[Seq[StreamSeqTopic]] {
        tempSeq =>
          tempSeq.map(temp => {
            val seqTopic: Seq[SimpleTopic] = getSimpleTopicSeq(temp.stream.id)
            StreamSeqTopic(temp.stream, temp.kafkaName, temp.kafkaConnection, seqTopic)
          })
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get streams by project id $idOpt, stream id $streamIdOpt failed", ex)
        throw ex
    }
  }

  def getSimpleTopicSeq(streamId: Long): Seq[SimpleTopic] =
    Await.result(db.run((streamInTopicTable.filter(_.active === true).filter(_.streamId === streamId) join nsDatabaseTable.filter(_.active === true) on (_.nsDatabaseId === _.id)).map {
      case (inTopic, database) => (database.id, database.nsDatabase, inTopic.partitionOffsets, inTopic.rate) <> (SimpleTopic.tupled, SimpleTopic.unapply)
    }.result).mapTo[Seq[SimpleTopic]], minTimeOut)

  def updateOffsetFromFeedback(streamId: Long, userId: Long): Seq[TopicDetail] = {
    try {
      val topicSeq = Await.result(getTopicDetailByStreamId(streamId), minTimeOut)
      if (topicSeq.size != 0) {
        val topicFeedbackSeq = Await.result(db.run(feedbackOffsetTable.filter(_.streamId === streamId).sortBy(_.feedbackTime.desc).take(topicSeq.size + 1).result).mapTo[Seq[FeedbackOffset]], minTimeOut)
        val map = topicFeedbackSeq.map(topic => (topic.topicName, topic.partitionOffsets)).toMap
        val updateSeq = new ArrayBuffer[StreamInTopic]
        val latestOffset = topicSeq.map(
          topic => {
            if (map.contains(topic.name)) {
              updateSeq += StreamInTopic(topic.id, topic.streamId, topic.nsInstanceId, topic.nsDatabaseId,
                map.get(topic.name).get, topic.rate, topic.active, topic.createTime, topic.createBy, currentSec, userId)
              TopicDetail(topic.id, topic.streamId, topic.nsInstanceId, topic.nsDatabaseId, topic.partition,
                map.get(topic.name).get, topic.rate, topic.active, topic.createTime, topic.createBy, currentSec, userId, topic.name)
            }
            else topic
          }
        )
        if (updateSeq.size != 0)
          updateSeq.map(topic => Await.result(db.run(streamInTopicTable.filter(_.id === topic.id).update(topic)), minTimeOut))
        latestOffset
      }
      else topicSeq
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream $streamId latest offset from stream_feedback_offset table failed", ex)
        throw ex
    }
  }

  def getResource(projectId: Long): Future[Resource] = {
    try {
      val project = Await.result(db.run(projectTable.filter(_.id === projectId).result.head).mapTo[Project], minTimeOut)
      val streamSeq = super.findByFilter(stream => stream.projectId === projectId && (stream.status === "running" || stream.status === "waiting" || stream.status === "starting" || stream.status === "stopping")).mapTo[Seq[Stream]]
      val totalCores = project.resCores
      val totalMemory = project.resMemoryG
      var usedCores = 0
      var usedMemory = 0
      val streamResources = Await.result(streamSeq.map[Seq[StreamResource]] {
        streamSeq =>
          streamSeq.sortBy(_.id).map {
            stream =>
              val config = json2caseClass[StartConfig](stream.startConfig)
              usedCores += config.driverCores + config.executorNums * config.perExecutorCores
              usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
              StreamResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
          }
      }, minTimeOut)
      Future(Resource(totalCores, totalMemory, totalCores - usedCores, totalMemory - usedMemory, streamResources))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream resource information by project id $projectId failed", ex)
        throw ex
    }
  }

  def getUpdateStream(streamSeqTopic: Seq[StreamSeqTopic], action: String = "refresh"): Seq[(Stream, StreamSeqTopic)] = {
    val streamsToProcess: Seq[(StreamSeqTopic, String)] = streamSeqTopic.map(streamTopic => {
      val stream = streamTopic.stream
      val nextAction = nextActionRule(stream, action)
      (streamTopic, nextAction)
    })
    if (streamsToProcess.isEmpty) return Seq()
    val streamWithInfo: Seq[(StreamSeqTopic, AppInfo)] = statusRuleStream(streamsToProcess)
    val streamInfo = streamWithInfo.map(streamInfo => {
      val nextStatus = streamInfo._2
      val stream = streamInfo._1.stream
      val returnStartTime = if (nextStatus.startedTime == "" || nextStatus.startedTime == null) Some("") else Some(nextStatus.startedTime)
      val returnStopTime = if (nextStatus.finishedTime == "" || nextStatus.finishedTime == null) Some("") else Some(nextStatus.finishedTime)
      val returnStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, Some(nextStatus.appId), stream.logPath, nextStatus.appState, returnStartTime, returnStopTime, stream.active, stream.createTime, stream.createBy, stream.updateTime, stream.updateBy)

      val updateStartTime = if (nextStatus.startedTime == "") null else Some(nextStatus.startedTime)
      val updateStopTime = if (nextStatus.finishedTime == "") null else Some(nextStatus.finishedTime)
      (Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, Some(nextStatus.appId), stream.logPath, nextStatus.appState, updateStartTime, updateStopTime, stream.active, stream.createTime, stream.createBy, stream.updateTime, stream.updateBy), StreamSeqTopic(returnStream, streamInfo._1.kafkaName, streamInfo._1.kafkaConnection, streamInfo._1.topicInfo))
    })
    streamInfo
  }

  def getReturnRes(streamSeqTopic: StreamSeqTopic): StreamSeqTopicActions = {
    streamSeqTopic.stream.status match {
      case "new" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "renew,stop")
      case "waiting" =>
        val disableActions =
          if (streamSeqTopic.stream.sparkAppid.getOrElse("") == "")
            "start,stop"
          else "start"
        StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, disableActions)
      case "running" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "start")
      case "stopping" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "start,renew")
      case "stopped" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "stop,renew")
      case "failed" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "stop")
      case "starting" => StreamSeqTopicActions(streamSeqTopic.stream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "renew,stop,start")
    }
  }

  def getStreamById(streamId: Long) = {
    db.run((streamTable.filter(_.active === true).filter(_.id === streamId) join instanceTable.filter(_.active === true) on (_.instanceId === _.id)).map {
      case (stream, instance) =>
        (stream, instance.connUrl) <> (StreamWithBrokers.tupled, StreamWithBrokers.unapply)
    }.result.headOption).mapTo[Option[StreamWithBrokers]]
  }

  def getAllActiveStream: Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }

  def getStreamNameByStreamID(streamId: Long): Future[Stream] = {
    db.run(streamTable.filter(_.active === true).filter(_.id === streamId).result.head).mapTo[Stream]
  }

  def insertIntoStreamInTopic(streamInTopics: Seq[StreamInTopic]): Future[Seq[Long]] = {
    db.run(streamInTopicTable returning streamInTopicTable.map(_.id) ++= streamInTopics)
  }

  def getActiveStreamByProjectId(projectId: Long): Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).filter(_.projectId === projectId).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }

  def insertStreamReturnRes(insertStreams: Seq[Stream]): Future[Seq[Stream]] = {
    super.insert(insertStreams).map(streams => {
      streams.map(stream =>
        Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, Some(stream.sparkAppid.getOrElse("")), Some(stream.logPath.getOrElse("")), stream.status, Some(stream.startedTime.getOrElse("")), Some(stream.stoppedTime.getOrElse("")), stream.active, stream.createTime, stream.createBy, stream.updateTime, stream.updateBy))
    })
  }

  def nextActionRule(stream: Stream, action: String): String = {
    val currentStatus = stream.status
    if (action == REFRESH.toString) {
      if (currentStatus == "starting") REFRESHLOG.toString
      else REFRESHSPARK.toString
      //      REFRESHSPARK.toString
    }
    else
      action
  }

  def getConfList = {
    lazy val driverConf = RiderConfig.spark.driverExtraConf
    lazy val executorConf = RiderConfig.spark.executorExtraConf
    driverConf + "," + executorConf
  }

  def statusRuleStream(streams: Seq[(StreamSeqTopic, String)]) = {
    val fromTime =
      if (streams.nonEmpty && streams.exists(_._1.stream.startedTime.getOrElse("") != ""))
        streams.filter(_._1.stream.startedTime.getOrElse("") != "").map(_._1.stream.startedTime).min.getOrElse("")
      else ""
    //    riderLogger.info(s"fromTime: $fromTime")
    val appInfoList: List[AppResult] =
      if (fromTime == "") List() else getAllYarnAppStatus(fromTime).sortWith(_.appId < _.appId)
    //    riderLogger.info(s"app info size: ${appInfoList.size}")
    streams.map(
      streamAction => {
        val stream = streamAction._1.stream
        val action = streamAction._2
        val dbStatus = stream.status
        val dbUpdateTime = stream.updateTime
        val startedTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime.get
        val stoppedTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime.get
        val appInfo = {
          if (action == "start") AppInfo("", "starting", currentSec, null)
          else if (action == "stop") AppInfo("", "stopping", startedTime, stoppedTime)
          else {
            val sparkStatus: AppInfo = action match {
              case "refresh_spark" =>
                val s = getAppStatusByRest(appInfoList, stream.name, stream.status, startedTime, dbUpdateTime)
                //                riderLogger.info(s"spark status: $s")
                s
              case "refresh_log" =>
                val status = SparkJobClientLog.getAppStatusByLog(stream.name, dbStatus)
                //                riderLogger.info(s"log status: $status")
                status match {
                  case "running" =>
                    getAppStatusByRest(appInfoList, stream.name, status, startedTime, dbUpdateTime)
                  case "waiting" =>
                    val curInfo = getAppStatusByRest(appInfoList, stream.name, status, startedTime, dbUpdateTime)
                    AppInfo(curInfo.appId, curInfo.appState, startedTime, curInfo.finishedTime)
                  case "starting" => getAppStatusByRest(appInfoList, stream.name, status, startedTime, dbUpdateTime)
                  case "failed" => AppInfo("", "failed", null, null)
                }
              case _ => AppInfo("", stream.status, null, null)
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
                  case _ => AppInfo(stream.sparkAppid.getOrElse(""), "waiting", startedTime, stoppedTime)
                }
                case "running" =>
                  if (List("FAILED", "KILLED", "FINISHED").contains(sparkStatus.appState.toUpperCase)) {
                    AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)
                  }
                  else {
                    AppInfo(stream.sparkAppid.getOrElse(""), "running", startedTime, stoppedTime)
                  }
                case "stopping" =>
                  if (sparkStatus.appState == "KILLED") {
                    AppInfo(sparkStatus.appId, "stopped", sparkStatus.startedTime, sparkStatus.finishedTime)
                  }
                  else {
                    AppInfo(stream.sparkAppid.getOrElse(""), "stopping", startedTime, stoppedTime)
                  }
                case "new" =>
                  AppInfo(stream.sparkAppid.getOrElse(""), "new", startedTime, stoppedTime)
                case "stopped" =>
                  AppInfo(stream.sparkAppid.getOrElse(""), "stopped", startedTime, stoppedTime)
                case "failed" =>
                  sparkStatus.appState.toUpperCase match {
                    case "RUNNING" => AppInfo(sparkStatus.appId, "running", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "ACCEPTED" => AppInfo(sparkStatus.appId, "waiting", sparkStatus.startedTime, sparkStatus.finishedTime)
                    case "KILLED" | "FINISHED" | "FAILED" | _ => AppInfo(sparkStatus.appId, "failed", sparkStatus.startedTime, sparkStatus.finishedTime)

                  }
                case _ => AppInfo(stream.sparkAppid.getOrElse(""), dbStatus, startedTime, stoppedTime)
              }
              resStatus
            }
          }
        }
        (streamAction._1, appInfo)
      })

  }

  def updateStreamTable(stream: Stream): Future[Int] = {
    db.run(streamTable.filter(_.id === stream.id).update(stream))
  }

  def deleteStreamInTopicByStreamId(databaseIds: Seq[Long]): Future[Int] = {
    db.run(streamInTopicTable.filter(_.nsDatabaseId inSet databaseIds).delete)
  }

  def getStreamInTopicByStreamId(streamId: Long) = {
    db.run((streamInTopicTable.filter(_.streamId === streamId).filter(_.active === true) join nsDatabaseTable.filter(_.active === true) on (_.nsDatabaseId === _.id)).map {
      case (streamInTopic, nsDatabase) => (streamInTopic.id,
        streamInTopic.streamId,
        streamInTopic.nsInstanceId,
        streamInTopic.nsDatabaseId,
        nsDatabase.nsDatabase,
        nsDatabase.partitions.getOrElse(0),
        streamInTopic.partitionOffsets,
        streamInTopic.rate,
        streamInTopic.active,
        streamInTopic.createTime,
        streamInTopic.createBy,
        streamInTopic.updateTime,
        streamInTopic.updateBy) <> (StreamInTopicName.tupled, StreamInTopicName.unapply)
    }.result).mapTo[Seq[StreamInTopicName]]
  }

  def getStreamTopicPartition(streamId: Long): Future[Seq[StreamTopicPartition]] = {
    db.run((streamInTopicTable.filter(_.streamId === streamId).filter(_.active === true) join nsDatabaseTable.filter(_.active === true) on (_.nsDatabaseId === _.id)).map {
      case (streamInTopic, nsDatabase) => (
        streamInTopic.streamId,
        nsDatabase.nsDatabase,
        nsDatabase.partitions) <> (StreamTopicPartition.tupled, StreamTopicPartition.unapply)
    }.result).mapTo[Seq[StreamTopicPartition]]

  }

  def updateStreamInTopicTable(streamInTopics: Seq[StreamInTopic]) = {
    db.run(DBIO.seq(streamInTopics.map(streamInTopic => streamInTopicTable.filter(_.id === streamInTopic.id).update(streamInTopic)): _*))
  }

  def updateStreamsTable(streams: Seq[Stream]) = {
    db.run(DBIO.seq(streams.map(stream => streamTable.filter(_.id === stream.id).update(stream)): _*))
  }


  def getProjectNameById(id: Long): Future[Project] = {
    db.run(projectTable.filter(_.id === id).result.head).mapTo[Project]
  }

  def getKafkaByProjectId(projectId: Long): Future[Seq[Kafka]] = {
    db.run((relProjectNsTable.filter(_.projectId === projectId).filter(_.active === true) join namespaceTable.filter(_.active === true) on (_.nsId === _.id) join instanceTable.filter(_.active === true).filter(_.nsSys === "kafka") on (_._2.nsInstanceId === _.id)).map {
      case (proNamespace, instance) => (instance.id, instance.nsInstance) <> (Kafka.tupled, Kafka.unapply)
    }.distinct.result).mapTo[Seq[Kafka]]
  }

  def getTopicDetailByStreamId(streamId: Long) = {
    db.run((streamTable.filter(_.active === true).filter(_.id === streamId) join streamInTopicTable.filter(_.active === true) on (_.id === _.streamId) join nsDatabaseTable.filter(_.active === true) on (_._2.nsDatabaseId === _.id)).map {
      case (streamInTopic, database) => (streamInTopic._2.id, streamId, streamInTopic._2.nsInstanceId, database.id, database.partitions, streamInTopic._2.partitionOffsets, streamInTopic._2.rate, streamInTopic._2.active, streamInTopic._2.createTime, streamInTopic._2.createBy, streamInTopic._2.updateTime, streamInTopic._2.updateBy, database.nsDatabase) <> (TopicDetail.tupled, TopicDetail.unapply)
    }.result).mapTo[Seq[TopicDetail]]
  }

  def refreshTopicByStreamId(streamId: Long, userId: Long) = {
    updateOffsetFromFeedback(streamId, userId)
  }

  def getTopicByInstanceId(instanceId: Long) = {
    db.run((instanceTable.filter(_.active === true).filter(_.id === instanceId) join namespaceTable.filter(_.active === true) on (_.id === _.nsInstanceId) join nsDatabaseTable.filter(_.active === true) on (_._2.nsDatabaseId === _.id)).map {
      case (instance, database) => (database.id, database.nsDatabase, database.partitions.getOrElse(0)) <> (TopicSimple.tupled, TopicSimple.unapply)
    }.distinct.result).mapTo[Seq[TopicSimple]]
  }


  def generateStreamNameByProject(projectName: String, name: String): String = s"wormhole_${projectName}_$name"


  def checkStreamNameUnique(streamName: String) = {
    db.run(streamTable.filter(_.name === streamName).result)
  }

  def getStreamKafkaTopic(projectId: Long, streamId: Option[Long] = None, streamTypeOpt: Option[String] = None) = {
    val streamQuery = (streamId, streamTypeOpt) match {
      case (Some(id), None) => streamTable.filter(stream => stream.projectId === projectId && stream.active === true && stream.id === id)
      case (None, Some(streamType)) => streamTable.filter(stream => stream.projectId === projectId && stream.active === true && stream.streamType === streamType)
      case (_, _) => streamTable.filter(stream => stream.projectId === projectId && stream.active === true)
    }

    val streamSeq = db.run((streamQuery join instanceTable on (_.instanceId === _.id))
      .map {
        case (stream, instance) => (stream.id, stream.name, stream.streamType, instance.nsInstance, "") <> (StreamKafkaTopic.tupled, StreamKafkaTopic.unapply)
      }.result).mapTo[Seq[StreamKafkaTopic]]

    streamSeq.map[Seq[StreamKafkaTopic]] {
      streamSeq =>
        streamSeq.map(stream => {
          val topics = getSimpleTopicSeq(stream.id).map(_.name).mkString(",")
          StreamKafkaTopic(stream.id, stream.name, stream.streamType, stream.kafka, topics)
        })
    }
  }
}