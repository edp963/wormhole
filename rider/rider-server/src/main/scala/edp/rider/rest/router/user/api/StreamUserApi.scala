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


package edp.rider.rest.router.user.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.RiderStarter.modules._
import edp.rider.common.Action._
import edp.rider.common.{RiderConfig, RiderLogger, StreamStatus}
import edp.rider.kafka.KafkaUtils._
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import edp.rider.rest.util.ResponseUtils.{getHeader, _}
import edp.rider.rest.util.StreamUtils._
import edp.rider.rest.util.UdfUtils._
import edp.rider.rest.util.{AuthorizationProvider, StreamUtils}
import edp.rider.service.util.CacheMap
import edp.rider.spark.SparkJobClientLog
import edp.rider.spark.SubmitSparkJob.runShellCommand
import edp.wormhole.common.util.JsonUtils.json2caseClass
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}

class StreamUserApi(jobDal: JobDal, streamDal: StreamDal, projectDal: ProjectDal, streamUdfDal: RelStreamUdfDal, inTopicDal: StreamInTopicDal, flowDal: FlowDal) extends BaseUserApiImpl(streamDal) with RiderLogger with JsonSerializer {

  def postRoute(route: String): Route = path(route / LongNumber / "streams") {
    id =>
      post {
        entity(as[SimpleStream]) {
          simple =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session => {
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  postResponse(id, simple, session)
                }
              }
            }
        }
      }
  }

  private def postResponse(projectId: Long, simpleStream: SimpleStream, session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      try {
        val formatCheck = checkConfigFormat(simpleStream.startConfig, simpleStream.launchConfig, simpleStream.sparkConfig.getOrElse(""))
        if (formatCheck._1) {
          val projectName = Await.result(projectDal.findById(projectId), minTimeOut).get.name
          val streamName = genStreamNameByProjectName(projectName, simpleStream.name)
          val insertStream = Stream(0, streamName, simpleStream.desc, projectId,
            simpleStream.instanceId, simpleStream.streamType, simpleStream.sparkConfig, simpleStream.startConfig, simpleStream.launchConfig,
            None, None, "new", None, None, active = true, currentSec, session.userId, currentSec, session.userId)
          if (StreamUtils.checkYarnAppNameUnique(simpleStream.name, projectId)) {
            onComplete(streamDal.insert(insertStream).mapTo[Stream]) {
              case Success(stream) =>
                CacheMap.streamCacheMapRefresh
                val streamDetail = streamDal.getBriefDetail(Some(projectId), Some(Seq(stream.id)))
                complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail.head))
              case Failure(ex) =>
                riderLogger.error(s"user ${
                  session.userId
                } insert stream failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }
          } else {
            riderLogger.warn(s"user ${session.userId} check stream name ${simpleStream.name} already exists success.")
            complete(OK, getHeader(409, s"${simpleStream.name} already exists", session))
          }
        } else {
          riderLogger.error(s"user ${
            session.userId
          } insert stream failed caused by ${formatCheck._2}.")
          complete(OK, getHeader(400, formatCheck._2, session))
        }
      } catch {
        case ex: Exception =>
          riderLogger.error(s"user ${
            session.userId
          } get stream detail failed", ex)
          complete(OK, getHeader(451, ex.getMessage, session))
      }
    } else {
      riderLogger.error(s"user ${
        session.userId
      } doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
    }
  }

  def getByFilterRoute(route: String): Route = path(route / LongNumber / "streams") {
    projectId =>
      get {
        parameter('streamName.as[String].?, 'streamType.as[String].?) {
          (streamNameOpt, streamTypeOpt) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  getByFilterResponse(projectId, streamNameOpt, streamTypeOpt, session)
                }
            }
        }
      }
  }

  private def getByFilterResponse(projectId: Long, streamNameOpt: Option[String], streamTypeOpt: Option[String], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      try {
        (streamNameOpt, streamTypeOpt) match {
          case (Some(name), None) =>
            if (StreamUtils.checkYarnAppNameUnique(name, projectId)) {
              riderLogger.info(s"user ${session.userId} check stream name $name doesn't exist success.")
              complete(OK, ResponseJson[String](getHeader(200, session), name))
            } else {
              riderLogger.warn(s"user ${session.userId} check stream name $name already exists success.")
              complete(OK, getHeader(409, s"$name already exists", session))
            }
          //            val projectName = Await.result(projectDal.findById(projectId), minTimeOut).get.name
          //            val realName = genStreamNameByProjectName(projectName, name)
          //            onComplete(streamDal.checkYarnAppNameUnique(realName).mapTo[Seq[Stream]]) {
          //              case Success(streams) =>
          //                if (streams.isEmpty) {
          //                  riderLogger.info(s"user ${session.userId} check stream name $name doesn't exist success.")
          //                  complete(OK, ResponseJson[String](getHeader(200, session), name))
          //                }
          //                else {
          //                  riderLogger.warn(s"user ${session.userId} check stream name $name already exists success.")
          //                  complete(OK, getHeader(409, s"$name already exists", session))
          //                }
          //              case Failure(ex) =>
          //                riderLogger.error(s"user ${session.userId} check stream name $name does exist failed", ex)
          //                complete(OK, getHeader(451, ex.getMessage, session))
          //            }
          case (None, Some(streamType)) =>
            val streams = streamDal.getBriefDetail(Some(projectId)).filter(_.stream.streamType == streamType).sortBy(_.stream.name)
            riderLogger.info(s"user ${session.userId} select streams where streamType is $streamType success.")
            complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
          case (None, None) =>
            val streams = streamDal.getBriefDetail(Some(projectId))
            riderLogger.info(s"user ${session.userId} select streams where project id is $projectId success.")
            complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
          case (_, _) =>
            riderLogger.error(s"user ${session.userId} request url is not supported.")
            complete(OK, ResponseJson[String](getHeader(403, session), msgMap(403)))
        }
      } catch {
        case ex: Exception =>
          riderLogger.error(s"user ${session.userId} refresh all streams failed", ex)
          complete(OK, getHeader(451, ex.getMessage, session))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
    }
  }

  override def getByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber) {
    (projectId, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                val stream = streamDal.getStreamDetail(Some(projectId), Some(Seq(streamId))).head
                riderLogger.info(s"user ${session.userId} select streams where project id is $projectId success.")
                complete(OK, ResponseJson[StreamDetail](getHeader(200, session), stream))
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }

  def putRoute(route: String): Route = path(route / LongNumber / "streams") {
    id =>
      put {
        entity(as[PutStream]) {
          putStream: PutStream =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session => {
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  putResponse(id, putStream, session)
                }
              }
            }
        }
      }
  }

  private def putResponse(projectId: Long, putStream: PutStream, session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val formatCheck = checkConfigFormat(putStream.startConfig, putStream.launchConfig, putStream.sparkConfig.getOrElse(""))
      if (formatCheck._1) {
        onComplete(streamDal.updateByPutRequest(putStream, session.userId).mapTo[Int]) {
          case Success(_) =>
            val stream = streamDal.getBriefDetail(Some(projectId), Some(Seq(putStream.id))).head
            riderLogger.info(s"user ${session.userId} update stream ${putStream.id} success.")
            complete(OK, ResponseJson[StreamDetail](getHeader(200, session), stream))
          case Failure(ex) =>
            riderLogger.error(s"user ${session.userId} update stream ${putStream.id} failed", ex)
            complete(OK, getHeader(451, session))
        }
      } else {
        riderLogger.error(s"user ${session.userId} update stream failed caused by ${formatCheck._2}")
        complete(OK, getHeader(400, formatCheck._2, session))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
    }
  }

  //  def getDefaultSparkConfList(route: String): Route = path(route / "streams" / "default" / "config") {
  //    authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
  //      session =>
  //        if (session.roleType != "user") {
  //          riderLogger.warn(s"${
  //            session.userId
  //          } has no permission to access it.")
  //          complete(OK, getHeader(403, session))
  //        }
  //        else {
  //          val defaultConf = streamDal.getDef
  //          complete(OK, ResponseJson[String](getHeader(200, session), defaultConf))
  //        }
  //    }
  //
  //  }

  def getLogByStreamId(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "logs") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                onComplete(streamDal.getStreamNameByStreamID(streamId).mapTo[Stream]) {
                  case Success(stream) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } refresh stream log where stream id is $streamId success.")
                    val log = SparkJobClientLog.getLogByAppName(stream.name, stream.logPath.getOrElse(""))
                    complete(OK, ResponseJson[String](getHeader(200, session), log))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } refresh stream log where stream id is $streamId failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $id.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }

  def stopRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "stop") {
    (id, streamId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                val stream = Await.result(streamDal.findById(streamId), minTimeOut).get
                if (checkAction(STOP.toString, stream.status)) {
                  val status = stopStream(stream.sparkAppid, stream.status)
                  riderLogger.info(s"user ${
                    session.userId
                  } stop stream $streamId success.")
                  onComplete(streamDal.updateByStatus(streamId, status, session.userId, stream.logPath.getOrElse("")).mapTo[Int]) {
                    case Success(_) =>
                      val streamDetail = streamDal.getBriefDetail(Some(id), Some(Seq(streamId))).head
                      complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail))
                    case Failure(ex) =>
                      riderLogger.error(s"user ${session.userId} stop stream where project id is $id failed", ex)
                      complete(OK, getHeader(451, session))
                  }
                } else {
                  riderLogger.info(s"user ${session.userId} can't stop stream $streamId now")
                  complete(OK, getHeader(406, s"stop is forbidden", session))
                }

              }
              else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $id.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }

  private def checkAction(action: String, status: String): Boolean = {
    if (getDisableActions(StreamStatus.withName(status)).contains(action)) false
    else true
  }

  def renewRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "renew") {
    (id, streamId) =>
      put {
        entity(as[Option[StreamDirective]]) {
          streamDirective =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  //                  renewResponse(id, streamId, streamDirective, session)
                  startResponse(id, streamId, streamDirective, session) //todo
                }
            }
        }
      }
  }

  private def renewResponse(projectId: Long, streamId: Long, streamDirectiveOpt: Option[StreamDirective], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val stream = Await.result(streamDal.findById(streamId), minTimeOut).get
      if (checkAction(RENEW.toString, stream.status)) {
        //        renewStreamDirective(streamId, streamDirectiveOpt, session.userId)
        riderLogger.info(s"user ${session.userId} renew stream $streamId success")
        val streamDetail = streamDal.getBriefDetail(Some(projectId), Some(Seq(streamId))).head
        complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail))
      } else {
        riderLogger.info(s"user ${session.userId} can't stop stream $streamId now")
        complete(OK, getHeader(406, s"renew is forbidden", session))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
    }
  }

  def startRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "start") {
    (id, streamId) =>
      put {
        entity(as[Option[StreamDirective]]) {
          streamDirective =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  startResponse(id, streamId, streamDirective, session)
                }
            }
        }
      }
  }

  private def startStreamDirective(streamId: Long, streamDirectiveOpt: Option[StreamDirective], userId: Long) = {
    // delete pre stream zk node
    removeStreamDirective(streamId, userId)
    // set new stream directive
    if (streamDirectiveOpt.nonEmpty) {
      val streamDirective = streamDirectiveOpt.get
      genUdfsStartDirective(streamId, streamDirective.udfInfo, userId)
      genTopicsStartDirective(streamId, streamDirective.topicInfo, userId)
    } else {
      Await.result(streamUdfDal.deleteByFilter(_.streamId === streamId), minTimeOut)
    }
  }

  //  private def renewStreamDirective(streamId: Long, streamDirectiveOpt: Option[StreamDirective], userId: Long) = {
  //    if (streamDirectiveOpt.nonEmpty) {
  //      val streamDirective = streamDirectiveOpt.get
  //      if (streamDirective.udfInfo.nonEmpty) {
  //        val insertUdfs = streamDirective.udfInfo.get.map(
  //          id => RelStreamUdf(0, streamId, id, currentSec, userId, currentSec, userId)
  //        )
  //        Await.result(streamUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
  //        sendUdfDirective(streamId,
  //          streamUdfDal.getStreamUdf(Seq(streamId)).filter(udf => streamDirective.udfInfo.get.contains(udf.id)),
  //          userId)
  //      }
  //      val topicMap = inTopicDal.getStreamTopic(Seq(streamId), false).map(topic => (topic.id, topic.name)).toMap
  //      if (streamDirective.topicInfo.nonEmpty) {
  //        val updateOffsets = streamDirective.topicInfo.get.map(
  //          topic => {
  //            Await.result(inTopicDal.updateOffset(streamId, topic.id, topic.partitionOffsets, topic.rate, userId), minTimeOut)
  //            StreamTopicTemp(topic.id, streamId, topicMap(topic.id), topic.partitionOffsets, topic.rate)
  //          }
  //        )
  //        sendTopicDirective(streamId, updateOffsets, userId)
  //      }
  //    }
  //  }

  private def startResponse(projectId: Long, streamId: Long, streamDirectiveOpt: Option[StreamDirective], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val streamDetail = streamDal.getBriefDetail(Some(projectId), Some(Seq(streamId))).head
      val stream = streamDetail.stream
      if (checkAction(START.toString, stream.status)) {
        try {
          val project: Project = Await.result(projectDal.findById(projectId), minTimeOut).head
          val (projectTotalCore, projectTotalMemory) = (project.resCores, project.resMemoryG)
          val (jobUsedCore, jobUsedMemory, _) = jobDal.getProjectJobsUsedResource(projectId)
          val (streamUsedCore, streamUsedMemory, _) = streamDal.getProjectStreamsUsedResource(projectId)
          val currentConfig = json2caseClass[StartConfig](stream.startConfig)
          val currentNeededCore = currentConfig.driverCores + currentConfig.executorNums * currentConfig.perExecutorCores
          val currentNeededMemory = currentConfig.driverMemory + currentConfig.executorNums * currentConfig.perExecutorMemory
          if ((projectTotalCore - jobUsedCore - streamUsedCore - currentNeededCore) < 0 || (projectTotalMemory - jobUsedMemory - streamUsedMemory - currentNeededMemory) < 0) {
            riderLogger.warn(s"user ${session.userId} start stream ${stream.id} failed, caused by resource is not enough")
            complete(OK, setFailedResponse(session, "resource is not enough"))
          } else {
            startStreamDirective(streamId, streamDirectiveOpt, session.userId)
            //            runShellCommand(s"rm -rf ${SubmitSparkJob.getLogPath(stream.name)}")
            //            val logPath = getLogPath(stream.name)
            //            startStream(streamDetail, logPath)
            riderLogger.info(s"user ${session.userId} start stream $streamId success.")
            //            onComplete(streamDal.updateByStatus(streamId, StreamStatus.STARTING.toString, session.userId, logPath).mapTo[Int]) {
            //              case Success(_) =>
            //                val streamDetail = streamDal.getBriefDetail(Some(projectId), Some(Seq(streamId))).head
            //                complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail))
            //              case Failure(ex) =>
            //                riderLogger.error(s"user ${session.userId} start stream where project id is $projectId failed", ex)
            //                complete(OK, getHeader(451, session))
            //            }
            complete(OK, ResponseJson[StartResponse](getHeader(200, session), StartResponse(getDisableActions(StreamStatus.STARTING))))
          }
        } catch {
          case ex: Exception =>
            riderLogger.error(s"user ${session.userId} get resources for project ${projectId}  failed when start new stream", ex)
            complete(OK, setFailedResponse(session, ex.getMessage))
        }
      } else {
        riderLogger.info(s"user ${session.userId} can't start stream $streamId now")
        complete(OK, setFailedResponse(session, "start is forbidden"))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, setFailedResponse(session, "Insufficient Permission"))
    }
  }

  def deleteStream(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "delete") {
    (id, streamId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            if (session.projectIdList.contains(id)) {
              try {
                val streamDetailOpt = streamDal.getBriefDetail(Some(id), Some(Seq(streamId))).headOption
                streamDetailOpt match {
                  case Some(streamDetail) =>
                    if (checkAction(DELETE.toString, streamDetail.stream.status)) {
                      val flows = Await.result(flowDal.findByFilter(_.streamId === streamId), minTimeOut)
                      if (flows.nonEmpty) {
                        riderLogger.info(s"user ${session.userId} can't delete stream $streamId now, please delete flow ${flows.map(_.id).mkString(",")} first")
                        complete(OK, getHeader(412, s"please delete flow ${flows.map(_.id).mkString(",")} first", session))
                      } else {
                        removeStreamDirective(streamId, session.userId)
                        if (streamDetail.stream.sparkAppid.getOrElse("") != "") {
                          runShellCommand("yarn application -kill " + streamDetail.stream.sparkAppid.get)
                          riderLogger.info(s"user ${session.userId} stop stream $streamId success")
                        }
                        Await.result(streamDal.deleteById(streamId), minTimeOut)
                        Await.result(inTopicDal.deleteByFilter(_.streamId === streamId), minTimeOut)
                        CacheMap.streamCacheMapRefresh
                        //                        runShellCommand(s"rm -rf ${SubmitSparkJob.getLogPath(streamDetail.stream.name)}")
                        riderLogger.info(s"user ${session.userId} delete stream $streamId success")
                        complete(OK, getHeader(200, session))
                      }
                    }
                    else {
                      riderLogger.info(s"user ${session.userId} can't stop stream $streamId now")
                      complete(OK, getHeader(406, s"start is forbidden", session))
                    }
                  case None => complete(OK, getHeader(200, session))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"delete stream $streamId failed", ex)
                  throw ex
              }
            } else {
              riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
              complete(OK, getHeader(403, session))
            }
        }
      }
  }

  def getDefaultJvmConf(route: String): Route = path(route / "streams" / "default" / "config" / "jvm") {
    authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
      session =>
        if (session.roleType != "user") {
          riderLogger.warn(s"${
            session.userId
          } has no permission to access it.")
          complete(OK, getHeader(403, session))
        }
        else {
          val defaultConf = StreamUtils.getDefaultJvmConf
          complete(OK, ResponseJson[String](getHeader(200, session), defaultConf))
        }
    }

  }

  def getDefaultSparkConf(route: String): Route = path(route / "streams" / "default" / "config" / "spark") {
    authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
      session =>
        if (session.roleType != "user") {
          riderLogger.warn(s"${
            session.userId
          } has no permission to access it.")
          complete(OK, getHeader(403, session))
        }
        else {
          val defaultConf = StreamUtils.getDefaultSparkConf
          complete(OK, ResponseJson[String](getHeader(200, session), defaultConf))
        }
    }

  }

  def getTopicsRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "topics") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                getTopicsResponse(id, streamId, session)
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $id.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }

      }
  }

  private def getTopicsResponse(projectId: Long, streamId: Long, session: SessionClass): Route = {
    //    try {
    //      val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(Seq(streamId))).head
    //      if (streamDetail.topicInfo.nonEmpty) {
    //        val consumedOffsets = streamDetail.topicInfo.map(topic => ConsumedLatestOffset(topic.id, topic.name, topic.rate, topic.partitionOffsets))
    //        val topicSeq = inTopicDal.getStreamTopic(Seq(streamId))
    //        val kafkaOffsets = topicSeq.map(topic =>
    //          KafkaLatestOffset(topic.id, topic.name, getKafkaLatestOffset(streamDetail.kafkaInfo.connUrl, topic.name)))
    //        val finalOffsets = consumedOffsets.map(topic => {
    //          val consumedPart = topic.partitionOffsets.split(",").length
    //          val kafkaOffset = kafkaOffsets.filter(_.id == topic.id).head
    //          val kafkaPart = kafkaOffset.partitionOffsets.split(",").length
    //          val offset = if (kafkaPart > consumedPart) {
    //            topic.partitionOffsets + "," + (consumedPart until kafkaPart).toList.mkString(":0,") + ":0"
    //          } else if (kafkaPart < consumedPart) {
    //            topic.partitionOffsets.split(",").take(kafkaPart).mkString(",")
    //          } else topic.partitionOffsets
    //          ConsumedLatestOffset(topic.id, topic.name, topic.rate, offset)
    //        })
    //        riderLogger.info(s"user ${session.userId} get stream $streamId topics latest offset success")
    //        complete(OK, ResponseJson[TopicLatestOffset](getHeader(200, session), TopicLatestOffset(finalOffsets, kafkaOffsets)))
    //      } else {
    //        riderLogger.info(s"user ${session.userId} get stream $streamId topics latest offset success, there is no topics")
    //        complete(OK, getHeader(200, "There is no topics now.", session))
    //      }
    //    } catch {
    //      case ex: Exception =>
    //        riderLogger.info(s"user ${session.userId} get stream $streamId topics latest offset failed", ex)
    //        complete(OK, getHeader(451, session))
    //    }
    val topics = streamDal.getTopicsAllOffsets(streamId)
    riderLogger.info(s"user ${session.userId} get stream $streamId topics success.")
    complete(OK, ResponseJson[GetTopicsResponse](getHeader(200, session), topics))
  }

  def getResourceByProjectIdRoute(route: String): Route = path(route / LongNumber / "resources") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                try {
                  val project: Project = Await.result(projectDal.findById(id), minTimeOut).head
                  val (projectTotalCore, projectTotalMemory) = (project.resCores, project.resMemoryG)
                  val (jobUsedCore, jobUsedMemory, jobSeq) = jobDal.getProjectJobsUsedResource(id)
                  val (streamUsedCore, streamUsedMemory, streamSeq) = streamDal.getProjectStreamsUsedResource(id)
                  val appResources = jobSeq ++ streamSeq
                  val resources = Resource(projectTotalCore, projectTotalMemory, projectTotalCore - jobUsedCore - streamUsedCore, projectTotalMemory - jobUsedMemory - streamUsedMemory, appResources)
                  riderLogger.info(s"user ${session.userId} select all resources success where project id is $id.")
                  complete(OK, ResponseJson[Resource](getHeader(200, session), resources))
                } catch {
                  case ex: Exception =>
                    riderLogger.error(s"user ${session.userId} get resources for project ${id}  failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $id.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }

  def postUserDefinedTopicRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "topics" / "userdefined") {
    (id, streamId) =>
      post {
        entity(as[PostUserDefinedTopic]) {
          postTopic => {
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    try {
                      if (Await.result(streamDal.findById(streamId), minTimeOut).nonEmpty) {
                        postUserDefinedTopicResponse(id, streamId, postTopic, session)
                      } else {
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed caused by stream $streamId not found.")
                        complete(OK, setFailedResponse(session, "stream not found."))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed", ex)
                        complete(OK, setFailedResponse(session, ex.getMessage))
                    }
                  } else {
                    riderLogger.error(s"user ${
                      session.userId
                    } doesn't have permission to access the project $id.")
                    complete(OK, setFailedResponse(session, "Insufficient Permission"))
                  }
                }
            }
          }
        }
      }
  }

  def postUserDefinedTopicResponse(projectId: Long, streamId: Long, postTopic: PostUserDefinedTopic, session: SessionClass): Route = {
    // topic duplication check
    if (streamDal.checkTopicExists(streamId, postTopic.name)) {
      throw new Exception("stream topic relation already exists.")
    }
    val kafkaInfo = streamDal.getKafkaInfo(streamId)
    // get kafka earliest/latest offset
    val latestOffset = getKafkaLatestOffset(kafkaInfo._2, postTopic.name)
    val earliestOffset = getKafkaEarliestOffset(kafkaInfo._2, postTopic.name)

    // insert userdefined topic
    val topicInsert = Await.result(udfTopicDal.insert(
      StreamUserDefinedTopic(0, streamId, kafkaInfo._1, postTopic.name, earliestOffset, RiderConfig.spark.topicDefaultRate, currentSec, session.userId, currentSec, session.userId)), minTimeOut)

    // response
    val topicResponse = TopicAllOffsets(topicInsert.id, topicInsert.topic, topicInsert.rate, earliestOffset, earliestOffset, latestOffset)
    riderLogger.info(s"user ${session.userId} insert user defined topic success.")
    complete(OK, ResponseJson[TopicAllOffsets](getHeader(200, session), topicResponse))
  }

  def deleteUserDefinedTopicRoute(route: String) = path(route / LongNumber / "streams" / LongNumber / "topics" / "userdefined" / LongNumber) {
    (id, streamId, topicId) =>
      delete {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                try {
                  deleteUserDefinedTopicResponse(id, streamId, topicId, session)
                } catch {
                  case ex: Exception =>
                    riderLogger.error(s"user ${session.userId} delete user defined topic $topicId failed", ex)
                    complete(OK, setFailedResponse(session, ex.getMessage))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $id.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }

  def deleteUserDefinedTopicResponse(id: Long, streamId: Long, topicId: Long, session: SessionClass): Route = {
    Await.result(udfTopicDal.deleteById(topicId), minTimeOut)
    riderLogger.info(s"user ${session.userId} delete user defined topic $topicId success.")
    complete(OK, setSuccessResponse(session))
  }

}
