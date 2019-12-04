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
import edp.rider.common.{RiderConfig, RiderLogger, StreamStatus, StreamType}
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities.{FlowPriorities, _}
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import edp.rider.rest.util.ResponseUtils.{getHeader, _}
import edp.rider.rest.util.StreamUtils._
import edp.rider.rest.util.{AuthorizationProvider, InstanceUtils, StreamUtils}
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.yarn.YarnClientLog
import edp.rider.zookeeper.PushDirective
import edp.wormhole.util.JsonUtils
import edp.rider.kafka.WormholeGetOffsetUtils._
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
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
        val formatCheck = checkConfigFormat(simpleStream.startConfig, simpleStream.launchConfig, simpleStream.JVMDriverConfig.getOrElse(""), simpleStream.JVMExecutorConfig.getOrElse(""), simpleStream.othersConfig.getOrElse(""))
        if (formatCheck._1) {
          val projectName = Await.result(projectDal.findById(projectId), minTimeOut).get.name
          val streamName = genStreamNameByProjectName(projectName, simpleStream.name)
          val insertStream = Stream(0, streamName, simpleStream.desc, projectId,
            simpleStream.instanceId, simpleStream.streamType, simpleStream.functionType, simpleStream.JVMDriverConfig, simpleStream.JVMExecutorConfig, simpleStream.othersConfig, simpleStream.startConfig, simpleStream.launchConfig, simpleStream.specialConfig,
            None, None, "new", None, None, active = true, UserTimeInfo(currentSec, session.userId, currentSec, session.userId))
          if (StreamUtils.checkYarnAppNameUnique(simpleStream.name, projectId)) {
            onComplete(streamDal.insert(insertStream).mapTo[Stream]) {
              case Success(stream) =>
                val streamDetail = streamDal.getBriefDetail(Some(projectId), Some(Seq(stream.id)))
                complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail.head))
              case Failure(ex) =>
                riderLogger.error(s"user ${
                  session.userId
                } insert stream failed", ex)
                complete(OK, setFailedResponse(session, ex.getMessage))
            }
          } else {
            riderLogger.warn(s"user ${session.userId} check stream name ${simpleStream.name} already exists success.")
            //complete(OK, getHeader(409, s"${simpleStream.name} already exists", session))
            complete(OK, setFailedResponse(session, s"${simpleStream.name} already exists"))
          }
        } else {
          riderLogger.error(s"user ${
            session.userId
          } insert stream failed caused by ${formatCheck._2}.")
          //complete(OK, getHeader(400, formatCheck._2, session))
          complete(OK, setFailedResponse(session, formatCheck._2))
        }
      } catch {
        case ex: Exception =>
          riderLogger.error(s"user ${
            session.userId
          } get stream detail failed", ex)
          //complete(OK, getHeader(451, ex.getMessage, session))
          complete(OK, setFailedResponse(session, ex.getMessage))
      }
    } else {
      riderLogger.error(s"user ${
        session.userId
      } doesn't have permission to access the project $projectId.")
      //complete(OK, getHeader(403, session))
      complete(OK, setFailedResponse(session, "Insufficient Permission"))
    }
  }

  def getByFilterRoute(route: String): Route = path(route / LongNumber / "streams") {
    projectId =>
      get {
        parameter('streamName.as[String].?, 'streamType.as[String].?, 'functionType.as[String].?) {
          (streamNameOpt, streamTypeOpt, functionTypeOpt) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  //complete(OK, getHeader(403, session))
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
                }
                else {
                  getByFilterResponse(projectId, streamNameOpt, streamTypeOpt, functionTypeOpt, session)
                }
            }
        }
      }
  }

  private def getByFilterResponse(projectId: Long, streamNameOpt: Option[String], streamTypeOpt: Option[String], functionTypeOpt: Option[String], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      try {
        (streamNameOpt, streamTypeOpt, functionTypeOpt) match {
          case (Some(name), None, None) =>
            if (StreamUtils.checkYarnAppNameUnique(name, projectId)) {
              riderLogger.info(s"user ${session.userId} check stream name $name doesn't exist success.")
              complete(OK, ResponseJson[String](getHeader(200, session), name))
            } else {
              riderLogger.warn(s"user ${session.userId} check stream name $name already exists success.")
              complete(OK, setFailedResponse(session, s"$name already exists"))
            }
          case (None, Some(streamType), None) =>
            val streams = streamDal.getSimpleStreamInfo(projectId, streamType)
            riderLogger.info(s"user ${session.userId} select streams where streamType is $streamType success.")
            complete(OK, ResponseSeqJson[SimpleStreamInfo](getHeader(200, session), streams))
          case (None, None, Some(functionType)) =>
            val streams = streamDal.getSimpleStreamInfo(projectId, "spark", Some(functionType))
            riderLogger.info(s"user ${session.userId} select streams where project id is $projectId success.")
            complete(OK, ResponseSeqJson[SimpleStreamInfo](getHeader(200, session), streams))
          case (None, Some(_), Some(_)) =>
            riderLogger.error(s"user ${session.userId} request url is not supported.")
            complete(OK, setFailedResponse(session, "Insufficient Permission"))
          case (None, None, None) =>
            val streams = streamDal.getBriefDetail(Some(projectId))
            riderLogger.info(s"user ${session.userId} select streams where project id is $projectId success.")
            complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
          case (_, _, _) =>
            riderLogger.error(s"user ${session.userId} request url is not supported.")
            complete(OK, setFailedResponse(session, "Insufficient Permission"))
        }
      } catch {
        case ex: Exception =>
          riderLogger.error(s"user ${session.userId} refresh all streams failed", ex)
          complete(OK, getHeader(451, ex.getMessage, session))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      //complete(OK, getHeader(403, session))
      complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
      val formatCheck = checkConfigFormat(putStream.startConfig, putStream.launchConfig, putStream.JVMDriverConfig.getOrElse(""), putStream.JVMExecutorConfig.getOrElse(""), putStream.othersConfig.getOrElse(""))
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
                    val log = YarnClientLog.getLogByAppName(stream.name, stream.logPath.getOrElse(""))
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
                if (checkAction(stream.streamType, STOP.toString, stream.status)) {
                  val status = stopStream(stream.id, stream.streamType, stream.sparkAppid, stream.status, session.userId)
                  riderLogger.info(s"user ${
                    session.userId
                  } stop stream $streamId success.")
                  onComplete(streamDal.updateStatusByStop(streamId, status._1, session.userId).mapTo[Int]) {
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

  private def checkAction(streamType: String, action: String, status: String): Boolean = {
    if (getDisableActions(streamType, status).contains(action)) false
    else true
  }

  def renewRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "renew") {
    (id, streamId) =>
      put {
        entity(as[StreamDirective]) {
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
                  renewResponse(id, streamId, streamDirective, session)
                }
            }
        }
      }
  }

  private def renewResponse(projectId: Long, streamId: Long, streamDirective: StreamDirective, session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val streamOpt = Await.result(streamDal.findById(streamId), minTimeOut)
      streamOpt match {
        case Some(stream) =>
          if (checkAction(stream.streamType, RENEW.toString, stream.status)) {
            renewStreamDirective(streamId, streamDirective, session.userId)
            riderLogger.info(s"user ${session.userId} renew stream $streamId success")
            complete(OK, ResponseJson[StartResponse](getHeader(200, session),
              StartResponse(streamId, stream.status, getDisableActions(stream.streamType, stream.status), getHideActions(stream.streamType),
                stream.sparkAppid, stream.startedTime, stream.stoppedTime)))
          } else {
            riderLogger.info(s"user ${session.userId} can't stop stream $streamId now")
            complete(OK, setFailedResponse(session, "renew is forbidden"))
          }
        case None =>
          riderLogger.info(s"user ${session.userId} renew stream $streamId failed caused by stream not found.")
          complete(OK, setFailedResponse(session, "stream not found."))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, setFailedResponse(session, "Insufficient permission"))
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
                  //complete(OK, getHeader(403, session))
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
                }
                else {
                  startResponse(id, streamId, streamDirective, session)
                }
            }
        }
      }
  }

  private def startStreamDirective(streamId: Long, streamDirectiveOpt: Option[StreamDirective], userId: Long) = {
    // delete pre stream zk udf/topic node
    PushDirective.removeTopicDirective(streamId)
    PushDirective.removeUdfDirective(streamId)
    // set new stream directive
    if (streamDirectiveOpt.nonEmpty) {
      val streamDirective = streamDirectiveOpt.get
      genUdfsStartDirective(streamId, streamDirective.udfInfo, userId)
      genTopicsStartDirective(streamId, streamDirective.topicInfo, userId)
    } else {
      genUdfsStartDirective(streamId, Seq(), userId)
      genTopicsStartDirective(streamId, None, userId)
    }
  }

  private def renewStreamDirective(streamId: Long, streamDirective: StreamDirective, userId: Long) = {
    genUdfsRenewDirective(streamId, streamDirective.udfInfo, userId)
    genTopicsRenewDirective(streamId, streamDirective.topicInfo, userId)
  }

  private def startResponse(projectId: Long, streamId: Long, streamDirectiveOpt: Option[StreamDirective], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val streamOpt = Await.result(streamDal.findById(streamId), minTimeOut)
      streamOpt match {
        case Some(stream) =>
          if (checkAction(stream.streamType, START.toString, stream.status)) {
            try {
              val project: Project = Await.result(projectDal.findById(projectId), minTimeOut).head
              val (projectTotalCore, projectTotalMemory) = (project.resCores, project.resMemoryG)
              val (jobUsedCore, jobUsedMemory, _) = jobDal.getProjectJobsUsedResource(projectId)
              val (streamUsedCore, streamUsedMemory, _) = streamDal.getProjectStreamsUsedResource(projectId)
              val (currentNeededCore, currentNeededMemory) =
                StreamType.withName(stream.streamType) match {
                  case StreamType.SPARK =>
                    val currentConfig = JsonUtils.json2caseClass[StartConfig](stream.startConfig)
                    val currentNeededCore = currentConfig.driverCores + currentConfig.executorNums * currentConfig.perExecutorCores
                    val currentNeededMemory = currentConfig.driverMemory + currentConfig.executorNums * currentConfig.perExecutorMemory
                    (currentNeededCore, currentNeededMemory)
                  case StreamType.FLINK =>
                    val currentConfig = JsonUtils.json2caseClass[FlinkResourceConfig](stream.startConfig)
                    val currentNeededCore = currentConfig.taskManagersNumber * currentConfig.perTaskManagerSlots
                    val currentNeededMemory = currentConfig.jobManagerMemoryGB + currentConfig.taskManagersNumber * currentConfig.perTaskManagerMemoryGB
                    (currentNeededCore, currentNeededMemory)
                }

              if ((projectTotalCore - jobUsedCore - streamUsedCore - currentNeededCore) < 0 || (projectTotalMemory - jobUsedMemory - streamUsedMemory - currentNeededMemory) < 0) {
                riderLogger.warn(s"user ${session.userId} start stream ${stream.id} failed, caused by resource is not enough")
                complete(OK, setFailedResponse(session, "resource is not enough"))
              } else {
                if (StreamType.withName(stream.streamType) == StreamType.SPARK)
                  startStreamDirective(streamId, streamDirectiveOpt, session.userId)
                val logPath = getLogPath(stream.name)
                val (result, pid) = startStream(stream, logPath)
                riderLogger.info(s"user ${session.userId} start stream $streamId")
                val status = if (result) StreamStatus.STARTING.toString
                else StreamStatus.FAILED.toString
                onComplete(streamDal.updateStatusByStart(streamId, status, session.userId, logPath, pid).mapTo[Int]) {
                  case Success(_) =>
                    val stream = Await.result(streamDal.findById(streamId), minTimeOut).get
                    val startResponse = StartResponse(streamId,
                      status,
                      getDisableActions(stream.streamType, status),
                      getHideActions(stream.streamType),
                      stream.sparkAppid, stream.startedTime, stream.stoppedTime
                    )
                    complete(OK, ResponseJson[StartResponse](getHeader(200, session), startResponse))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} start stream where project id is $projectId failed", ex)
                    complete(OK, setFailedResponse(session, ex.getMessage))
                }
              }
            } catch {
              case ex: Exception =>
                riderLogger.error(s"user ${session.userId} start stream failed", ex)
                complete(OK, setFailedResponse(session, ex.getMessage))
            }
          } else {
            riderLogger.info(s"user ${session.userId} can't start stream $streamId now")
            complete(OK, setFailedResponse(session, "start is forbidden"))
          }
        case None =>
          riderLogger.info(s"user ${session.userId} start stream $streamId failed caused by stream not found.")
          complete(OK, setFailedResponse(session, "stream not found."))
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
                    if (checkAction(streamDetail.stream.streamType, DELETE.toString, streamDetail.stream.status)) {
                      val flows = Await.result(flowDal.findByFilter(_.streamId === streamId), minTimeOut)
                      if (flows.nonEmpty) {
                        riderLogger.info(s"user ${session.userId} can't delete stream $streamId now, please delete flow ${flows.map(_.id).mkString(",")} first")
                        complete(OK, getHeader(412, s"please delete flow ${flows.map(_.id).mkString(",")} first", session))
                      } else {
                        removeStreamDirective(streamId, session.userId)
                        if (streamDetail.stream.sparkAppid.getOrElse("") != "" && (streamDetail.stream.status == StreamStatus.RUNNING.toString || streamDetail.stream.status == StreamStatus.WAITING.toString || streamDetail.stream.status == StreamStatus.STOPPING.toString)) {
                          val stopSuccess = runYarnKillCommand("yarn application -kill " + streamDetail.stream.sparkAppid.get)
                          riderLogger.info(s"user ${session.userId} stop stream $streamId ${stopSuccess.toString}")
                          if (!stopSuccess) complete(OK, getHeader(400, s"stop stream failed can't delete", session))
                        }
                        Await.result(streamDal.deleteById(streamId), minTimeOut)
                        Await.result(inTopicDal.deleteByFilter(_.streamId === streamId), minTimeOut)
                        Await.result(streamUdfTopicDal.deleteByFilter(_.streamId === streamId), minTimeOut)
                        Await.result(streamUdfDal.deleteByFilter(_.streamId === streamId), minTimeOut)

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
          complete(OK, ResponseJson[RiderJVMConfig](getHeader(200, session), defaultConf))
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
          complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
                Await.result(streamDal.findById(streamId), minTimeOut) match {
                  case Some(_) => getTopicsResponse(id, streamId, session)
                  case None =>
                    riderLogger.info(s"user ${session.userId} get stream $streamId topics failed caused by stream not found.")
                    complete(OK, setFailedResponse(session, "stream not found."))
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


  private def getTopicsResponse(projectId: Long, streamId: Long, session: SessionClass): Route = {
    try {
      val topics = streamDal.getTopicsAllOffsets(streamId)
      riderLogger.info(s"user ${session.userId} get stream $streamId topics success.")
      complete(OK, ResponseJson[GetTopicsResponse](getHeader(200, session), topics))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user ${session.userId} get stream $streamId topics failed", ex)
        complete(OK, setFailedResponse(session, ex.getMessage))
    }
  }

  def getUdfsRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "udfs") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(id)) {
                Await.result(streamDal.findById(streamId), minTimeOut) match {
                  case Some(_) => getUdfsResponse(streamId, session)
                  case None =>
                    riderLogger.info(s"user ${session.userId} get stream $streamId topics failed caused by stream not found.")
                    complete(OK, setFailedResponse(session, "stream not found."))
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

  def getUdfsResponse(streamId: Long, session: SessionClass): Route = {
    val udfs = streamUdfDal.getStreamUdf(streamId)
    riderLogger.info(s"user ${session.userId} get stream $streamId topics success.")
    complete(OK, ResponseSeqJson[StreamUdfResponse](getHeader(200, session), udfs))
  }

  def getResourceByProjectIdRoute(route: String): Route = path(route / LongNumber / "resources") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
                //complete(OK, getHeader(403, session))
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
    val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(kafkaInfo._3.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
    // get kafka earliest/latest offset
    val latestOffset = getLatestOffset(kafkaInfo._2, postTopic.name, inputKafkaKerberos)
    val earliestOffset = getEarliestOffset(kafkaInfo._2, postTopic.name, inputKafkaKerberos)

    val topicResponse = SimpleTopicAllOffsets(postTopic.name, RiderConfig.spark.topicDefaultRate, earliestOffset, earliestOffset, latestOffset)

    riderLogger.info(s"user ${session.userId} get user defined topic offsets success.")
    complete(OK, ResponseJson[SimpleTopicAllOffsets](getHeader(200, session), topicResponse))
  }

  def postTopicsOffsetRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "topics") {
    (id, streamId) =>
      post {
        entity(as[GetTopicsOffsetRequest]) {
          topics => {
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  //complete(OK, getHeader(403, session))
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    try {
                      if (Await.result(streamDal.findById(streamId), minTimeOut).nonEmpty) {
                        postTopicsOffsetResponse(id, streamId, topics, session)
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


  def postTopicsOffsetResponse(id: Long, streamId: Long, topics: GetTopicsOffsetRequest, session: SessionClass): Route = {
    val allTopics = streamDal.getTopicsAllOffsets(streamId)
    val userDefinedTopics = allTopics.userDefinedTopics
    val userDefinedTopicsName = userDefinedTopics.map(_.name)
    val newTopics = topics.userDefinedTopics.filter(!userDefinedTopicsName.contains(_))
    val newTopicsOffset = newTopics.map(topic => {
      val kafkaInfo = streamDal.getKafkaInfo(streamId)
      val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(kafkaInfo._3.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
      val latestOffset = getLatestOffset(kafkaInfo._2, topic, inputKafkaKerberos)
      val earliestOffset = getEarliestOffset(kafkaInfo._2, topic, inputKafkaKerberos)
      val consumedOffset = earliestOffset
      SimpleTopicAllOffsets(topic, RiderConfig.spark.topicDefaultRate, consumedOffset, earliestOffset, latestOffset)
    })
    val response = GetTopicsOffsetResponse(
      allTopics.autoRegisteredTopics.map(topic =>
        SimpleTopicAllOffsets(topic.name, topic.rate, topic.consumedLatestOffset, topic.kafkaEarliestOffset, topic.kafkaLatestOffset)),
      userDefinedTopics.filter(topic => topics.userDefinedTopics.contains(topic.name)).map(topic =>
        SimpleTopicAllOffsets(topic.name, topic.rate, topic.consumedLatestOffset, topic.kafkaEarliestOffset, topic.kafkaLatestOffset))
        ++: newTopicsOffset)
    riderLogger.info(s"user ${session.userId} get stream $streamId topics offset success.")
    complete(OK, ResponseJson[GetTopicsOffsetResponse](getHeader(200, session), response))
  }


  def getDefaultConfig(route: String): Route = path(route / "defaultconfigs") {
    get {
      parameter('streamType.as[String]) {
        (streamType) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "user") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
              else {
                StreamType.withName(streamType) match {
                  case StreamType.SPARK =>
                    val jvm = StreamUtils.getDefaultJvmConf
                    val sparkResource = SparkResourceConfig(RiderConfig.spark.driverCores, RiderConfig.spark.driverMemory, RiderConfig.spark.executorNum,
                      RiderConfig.spark.executorCores, RiderConfig.spark.executorMemory, RiderConfig.spark.batchDurationSec, RiderConfig.spark.parallelismPartition, RiderConfig.spark.maxPartitionFetchMb)
                    val othersConfig = RiderConfig.spark.sparkConfig
                    val defaultConfig = SparkDefaultConfig(jvm.JVMDriverConfig, jvm.JVMExecutorConfig, sparkResource, othersConfig)
                    complete(OK, ResponseJson[SparkDefaultConfig](getHeader(200, session), defaultConfig))
                  case StreamType.FLINK =>
                    val defaultConfig = RiderConfig.defaultFlinkConfig
                    complete(OK, ResponseJson[FlinkDefaultConfig](getHeader(200, session), defaultConfig))
                }
              }
          }
      }
    }
  }

  def getYarnUi(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "yarnUi") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(id)) {
                Await.result(streamDal.findById(streamId), minTimeOut) match {
                  case Some(stream) =>
                    StreamUtils.getAppInfo(stream.startedTime.getOrElse(""), stream.name) match {
                      case Some(appInfo) =>
                        complete(OK, ResponseJson[String](getHeader(200, session), getYarnUri(appInfo.appStatus, appInfo.appId)))
                      case _ =>
                        riderLogger.info(s"user ${session.userId} get stream $streamId on yarnUi failed caused by stream dose not exist on yarn.")
                        complete(OK, setFailedResponse(session, "stream dose not exist on yarn."))
                    }
                  case None =>
                    riderLogger.info(s"user ${session.userId} get stream $streamId on yarnUi failed caused by stream not found.")
                    complete(OK, setFailedResponse(session, "stream not found."))
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

  def getFlowPrioritiesByStreamId(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / "order") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(id)) {
                Await.result(flowDal.findByFilter(flow => flow.active === true && flow.streamId === streamId), minTimeOut) match {
                  case Seq() =>
                    riderLogger.info(s"user ${session.userId} get flow of stream $streamId failed caused by there is no flow existing in the stream.")
                    complete(OK, setFailedResponse(session, "there is no flow existing in the stream."))
                  case Seq(flowSeq@_*) =>
                    val flowPrioritySeq = flowSeq.map(flow => new FlowPriority(flow.id, flow.flowName, flow.priorityId)).seq
                    complete(OK, ResponseJson[FlowPriorities](getHeader(200, session), new FlowPriorities(flowPrioritySeq)))
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

  def updateFlowOrder(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / "order") {
    (id, streamId) =>
      put {
        entity(as[FlowPriorities]) {
          flowPriorities =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                } else {
                  if (session.projectIdList.contains(id)) {
                    flowPriorities.flowPrioritySeq.foreach(flowPriority =>
                      flowDal.updatePriority(flowPriority.id, flowPriority.priorityId))
                    complete(OK, setSuccessResponse(session))
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
