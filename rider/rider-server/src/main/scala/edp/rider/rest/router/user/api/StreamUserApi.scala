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
import edp.rider.common.Action._
import edp.rider.common.{RiderConfig, RiderLogger, StreamStatus}
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import edp.rider.rest.util.ProjectUtils._
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.rest.util.StreamUtils._
import edp.rider.rest.util.UdfUtils._
import edp.rider.service.util.CacheMap
import edp.rider.spark.{SparkJobClientLog, SubmitSparkJob}
import edp.rider.spark.SubmitSparkJob.runShellCommand
import slick.jdbc.MySQLProfile.api._
import edp.rider.kafka.KafkaUtils._

import scala.concurrent.Await
import scala.util.{Failure, Success}


class StreamUserApi(streamDal: StreamDal, projectDal: ProjectDal, streamUdfDal: RelStreamUdfDal, inTopicDal: StreamInTopicDal, flowDal: FlowDal) extends BaseUserApiImpl(streamDal) with RiderLogger with JsonSerializer {

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
          onComplete(streamDal.insert(insertStream).mapTo[Stream]) {
            case Success(stream) =>
              val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(stream.id))
              complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail.head))
            case Failure(ex) =>
              riderLogger.error(s"user ${
                session.userId
              } insert stream failed", ex)
              complete(OK, getHeader(451, ex.getMessage, session))
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
            val projectName = Await.result(projectDal.findById(projectId), minTimeOut).get.name
            val realName = genStreamNameByProjectName(projectName, name)
            onComplete(streamDal.checkStreamNameUnique(realName).mapTo[Seq[Stream]]) {
              case Success(streams) =>
                if (streams.isEmpty) {
                  riderLogger.info(s"user ${session.userId} check stream name $name doesn't exist success.")
                  complete(OK, ResponseJson[String](getHeader(200, session), name))
                }
                else {
                  riderLogger.warn(s"user ${session.userId} check stream name $name already exists success.")
                  complete(OK, getHeader(409, s"$name already exists", session))
                }
              case Failure(ex) =>
                riderLogger.error(s"user ${session.userId} check stream name $name does exist failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }
          case (None, Some(streamType)) =>
            val streams = streamDal.getStreamDetail(Some(projectId)).filter(_.stream.streamType == streamType).sortBy(_.stream.name)
            riderLogger.info(s"user ${session.userId} select streams where streamType is $streamType success.")
            complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
          case (None, None) =>
            val streams = streamDal.getStreamDetail(Some(projectId))
            riderLogger.info(s"user ${session.userId} select streams where project id is $projectId success.")
            complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
          case (_, _) =>
            riderLogger.error(s"user ${session.userId} request url is not supported.")
            complete(OK, getHeader(501, session))
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
                val stream = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head
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
            val stream = streamDal.getStreamDetail(Some(projectId), Some(putStream.id)).head
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

  def getDefaultSparkConfList(route: String): Route = path(route / "streams" / "default" / "config") {
    authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
      session =>
        if (session.roleType != "user") {
          riderLogger.warn(s"${
            session.userId
          } has no permission to access it.")
          complete(OK, getHeader(403, session))
        }
        else {
          val defaultConf = streamDal.getConfList
          complete(OK, ResponseJson[String](getHeader(200, session), defaultConf))
        }
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
                    val log = SparkJobClientLog.getLogByAppName(stream.name)
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
                  onComplete(streamDal.updateByStatus(streamId, status, session.userId).mapTo[Int]) {
                    case Success(_) =>
                      val streamDetail = streamDal.getStreamDetail(Some(id), Some(streamId)).head
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
    if (getDisableActions(status).contains(action)) false
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
                  renewResponse(id, streamId, streamDirective, session)
                }
            }
        }
      }
  }

  private def renewResponse(projectId: Long, streamId: Long, streamDirectiveOpt: Option[StreamDirective], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val stream = Await.result(streamDal.findById(streamId), minTimeOut).get
      if (checkAction(RENEW.toString, stream.status)) {
        renewStreamDirective(streamId, streamDirectiveOpt, session.userId)
        riderLogger.info(s"user ${session.userId} renew stream $streamId success")
        val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head
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
    if (streamDirectiveOpt.nonEmpty) {
      val streamDirective = streamDirectiveOpt.get
      if (streamDirective.udfInfo.nonEmpty) {
        val deleteUdfIds = streamUdfDal.getDeleteUdfIds(streamId, streamDirective.udfInfo.get)
        Await.result(streamUdfDal.deleteByFilter(udf => udf.streamId === streamId && udf.udfId.inSet(deleteUdfIds)), minTimeOut)
        val insertUdfs = streamDirective.udfInfo.get.map(
          id => RelStreamUdf(0, streamId, id, currentSec, userId, currentSec, userId)
        )
        Await.result(streamUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
        removeUdfDirective(streamId, userId = userId)
        sendUdfDirective(streamId, streamUdfDal.getStreamUdf(Seq(streamId)), userId)
      } else {
        Await.result(streamUdfDal.deleteByFilter(_.streamId === streamId), minTimeOut)
        removeUdfDirective(streamId, userId = userId)
      }
      if (streamDirective.topicInfo.nonEmpty) {
        streamDirective.topicInfo.get.foreach(
          topic => Await.result(inTopicDal.updateOffset(streamId, topic.id, topic.partitionOffsets, topic.rate, userId), minTimeOut)
        )
        removeAndSendDirective(streamId, inTopicDal.getStreamTopic(Seq(streamId)), userId)
      }
    } else {
      Await.result(streamUdfDal.deleteByFilter(_.streamId === streamId), minTimeOut)
      removeUdfDirective(streamId, userId = userId)
    }
  }

  private def renewStreamDirective(streamId: Long, streamDirectiveOpt: Option[StreamDirective], userId: Long) = {
    if (streamDirectiveOpt.nonEmpty) {
      val streamDirective = streamDirectiveOpt.get
      if (streamDirective.udfInfo.nonEmpty) {
        val insertUdfs = streamDirective.udfInfo.get.map(
          id => RelStreamUdf(0, streamId, id, currentSec, userId, currentSec, userId)
        )
        Await.result(streamUdfDal.insertOrUpdate(insertUdfs).mapTo[Int], minTimeOut)
        sendUdfDirective(streamId, streamUdfDal.getStreamUdf(Seq(streamId)), userId)
      }
      if (streamDirective.topicInfo.nonEmpty) {
        streamDirective.topicInfo.get.foreach(
          topic => Await.result(inTopicDal.updateOffset(streamId, topic.id, topic.partitionOffsets, topic.rate, userId), minTimeOut)
        )
        sendTopicDirective(streamId, inTopicDal.getStreamTopic(Seq(streamId)), userId)
      }
    }
  }

  private def startResponse(projectId: Long, streamId: Long, streamDirectiveOpt: Option[StreamDirective], session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head
      val stream = streamDetail.stream
      if (checkAction(START.toString, stream.status)) {
        val resource = Await.result(streamDal.getResource(projectId), minTimeOut)
        if (!isResourceEnough(stream.startConfig, resource)) {
          riderLogger.warn(s"user ${session.userId} start stream $streamId failed, caused by resource is not enough")
          complete(OK, getHeader(507, session))
        } else {
          startStreamDirective(streamId, streamDirectiveOpt, session.userId)
          runShellCommand(s"rm -rf ${SubmitSparkJob.getLogPath(stream.name)}")
          startStream(streamDetail)
          riderLogger.info(s"user ${session.userId} start stream $streamId success")
          onComplete(streamDal.updateByStatus(streamId, StreamStatus.STARTING.toString, session.userId).mapTo[Int]) {
            case Success(_) =>
              val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head
              complete(OK, ResponseJson[StreamDetail](getHeader(200, session), streamDetail))
            case Failure(ex) =>
              riderLogger.error(s"user ${session.userId} start stream where project id is $projectId failed", ex)
              complete(OK, getHeader(451, session))
          }
        }
      } else {
        riderLogger.info(s"user ${session.userId} can't stop stream $streamId now")
        complete(OK, getHeader(406, s"start is forbidden", session))
      }
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
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
                val streamDetailOpt = streamDal.getStreamDetail(Some(id), Some(streamId)).headOption
                streamDetailOpt match {
                  case Some(streamDetail) =>
                    val flows = Await.result(flowDal.findByFilter(_.streamId === streamId), minTimeOut)
                    if (flows.nonEmpty) {
                      riderLogger.info(s"user ${session.userId} can't delete stream $streamId now, please delete flow ${flows.map(_.id).mkString(",")} first")
                      complete(OK, getHeader(412, s"please delete flow ${flows.map(_.id).mkString(",")} first", session))
                    } else {
                      removeStreamDirective(streamId, streamDetail.topicInfo, session.userId)
                      if (streamDetail.stream.sparkAppid.getOrElse("") != "") {
                        runShellCommand("yarn application -kill " + streamDetail.stream.sparkAppid.get)
                        riderLogger.info(s"user ${session.userId} stop stream $streamId success")
                      }
                      Await.result(streamDal.deleteById(streamId), minTimeOut)
                      Await.result(inTopicDal.deleteByFilter(_.streamId === streamId), minTimeOut)
                      CacheMap.streamCacheMapRefresh
                      riderLogger.info(s"user ${session.userId} delete stream $streamId success")
                      complete(OK, getHeader(200, session))
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

  def getConfList(route: String): Route = path(route / "streams" / "default" / "config") {
    authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
      session =>
        if (session.roleType != "user") {
          riderLogger.warn(s"${
            session.userId
          } has no permission to access it.")
          complete(OK, getHeader(403, session))
        }
        else {
          val defaultConf = streamDal.getConfList
          complete(OK, ResponseJson[String](getHeader(200, session), defaultConf))
        }
    }

  }

  def getLatestOffset(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "topics" / "offsets" / "latest") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              getLatestOffsetResponse(id, streamId, session)
            }
        }

      }
  }

  private def getLatestOffsetResponse(projectId: Long, streamId: Long, session: SessionClass): Route = {
    if (session.projectIdList.contains(projectId)) {
      val streamDetail = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head
      val offsets = streamDetail.topicInfo.map(topic =>
        TopicLatestOffset(topic.id, topic.name, getKafkaLatestOffset(streamDetail.kafkaInfo.connUrl, topic.name)))
      riderLogger.info(s"user ${session.userId} get stream $streamId topics latest offset success")
      complete(OK, ResponseSeqJson[TopicLatestOffset](getHeader(200, session), offsets))
    } else {
      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
      complete(OK, getHeader(403, session))
    }
  }

  def getResourceByProjectIdRoute(route: String): Route = path(route / LongNumber / "resources") {
    id =>
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
                onComplete(streamDal.getResource(id).mapTo[Resource]) {
                  case Success(resources) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } select all resources success where project id is $id.")
                    complete(OK, ResponseJson[Resource](getHeader(200, session), resources))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } select all resources failed where project id is $id", ex)
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
}
