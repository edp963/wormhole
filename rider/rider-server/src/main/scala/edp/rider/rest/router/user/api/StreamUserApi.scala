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

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import edp.rider.common.RiderLogger
import edp.rider.kafka.KafkaUtils._
import edp.rider.rest.persistence.base.BaseDal
import edp.rider.rest.persistence.dal.{FlowDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.StreamUtils._
import edp.rider.service.util.CacheMap
import edp.rider.spark.SparkJobClientLog
import edp.rider.spark.SubmitSparkJob.{generateStreamStartSh, getConfig, riderLogger, runShellCommand}
import edp.wormhole.common.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


class StreamUserApi(streamDal: StreamDal, flowDal: FlowDal, inTopicDal: BaseDal[StreamInTopicTable, StreamInTopic]) extends BaseUserApiImpl(streamDal) with RiderLogger {

  override def getByAllRoute(route: String): Route = path(route / LongNumber / "streams") {
    projectId =>
      get {
        parameter('streamName.as[String].?, 'streamType.as[String].?) {
          (streamName, streamTypeOpt) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    try {
                      (streamName, streamTypeOpt) match {
                        case (Some(name), None) =>
                          val projectName = Await.result(streamDal.getProjectNameById(projectId), minTimeOut).name
                          val realName = streamDal.generateStreamNameByProject(projectName, name)
                          onComplete(streamDal.checkStreamNameUnique(realName).mapTo[Seq[Stream]]) {
                            case Success(streams) =>
                              if (streams.isEmpty) {
                                riderLogger.info(s"user ${session.userId} check stream name $name doesn't exist success.")
                                complete(OK, ResponseJson[String](getHeader(200, session), name))
                              }
                              else {
                                riderLogger.warn(s"user ${session.userId} check stream name $name already exists success.")
                                complete(OK, getHeader(409, s"$streamName  already exists", session))
                              }
                            case Failure(ex) =>
                              riderLogger.error(s"user ${session.userId} check stream name $name does exist failed", ex)
                              complete(OK, getHeader(451, ex.getMessage, session))
                          }
                        case (None, Some(streamType)) =>
                          onComplete(streamDal.getStreamKafkaTopic(projectId, streamTypeOpt = Some(streamType)).mapTo[Seq[StreamKafkaTopic]]) {
                            case Success(streams) =>
                              riderLogger.info(s"user ${session.userId} select streams where streamType is $streamType success.")
                              complete(OK, ResponseSeqJson[StreamKafkaTopic](getHeader(200, session), streams))
                            case Failure(ex) =>
                              riderLogger.error(s"user ${session.userId} select streams where streamType is $streamType failed", ex)
                              complete(OK, getHeader(451, ex.getMessage, session))
                          }
                        case (None, None) => returnStreamRes(projectId, None, session)
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
            }
        }
      }

  }

  //get stream by project id and stream id.
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
                returnStreamRes(projectId, Some(streamId), session)
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }


  //search stream table,get return stream by project id and stream id.
  def returnStreamRes(projectId: Long, streamId: Option[Long], session: SessionClass) = {
    onComplete(streamDal.getStreamsByProjectId(Some(projectId), streamId).mapTo[Seq[StreamSeqTopic]]) {
      case Success(streams) =>
        riderLogger.info(s"user ${session.userId} select streams where project id is $projectId and stream id is $streamId success.")
        val allStreams: Seq[(Stream, StreamSeqTopic)] = streamDal.getUpdateStream(streams)
        val seqStreams = allStreams.map(stream => stream._1)
        val realReturns = allStreams.map(stream => stream._2)
        val realRes: Seq[StreamSeqTopicActions] = realReturns.map(returnStream => streamDal.getReturnRes(returnStream))
        riderLogger.info(s"user ${session.userId} update streams after refresh the yarn/spark rest api or log success.")
        //        complete(OK, ResponseSeqJson[StreamSeqTopicActions](getHeader(200, session), realRes))
        onComplete(streamDal.updateStreamsTable(seqStreams)) {
          case Success(success) =>
            riderLogger.info(s"user ${session.userId} update streams after refresh the yarn/spark rest api or log success.")
            complete(OK, ResponseSeqJson[StreamSeqTopicActions](getHeader(200, session), realRes.sortBy(_.stream.id)))
          case Failure(ex) =>
            riderLogger.error(s"user ${session.userId} update streams after refresh the yarn/spark rest api or log failed", ex)
            complete(OK, getHeader(451, ex.getMessage, session))
        }
      case Failure(ex) =>
        riderLogger.error(s"user ${session.userId} select streams where project id is $projectId and stream id is $streamId failed", ex)
        complete(OK, getHeader(451, ex.getMessage, session))
    }
  }

  //modify stream
  def putRoute(route: String): Route = path(route / LongNumber / "streams") {
    id =>
      put {
        entity(as[StreamTopic]) {
          streamTopic =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session => {
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    val formatCheck = checkConfigFormat(streamTopic.startConfig, streamTopic.launchConfig, streamTopic.sparkConfig.getOrElse(""))
                    if (formatCheck._1) {
                      val startTime = if (streamTopic.startedTime.getOrElse("") == "") null else streamTopic.startedTime
                      val stopTime = if (streamTopic.stoppedTime.getOrElse("") == "") null else streamTopic.stoppedTime
                      val stream = Stream(streamTopic.id, streamTopic.name, streamTopic.desc, streamTopic.projectId, streamTopic.instanceId, streamTopic.streamType, streamTopic.sparkConfig, streamTopic.startConfig, streamTopic.launchConfig, streamTopic.sparkAppid, streamTopic.logPath, streamTopic.status, startTime, stopTime, streamTopic.active, streamTopic.createTime, streamTopic.createBy, currentSec, session.userId)
                      val streamId = streamTopic.id
                      onComplete(streamDal.getStreamById(streamId).mapTo[Option[StreamWithBrokers]]) {
                        case Success(opStreamWithBrokers) =>
                          riderLogger.info(s"user ${session.userId} select stream where id is $streamId success.")
                          opStreamWithBrokers match {
                            case Some(streamWithBrokers) =>
                              val brokers = streamWithBrokers.brokers
                              onComplete(streamDal.updateStreamTable(stream).mapTo[Int]) {
                                case Success(result) =>
                                  riderLogger.info(s"user ${session.userId} update stream success.")
                                  onComplete(streamDal.getTopicByInstanceId(streamTopic.instanceId).mapTo[Seq[TopicSimple]]) {
                                    case Success(simpleTopics) =>
                                      riderLogger.info(s"user ${session.userId} select streamInTopics where instance id is ${streamTopic.instanceId} success.")
                                      val topicsInInstance = simpleTopics.map(topic => (topic.id, topic.name)).toMap
                                      onComplete(streamDal.getStreamInTopicByStreamId(streamId).mapTo[Seq[StreamInTopicName]]) {
                                        case Success(streamInTopics) =>
                                          riderLogger.info(s"user ${session.userId} select streamInTopics where stream id is $streamId success.")
                                          val topicsInTable: Map[Long, String] = streamInTopics.map(topic => (topic.nsDatabaseId, topic.nsDatabase)).toMap
                                          var deleteTopics = ArrayBuffer[Long]()
                                          var insertTopics = ArrayBuffer[(Long, String)]()
                                          if (streamTopic.topic.length == 0) {
                                            topicsInTable.keySet.foreach(topic => deleteTopics += topic)
                                          }
                                          else {
                                            val inputTopics = streamTopic.topic.split(",").map(topic => topic.toLong)
                                            inputTopics.foreach(topic =>
                                              if (!topicsInTable.contains(topic)) {
                                                insertTopics += (topic -> topicsInInstance(topic))
                                              }
                                            )
                                            topicsInTable.keySet.foreach(topic =>
                                              if (!inputTopics.contains(topic)) deleteTopics += topic
                                            )
                                          }
                                          onComplete(streamDal.deleteStreamInTopicByStreamId(deleteTopics).mapTo[Int]) {
                                            case Success(num) =>
                                              riderLogger.info(s"user ${session.userId} delete streamInTopics where stream id is $streamId success.")
                                              if (streamTopic.topic.length != 0) {
                                                val listInsertTopic = insertTopics.map(topic => {
                                                  val topicOffset = getKafkaLatestOffset(brokers, topic._2)
                                                  StreamInTopic(0, stream.id, stream.instanceId, topic._1, topicOffset, 100, active = true, currentSec, session.userId, currentSec, session.userId)
                                                })
                                                onComplete(streamDal.insertIntoStreamInTopic(listInsertTopic).mapTo[Seq[Long]]) {
                                                  case Success(seqLong) =>
                                                    riderLogger.info(s"user ${session.userId} insert streamInTopics where stream id is $streamId success.")
                                                    returnStreamRes(stream.projectId, Some(stream.id), session)
                                                  case Failure(ex) =>
                                                    riderLogger.error(s"user ${session.userId} insert streamInTopics where stream id is $streamId failed", ex)
                                                    if (ex.getMessage.contains("Duplicate entry"))
                                                      complete(OK, getHeader(409, s"duplicate stream id and topic id insert", session))
                                                    else
                                                      complete(OK, getHeader(451, ex.getMessage, session))
                                                }
                                              }
                                              else {
                                                returnStreamRes(stream.projectId, Some(stream.id), session)
                                              }
                                            case Failure(ex) =>
                                              riderLogger.error(s"user ${session.userId} delete streamInTopics where stream id is $streamId failed", ex)
                                              complete(OK, getHeader(451, ex.getMessage, session))
                                          }
                                        case Failure(ex) =>
                                          riderLogger.error(s"user ${session.userId} select streamInTopics where stream id is $streamId failed", ex)
                                          complete(OK, getHeader(451, ex.getMessage, session))
                                      }
                                    case Failure(ex) =>
                                      riderLogger.error(s"user ${session.userId} select streamInTopics where instance id is ${streamTopic.instanceId} failed", ex)
                                      complete(OK, getHeader(451, ex.getMessage, session))
                                  }
                                case Failure(ex) =>
                                  riderLogger.error(s"user ${session.userId} update stream failed", ex)
                                  complete(OK, getHeader(451, ex.getMessage, session))
                              }
                            case None =>
                              riderLogger.info(s"user ${session.userId} select stream where id is $streamId success.")
                              complete(OK, ResponseJson[String](getHeader(200, session), ""))
                          }
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} select stream where id is $streamId failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.error(s"user ${session.userId} update stream failed caused by ${formatCheck._2}")
                      complete(OK, getHeader(400, formatCheck._2, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                    complete(OK, getHeader(403, session))
                  }
                }
              }
            }
        }
      }
  }

  def putStreamInTopicRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "intopics") {
    (id, streamId) =>
      put {
        entity(as[Seq[StreamInTopic]]) {
          streamInTopic =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session => {
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    streamInTopic.map(intopic =>
                      StreamInTopic(intopic.id, intopic.streamId, intopic.nsInstanceId, intopic.nsDatabaseId, intopic.partitionOffsets, intopic.rate,
                        intopic.active, intopic.createTime, intopic.createBy, currentSec, session.userId))
                    onComplete(streamDal.updateStreamInTopicTable(streamInTopic).mapTo[Unit]) {
                      case Success(result) =>
                        riderLogger.info(s"user ${session.userId} update streamInTopics success.")
                        onComplete(streamDal.getTopicDetailByStreamId(streamId)) {
                          case Success(topics) => complete(OK, ResponseSeqJson[TopicDetail](getHeader(200, session), topics))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} update streamInTopics failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} update streamInTopics failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                    complete(OK, getHeader(403, session))
                  }
                }
              }
            }
        }
      }
  }


  //insert stream by simple stream entity.
  def postStreamRoute(route: String): Route = path(route / LongNumber / "streams") {
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
                  if (session.projectIdList.contains(id)) {
                    try {
                      val formatCheck = checkConfigFormat(simple.startConfig, simple.launchConfig, simple.sparkConfig.getOrElse(""))
                      if (formatCheck._1) {
                        val projectName = Await.result(streamDal.getProjectNameById(simple.projectId), minTimeOut).name
                        val streamName = streamDal.generateStreamNameByProject(projectName, simple.name)
                        val insertStream = Stream(0, streamName, simple.desc, simple.projectId,
                          simple.instanceId,
                          simple.streamType,
                          simple.sparkConfig,
                          simple.startConfig,
                          simple.launchConfig,
                          Some(""),
                          Some(""),
                          "new",
                          null,
                          null,
                          active = true, currentSec, session.userId, currentSec, session.userId)
                        onComplete(streamDal.insertStreamReturnRes(Seq(insertStream)).map(_.head).mapTo[Stream]) {
                          case Success(base) =>
                            riderLogger.info(s"user ${
                              session.userId
                            } inserted stream $base success.")
                            if (simple.topics == "" || simple.topics == null) {
                              CacheMap.streamCacheMapRefresh
                              returnStreamRes(simple.projectId, Some(base.id), session)
                            }
                            else {
                              val streamId = base.id
                              onComplete(streamDal.getStreamById(streamId).mapTo[Option[StreamWithBrokers]]) {
                                case Success(opStreamWithBrokers) =>
                                  riderLogger.info(s"user ${
                                    session.userId
                                  } select stream where id is $streamId success.")
                                  opStreamWithBrokers match {
                                    case Some(streamWithBrokers) =>
                                      val brokers = streamWithBrokers.brokers
                                      //                    val topics = simple.topics.split(",").map(topic => topic.toLong)
                                      onComplete(streamDal.getTopicByInstanceId(simple.instanceId).mapTo[Seq[TopicSimple]]) {
                                        case Success(simpleTopics) =>
                                          riderLogger.info(s"user ${
                                            session.userId
                                          } select streamInTopics where instance id is ${
                                            simple.instanceId
                                          } success.")
                                          val topicsInTable = simpleTopics.map(topic => (topic.id, topic.name)).toMap
                                          var insertTopics = ArrayBuffer[(Long, String)]()
                                          simple.topics.split(",").foreach(topic =>
                                            if (topicsInTable.keySet.contains(topic.toLong)) insertTopics += (topic.toLong -> topicsInTable(topic.toLong))
                                          )
                                          val listInsertTopic = insertTopics.map(topic => {
                                            val topicOffset = getKafkaLatestOffset(brokers, topic._2)
                                            val defaultRate = 200
                                            StreamInTopic(0, base.id, base.instanceId, topic._1, topicOffset, defaultRate, active = true, currentSec, session.userId, currentSec, session.userId)
                                          })
                                          onComplete(streamDal.insertIntoStreamInTopic(listInsertTopic).mapTo[Seq[Long]]) {
                                            case Success(seqLong) =>
                                              riderLogger.info(s"user ${
                                                session.userId
                                              } insert streamInTopics where stream id is $streamId success.")
                                              CacheMap.flowCacheMapRefresh
                                              returnStreamRes(simple.projectId, Some(base.id), session)
                                            case Failure(ex) =>
                                              riderLogger.error(s"user ${
                                                session.userId
                                              } insert streamInTopics where stream id is $streamId failed", ex)
                                              if (ex.getMessage.contains("Duplicate entry"))
                                                complete(OK, getHeader(409, "duplicate stream id and topic id", session))
                                              else
                                                complete(OK, getHeader(451, ex.getMessage, session))
                                          }
                                        case Failure(ex) =>
                                          riderLogger.error(s"user ${
                                            session.userId
                                          } select streamInTopics where instance id is ${
                                            simple.instanceId
                                          } failed", ex)
                                          complete(OK, getHeader(451, ex.getMessage, session))
                                      }
                                    case None =>
                                      riderLogger.info(s"user ${
                                        session.userId
                                      } select stream where id is $streamId success.")
                                      complete(OK, ResponseJson[String](getHeader(200, session), ""))
                                  }
                                case Failure(ex) =>
                                  riderLogger.error(s"user ${
                                    session.userId
                                  } select stream where id is $streamId failed", ex)
                                  complete(InternalServerError, getHeader(500, ex.getMessage, session))
                              }
                            }
                          case Failure(ex) =>
                            riderLogger.error(s"user ${
                              session.userId
                            } insert stream failed", ex)
                            if (ex.getMessage.contains("Duplicate entry"))
                              complete(OK, getHeader(409, s"${
                                insertStream.name
                              } already exists", session))
                            else
                              complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      } else {
                        riderLogger.error(s"user ${session.userId} insert stream failed caused by ${formatCheck._2}")
                        complete(OK, getHeader(400, formatCheck._2, session))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"user ${
                          session.userId
                        } insert stream failed", ex)
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
  }

  def getKafkasByProjectId(route: String): Route = path(route / LongNumber / "instances" / "kafka") {
    projectId =>
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
              if (session.projectIdList.contains(projectId)) {
                onComplete(streamDal.getKafkaByProjectId(projectId).mapTo[Seq[Kafka]]) {
                  case Success(kafka) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } select instances where project id is $projectId and nsSys is kafka success.")
                    complete(OK, ResponseSeqJson[Kafka](getHeader(200, session), kafka))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } select instances where project id is $projectId and nsSys is kafka failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
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

  def getStreamStopped(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "stop") {
    (projectId, streamId) =>
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
              if (session.projectIdList.contains(projectId)) {
                onComplete(streamDal.getStreamById(streamId).mapTo[Option[StreamWithBrokers]]) {
                  case Success(opStream) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } select stream log where stream id is $streamId success.")
                    opStream match {
                      case Some(streamWithBrokers) =>
                        val stream = streamWithBrokers.stream
                        val appId = stream.sparkAppid.get
                        if (appId == "" || appId == null)
                          complete(OK, ResponseJson[String](getHeader(200, session), "No application to stop"))
                        val updateStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.sparkAppid, stream.logPath, "stopping", stream.startedTime, stream.stoppedTime, stream.active, stream.createTime, stream.createBy, currentSec, stream.updateBy)
                        val cmdStr = "yarn application -kill " + appId
                        riderLogger.info(s"stop stream command: $cmdStr")
                        runShellCommand(cmdStr)
                        riderLogger.info(s"user ${
                          session.userId
                        } stop stream ${stream.id} success.")
                        onComplete(streamDal.updateStreamTable(updateStream).mapTo[Int]) {
                          case Success(num) =>
                            riderLogger.info(s"user ${
                              session.userId
                            } update stream after stop action success.")
                            returnStreamRes(projectId, Some(streamId), session)
                          case Failure(ex) =>
                            riderLogger.error(s"user ${
                              session.userId
                            } updated stream after stop action failed")
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case None =>
                        riderLogger.info(s"user ${
                          session.userId
                        } select stream log where stream id is $streamId success.")
                        complete(OK, ResponseJson[String](getHeader(200, session), ""))
                    }
                  //                  complete(OK, ResponseSeqJson[Kafka](getHeader(200, session), kafka))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } select stream log where stream id is $streamId failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }

  def getStreamRenew(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "renew") {
    (projectId, streamId) =>
      put {
        entity(as[Seq[SimpleTopic]]) {
          simpleTopics =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    onComplete(streamDal.getStreamsByProjectId(Some(projectId), Some(streamId)).mapTo[Seq[StreamSeqTopic]]) {
                      case Success(streams) =>
                        riderLogger.info(s"user ${
                          session.userId
                        } select stream where stream id is $streamId success.")
                        val allStreams: Seq[(Stream, StreamSeqTopic)] = streamDal.getUpdateStream(streams)
                        val realReturns = allStreams.map(stream => stream._2)
                        realReturns.headOption match {
                          case Some(streamSeqTopic) =>
                            removeStreamDirective(streamId, session.userId)
                            sendTopicDirective(streamId, simpleTopics, session.userId)
                            riderLogger.info(s"user ${session.userId} renew stream $streamId.")
                            val stream = streamSeqTopic.stream
                            val startTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime
                            val stopTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime
                            val returnStartTime = if (stream.startedTime.getOrElse("") == "") Some("") else stream.startedTime
                            val returnStopTime = if (stream.stoppedTime.getOrElse("") == "") Some("") else stream.stoppedTime
                            val updateStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.sparkAppid, stream.logPath, stream.status, startTime, stopTime, stream.active, stream.createTime, stream.createBy, currentSec, stream.updateBy)
                            val returnStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.sparkAppid, stream.logPath, stream.status, returnStartTime, returnStopTime, stream.active, stream.createTime, stream.createBy, currentSec, stream.updateBy)
                            val streamReturn = StreamSeqTopicActions(returnStream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "start,renew")
                            onComplete(streamDal.updateStreamTable(updateStream).mapTo[Int]) {
                              case Success(num) =>
                                riderLogger.info(s"user ${session.userId} update stream after renew action success.")
                                complete(OK, ResponseJson[StreamSeqTopicActions](getHeader(200, session), streamReturn))
                              case Failure(ex) =>
                                riderLogger.error(s"user ${session.userId} update stream after renew action failed", ex)
                                complete(OK, getHeader(451, ex.getMessage, session))
                            }
                          case None =>
                            riderLogger.info(s"user ${session.userId} select stream where stream id is $streamId success.")
                            complete(OK, ResponseJson[String](getHeader(200, session), ""))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select stream where stream id is $streamId failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, getHeader(403, session))
                  }
                }
            }
        }
      }
  }


  def getStreamStarted(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "start") {
    (projectId, streamId) =>
      put {
        entity(as[Seq[SimpleTopic]]) {
          simpleTopics =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    onComplete(streamDal.getStreamsByProjectId(Some(projectId), Some(streamId)).mapTo[Seq[StreamSeqTopic]]) {
                      case Success(streams) =>
                        riderLogger.info(s"user ${session.userId} select stream where stream id is $streamId success.")
                        val allStreams: Seq[(Stream, StreamSeqTopic)] = streamDal.getUpdateStream(streams, "start")
                        val realReturns = allStreams.map(stream => stream._2)
                        realReturns.headOption match {
                          case Some(streamSeqTopic) =>
                            onComplete(streamDal.getResource(projectId).mapTo[Resource]) {
                              case Success(resource) =>
                                riderLogger.info(s"user ${session.userId} select project resources where project id is $projectId success.")
                                val startConfig = json2caseClass[StartConfig](streamSeqTopic.stream.startConfig)
                                val remainCores = resource.remainCores
                                val remainMemory = resource.remainMemory
                                val startCores = startConfig.driverCores + startConfig.executorNums * startConfig.perExecutorCores
                                val startMemory = startConfig.driverMemory + startConfig.executorNums * startConfig.perExecutorMemory
                                if (remainCores < startCores || remainMemory < startMemory) {
                                  riderLogger.warn(s"user ${session.userId}")
                                  complete(OK, getHeader(507, "start operation is refused because resource is not enough.", session))
                                }
                                else {
                                  removeStreamDirective(streamId, session.userId)
                                  sendTopicDirective(streamId, simpleTopics, session.userId)
                                  val stream = streamSeqTopic.stream
                                  val streamName = streamSeqTopic.stream.name
                                  val brokers = streamSeqTopic.kafkaConnection
                                  val sparkConfig = streamSeqTopic.stream.sparkConfig.get //--conf relative
                                  //mem cores
                                  val launchConfig = json2caseClass[LaunchConfig](streamSeqTopic.stream.launchConfig)
                                  val streamType = streamSeqTopic.stream.streamType //config relative
                                  val args = getConfig(streamId, streamName, brokers, launchConfig)
                                  val commandSh = generateStreamStartSh(args, streamName, startConfig, sparkConfig, streamType)
                                  riderLogger.info(s"start stream command: $commandSh")
                                  runShellCommand(commandSh)
                                  val startTime = if (stream.startedTime.getOrElse("") == "") null else stream.startedTime
                                  val stopTime = if (stream.stoppedTime.getOrElse("") == "") null else stream.stoppedTime
                                  val returnStartTime = if (stream.startedTime.getOrElse("") == "") Some("") else stream.startedTime
                                  val returnStopTime = if (stream.stoppedTime.getOrElse("") == "") Some("") else stream.stoppedTime
                                  val updateStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.sparkAppid, stream.logPath, "starting", startTime, stopTime, stream.active, stream.createTime, stream.createBy, currentSec, stream.updateBy)
                                  val returnStream = Stream(stream.id, stream.name, stream.desc, stream.projectId, stream.instanceId, stream.streamType, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.sparkAppid, stream.logPath, "starting", returnStartTime, returnStopTime, stream.active, stream.createTime, stream.createBy, currentSec, stream.updateBy)
                                  val streamReturn = StreamSeqTopicActions(returnStream, streamSeqTopic.kafkaName, streamSeqTopic.kafkaConnection, streamSeqTopic.topicInfo, "start,stop,renew")
                                  onComplete(streamDal.updateStreamTable(updateStream).mapTo[Int]) {
                                    case Success(num) =>
                                      //                                      riderLogger.info(s"user ${session.userId} start stream $streamId.")
                                      riderLogger.info(s"user ${session.userId} update stream after start action success.")
                                      complete(OK, ResponseJson[StreamSeqTopicActions](getHeader(200, session), streamReturn))
                                    case Failure(ex) =>
                                      riderLogger.error(s"user ${session.userId} update stream after start action failed", ex)
                                      complete(OK, getHeader(451, ex.getMessage, session))
                                  }
                                }
                              case Failure(ex) =>
                                riderLogger.error(s"user ${session.userId} select project resources where project id is $projectId failed", ex)
                                complete(OK, getHeader(451, ex.getMessage, session))
                            }
                          case None =>
                            riderLogger.info(s"user ${session.userId} select stream where stream id is $streamId success.")
                            complete(OK, ResponseJson[String](getHeader(200, session), ""))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select stream where stream id is $streamId failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, getHeader(403, session))
                  }
                }
            }
        }
      }
  }

  def deleteStream(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "delete") {
    (projectId, streamId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            if (session.projectIdList.contains(projectId)) {
              try {
                val streamOpt = Await.result(streamDal.findById(streamId), minTimeOut)
                streamOpt match {
                  case Some(stream) =>
                    val flows = Await.result(flowDal.findByFilter(_.streamId === streamId), minTimeOut)
                    if (flows.nonEmpty) {
                      riderLogger.info(s"user ${session.userId} can't delete stream $streamId now, please delete flow ${flows.map(_.id).mkString(",")} first")
                      complete(OK, getHeader(412, s"please delete flow ${flows.map(_.id).mkString(",")} first", session))
                    } else {
                      removeStreamDirective(streamId, session.userId)
                      if (stream.sparkAppid.getOrElse("") != "") {
                        runShellCommand("yarn application -kill " + stream.sparkAppid.get)
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
              riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
              complete(OK, getHeader(403, session))
            }
        }
      }
  }


  def getTopicsByInstanceId(route: String): Route = path(route / LongNumber / "instances" / LongNumber / "databases") {
    (id, instanceId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                onComplete(streamDal.getTopicByInstanceId(instanceId).mapTo[Seq[TopicSimple]]) {
                  case Success(topic) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } select databases where instance id is $instanceId success.")
                    complete(OK, ResponseSeqJson[TopicSimple](getHeader(200, session), topic))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } select databases where instance id is $instanceId failed", ex)
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

  def getTopicsByStreamId(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "intopics") {
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
                try {
                  val topics = streamDal.refreshTopicByStreamId(streamId, session.userId)
                  riderLogger.info(s"user ${
                    session.userId
                  } select topics where stream id is $streamId success.")
                  complete(OK, ResponseSeqJson[TopicDetail](getHeader(200, session), topics))
                } catch {
                  case ex: Exception =>
                    riderLogger.error(s"user ${
                      session.userId
                    } select topics where stream id is $streamId failed", ex)
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
