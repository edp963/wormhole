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
import edp.rider.RiderStarter.modules._
import edp.rider.common._
import edp.rider.kafka.KafkaUtils
import edp.rider.kafka.KafkaUtils.{getKafkaEarliestOffset, getKafkaLatestOffset}
import edp.rider.rest.persistence.dal.{FlowDal, FlowUdfDal, StreamDal}
import edp.rider.rest.persistence.entities.{FlowTable, _}
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.{AuthorizationProvider, FlowUtils, StreamUtils}
import edp.rider.service.util.CacheMap
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}

class FlowUserApi(flowDal: FlowDal, streamDal: StreamDal, flowUdfDal: FlowUdfDal) extends BaseUserApiImpl[FlowTable, Flow](flowDal) with RiderLogger with JsonSerializer {

  override def getByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / LongNumber) {
    (projectId, streamId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, setFailedResponse(session, "Insufficient permission"))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                streamDal.refreshStreamStatus(Some(projectId), Some(Seq(streamId)))
                riderLogger.info(s"user ${session.userId} refresh streams.")
                onComplete(flowDal.getById(flowId).mapTo[Option[FlowStream]]) {
                  case Success(flowStreamOpt) =>
                    riderLogger.info(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId success.")
                    flowStreamOpt match {
                      case Some(flowStream) =>
                        complete(OK, ResponseJson[FlowStream](getHeader(200, session), flowStream))
                      case None => complete(OK, ResponseJson[String](getHeader(200, session), ""))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId failed", ex)
                    complete(OK, setFailedResponse(session, ex.getMessage))
                }
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, setFailedResponse(session, "Insufficient permission"))
              }
            }
        }
      }
  }

  def getByFilterRoute(route: String): Route = path(route / LongNumber / "flows") {
    projectId =>
      get {
        parameters('visible.as[Boolean].?, 'sourceNs.as[String].?, 'sinkNs.as[String].?) {
          (visible, sourceNsOpt, sinkNsOpt) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    (visible, sourceNsOpt, sinkNsOpt) match {
                      case (None, Some(sourceNs), Some(sinkNs)) =>
                        onComplete(flowDal.findByFilter(flow => flow.sourceNs === sourceNs && flow.sinkNs === sinkNs).mapTo[Seq[Flow]]) {
                          case Success(flows) =>
                            if (flows.isEmpty) {
                              riderLogger.info(s"user ${session.userId} check flow source namespace $sourceNs and sink namespace $sinkNs doesn't exist.")
                              complete(OK, getHeader(200, session))
                            }
                            else {
                              riderLogger.warn(s"user ${session.userId} check flow source namespace $sourceNs and sink namespace $sinkNs already exists.")
                              complete(OK, getHeader(409, s"this source namespace $sourceNs and sink namespace $sinkNs already exists", session))
                            }
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} check flow source namespace $sourceNs and sink namespace $sinkNs does exist failed", ex)
                            complete(OK, setFailedResponse(session, ex.getMessage))
                        }
                      case (_, None, None) =>
                        streamDal.refreshStreamStatus(Some(projectId))
                        riderLogger.info(s"user ${session.userId} refresh project $projectId all streams.")
                        val future = if (visible.getOrElse(true)) flowDal.defaultGetAll(flow => flow.active === true && flow.projectId === projectId)
                        else flowDal.defaultGetAll(_.projectId === projectId)
                        onComplete(future.mapTo[Seq[FlowStream]]) {
                          case Success(flowStreams) =>
                            riderLogger.info(s"user ${session.userId} refresh project $projectId all flows success.")
                            complete(OK, ResponseSeqJson[FlowStream](getHeader(200, session), flowStreams.sortBy(_.id)))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} refresh project $projectId all flows failed", ex)
                            complete(OK, setFailedResponse(session, ex.getMessage))
                        }

                      case (_, _, _) =>
                        riderLogger.error(s"user ${session.userId} request url is not supported.")
                        complete(OK, setFailedResponse(session, "Insufficient permission"))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient permission"))
                  }
                }
            }
        }
      }

  }

  def postRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows") {
    (projectId, streamId) =>
      post {
        entity(as[SimpleFlow]) {
          simple =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    val checkFormat = FlowUtils.checkConfigFormat(simple.sinkConfig.getOrElse(""), simple.tranConfig.getOrElse(""))
                    if (checkFormat._1) {
                      val flowInsertSeq =
                        if (Await.result(streamDal.findById(streamId), minTimeOut).head.functionType != "hdfslog")
                          Seq(Flow(0, simple.projectId, simple.streamId, simple.sourceNs.trim, simple.sinkNs.trim, simple.parallelism, simple.consumedProtocol.trim, simple.sinkConfig,
                            simple.tranConfig, "new", None, None, None, active = true, currentSec, session.userId, currentSec, session.userId))
                        else
                          FlowUtils.flowMatch(projectId, streamId, simple.sourceNs).map(
                            sourceNs =>
                              Flow(0, simple.projectId, simple.streamId, sourceNs, sourceNs, simple.parallelism, simple.consumedProtocol, simple.sinkConfig,
                                simple.tranConfig, "new", None, None, None, active = true, currentSec, session.userId, currentSec, session.userId)
                          )
                      try {
                        val flows = flowDal.insertOrAbort(flowInsertSeq)
                        riderLogger.info(s"user ${session.userId} insert flows where project id is $projectId success.")
                        onComplete(flowDal.defaultGetAll(_.id inSet flows.map(_.id)).mapTo[Seq[FlowStream]]) {
                          case Success(flowStream) =>
                            val autoRegisteredTopics = flowStream.filter(_.streamType == StreamType.FLINK.toString)
                              .map {
                                flow =>
                                  val ns = namespaceDal.getNsDetail(flow.sourceNs)
                                  val latestOffset =
                                    try {
                                      KafkaUtils.getKafkaLatestOffset(ns._1.connUrl, ns._2.nsDatabase)
                                    } catch {
                                      case _: Exception =>
                                        ""
                                    }
                                  FlowInTopic(0, flow.id, ns._3.nsDatabaseId, latestOffset, RiderConfig.flink.defaultRate, true, currentSec, session.userId, currentSec, session.userId)
                              }
                            Await.result(flowInTopicDal.insert(autoRegisteredTopics), minTimeOut)
                            CacheMap.flowCacheMapRefresh
                            complete(OK, ResponseJson[Seq[FlowStream]](getHeader(200, session), flowStream))
                          case Failure(ex)
                          =>
                            riderLogger.error(s"user ${session.userId} refresh flow where project id is $projectId failed", ex)
                            complete(OK, setFailedResponse(session, ex.getMessage))
                        }
                      }
                      catch {
                        case ex: Exception =>
                          riderLogger.error(s"user ${session.userId} insert flows where project id is $projectId failed", ex)
                          complete(OK, setFailedResponse(session, ex.getMessage))
                      }
                    }
                    else {
                      riderLogger.warn(s"user ${session.userId} insert flow failed, caused by ${checkFormat._2}")
                      complete(OK, setFailedResponse(session, checkFormat._2))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient permission"))
                  }
                }
            }
        }
      }

  }


  def putRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows") {
    (projectId, streamId) =>
      put {
        entity(as[Flow]) {
          flow =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user")
                  complete(OK, getHeader(403, session))
                else {
                  streamDal.refreshStreamStatus(Some(projectId), Some(Seq(streamId)))
                  riderLogger.info(s"user ${session.userId} refresh streams.")
                  if (session.projectIdList.contains(projectId)) {
                    val checkFormat = FlowUtils.checkConfigFormat(flow.sinkConfig.getOrElse(""), flow.tranConfig.getOrElse(""))
                    if (checkFormat._1) {
                      val startedTime = if (flow.startedTime.getOrElse("") == "") null else flow.startedTime
                      val stoppedTime = if (flow.stoppedTime.getOrElse("") == "") null else flow.stoppedTime
                      val updateFlow = Flow(flow.id, flow.projectId, flow.streamId, flow.sourceNs.trim, flow.sinkNs.trim, flow.parallelism, flow.consumedProtocol.trim, flow.sinkConfig,
                        flow.tranConfig, flow.status, startedTime, stoppedTime, flow.logPath, flow.active, flow.createTime, flow.createBy, currentSec, session.userId)

                      val stream = Await.result(streamDal.findById(streamId), minTimeOut).head
                      val existFlow = Await.result(flowDal.findById(flow.id), minTimeOut).head

                      onComplete(flowDal.update(updateFlow).mapTo[Int]) {
                        case Success(_) =>
                          if (streamId != flow.streamId)
                            FlowUtils.stopFlow(streamId, flow.id, session.userId, stream.functionType, existFlow.sourceNs, existFlow.sinkNs, flow.tranConfig.getOrElse(""))
                          riderLogger.info(s"user ${session.userId} update flow ${updateFlow.id} where project id is $projectId success.")
                          onComplete(flowDal.defaultGetAll(_.id === updateFlow.id, "modify").mapTo[Seq[FlowStream]]) {
                            case Success(flowStream) =>
                              riderLogger.info(s"user ${session.userId} refresh flow where project id is $projectId and flow id is ${updateFlow.id} success.")
                              complete(OK, ResponseJson[FlowStream](getHeader(200, session), flowStream.head))
                            case Failure(ex) =>
                              riderLogger.error(s"user ${session.userId} refresh flow where project id is $projectId and flow id is ${updateFlow.id} failed", ex)
                              complete(OK, getHeader(451, ex.getMessage, session))
                          }
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} update flow ${updateFlow.id} where project id is $projectId failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.warn(s"user ${session.userId} update flow failed, caused by ${checkFormat._2}")
                      complete(OK, getHeader(400, checkFormat._2, session))
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

  def lookupSqlVerifyRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / "sqls" / "lookup") {
    (projectId, _) =>
      put {
        entity(as[Sql]) {
          sql =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user")
                  complete(OK, getHeader(403, session))
                else {
                  try {
                    if (session.projectIdList.contains(projectId)) {
                      //                      val verify = FlowUtils.sqlVerify(sql.sql)
                      //                      if (verify._1) {
                      //                        val tables = FlowUtils.getNsSeqByLookupSql(sql.sql)
                      //                        val nonPermTables = NamespaceUtils.permCheck(projectId, tables)
                      //                        if (nonPermTables.isEmpty) {
                      riderLogger.info(s"user ${session.userId} verify flow lookup sql all tables have permission")
                      complete(OK, getHeader(200, session))
                      //                        } else {
                      //                          riderLogger.info(s"user ${session.userId} verify flow $flowId lookup sql ${nonPermTables.mkString(",")} tables have non permission")
                      //                          complete(OK, getHeader(406, s"none permission to visit ${nonPermTables.mkString(",")} tables", session))
                      //                        }
                      //                      } else {
                      //                        complete(OK, getHeader(406,s"sql has syntax error, please check it", session))
                      //                      }
                    } else {
                      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                      complete(OK, getHeader(403, session))
                    }
                  } catch {
                    case ex: Exception =>
                      riderLogger.error(s"user ${session.userId} verify flow sql tables permission failed", ex)
                      complete(OK, getHeader(451, ex.getMessage, session))
                  }

                }
            }
        }
      }

  }

  def postFlowTopicsOffsetRoute(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "topics") {
    (projectId, flowId) =>
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
                  if (session.projectIdList.contains(projectId)) {
                    try {
                      if (Await.result(flowDal.findById(flowId), minTimeOut).nonEmpty) {
                        postFlowTopicsOffsetResponse(projectId, flowId, topics, session)
                      } else {
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed caused by flow $flowId not found.")
                        complete(OK, setFailedResponse(session, "flow not found."))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed", ex)
                        complete(OK, setFailedResponse(session, ex.getMessage))
                    }
                  } else {
                    riderLogger.error(s"user ${
                      session.userId
                    } doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient Permission"))
                  }
                }
            }
          }
        }
      }
  }

  def postFlowTopicsOffsetResponse(id: Long, flowId: Long, topics: GetTopicsOffsetRequest, session: SessionClass): Route = {
    val allTopics = flowDal.getFlowTopicsAllOffsets(flowId)
    val userDefinedTopics = allTopics.userDefinedTopics
    val userDefinedTopicsName = userDefinedTopics.map(_.name)
    val newTopics = topics.userDefinedTopics.filter(!userDefinedTopicsName.contains(_))
    val newTopicsOffset = newTopics.map(topic => {
      val kafkaInfo = flowDal.getFlowKafkaInfo(flowId)
      val latestOffset = getKafkaLatestOffset(kafkaInfo._2, topic)
      val earliestOffset = getKafkaEarliestOffset(kafkaInfo._2, topic)
      val consumedOffset = earliestOffset
      SimpleFlowTopicAllOffsets(topic, RiderConfig.flink.defaultRate, consumedOffset, earliestOffset, latestOffset)
    })
    val response = GetFlowTopicsOffsetResponse(
      allTopics.autoRegisteredTopics.map(topic =>
        SimpleFlowTopicAllOffsets(topic.name, RiderConfig.flink.defaultRate, topic.consumedLatestOffset, topic.kafkaEarliestOffset, topic.kafkaLatestOffset)),
      userDefinedTopics.filter(topic => topics.userDefinedTopics.contains(topic.name)).map(topic =>
        SimpleFlowTopicAllOffsets(topic.name, RiderConfig.flink.defaultRate, topic.consumedLatestOffset, topic.kafkaEarliestOffset, topic.kafkaLatestOffset))
        ++: newTopicsOffset)
    riderLogger.info(s"user ${session.userId} get flow $flowId topics offset success.")
    complete(OK, ResponseJson[GetFlowTopicsOffsetResponse](getHeader(200, session), response))
  }

  def postFlowUserDefinedTopicRoute(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "topics" / "userdefined") {
    (projectId, flowId) =>
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
                  if (session.projectIdList.contains(projectId)) {
                    try {
                      if (Await.result(flowDal.findById(flowId), minTimeOut).nonEmpty) {
                        postUserDefinedTopicResponse(projectId, flowId, postTopic, session)
                      } else {
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed caused by stream $flowId not found.")
                        complete(OK, setFailedResponse(session, "flow not found."))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"user ${session.userId} insert user defined topic failed", ex)
                        complete(OK, setFailedResponse(session, ex.getMessage))
                    }
                  } else {
                    riderLogger.error(s"user ${
                      session.userId
                    } doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient Permission"))
                  }
                }
            }
          }
        }
      }
  }

  def postUserDefinedTopicResponse(projectId: Long, flowId: Long, postTopic: PostUserDefinedTopic, session: SessionClass): Route = {
    // topic duplication check
    if (flowDal.checkFlowTopicExists(flowId, postTopic.name)) {
      throw new Exception("flow topic relation already exists.")
    }
    val kafkaInfo = flowDal.getFlowKafkaInfo(flowId)
    // get kafka earliest/latest offset
    val latestOffset = getKafkaLatestOffset(kafkaInfo._2, postTopic.name)
    val earliestOffset = getKafkaEarliestOffset(kafkaInfo._2, postTopic.name)

    // response
    val topicResponse = SimpleFlowTopicAllOffsets(postTopic.name, RiderConfig.flink.defaultRate, earliestOffset, earliestOffset, latestOffset)

    riderLogger.info(s"user ${session.userId} get user defined topic offsets success.")
    complete(OK, ResponseJson[SimpleFlowTopicAllOffsets](getHeader(200, session), topicResponse))
  }

  def getFlowTopicsRoute(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "topics") {
    (projectId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                Await.result(flowDal.findById(flowId), minTimeOut) match {
                  case Some(_) => getTopicsResponse(projectId, flowId, session)
                  case None =>
                    riderLogger.info(s"user ${session.userId} get flow $flowId topics failed caused by flow not found.")
                    complete(OK, setFailedResponse(session, "flow not found."))
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $projectId.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }


  private def getTopicsResponse(projectId: Long, flowId: Long, session: SessionClass): Route
  = {

    val topics = flowDal.getFlowTopicsAllOffsets(flowId)
    riderLogger.info(s"user ${session.userId} get flow $flowId topics success.")
    complete(OK, ResponseJson[GetTopicsResponse](getHeader(200, session), topics))
  }

  def getFlowUdfsRoute(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "udfs") {
    (projectId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(projectId)) {
                Await.result(flowDal.findById(flowId), minTimeOut) match {
                  case Some(_) => getFlowUdfsResponse(flowId, session)
                  case None =>
                    riderLogger.info(s"user ${session.userId} get flow $flowId topics failed caused by flow not found.")
                    complete(OK, setFailedResponse(session, "flow not found."))
                }
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }

  def getFlowUdfsResponse(flowId: Long, session: SessionClass): Route = {
    val udfs = flowUdfDal.getFlowUdf(flowId)
    riderLogger.info(s"user ${session.userId} get flow $flowId topics success.")
    complete(OK, ResponseSeqJson[FlowUdfResponse](getHeader(200, session), udfs))
  }

  def startFlinkRoute(route: String): Route = path(route / LongNumber / "flinkstreams" / "flows" / LongNumber / "start") {
    (projectId, flowId) =>
      put {
        entity(as[FlowDirective]) {
          flowDirective =>
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
                  startResponse(projectId, flowId, flowDirective, session)
                }
            }
        }
      }
  }

  private def startResponse(projectId: Long, flowId: Long, flowDirective: FlowDirective, session: SessionClass): Route = {
    try {
      if (session.projectIdList.contains(projectId)) {
        FlowUtils.updateUdfsByStart(flowId, flowDirective.udfInfo, session.userId)
        FlowUtils.updateTopicsByStart(flowId, flowDirective.topicInfo, session.userId)
        val flowStream = Await.result(flowDal.defaultGetAll(_.id === flowId, "start"), minTimeOut).head
        if (flowStream.msg.contains("success")) {
          riderLogger.info(s"user ${session.userId} start flow $flowId success.")
          //          flowDal.updateTimeAndUser(Seq(flowId), session.userId)
          //          complete(OK, ResponseJson[FlowStream](getHeader(200, session), flowStream))
          val response = StartFlinkFlowResponse(flowId, flowStream.status, flowStream.disableActions, FlowUtils.getHideActions(flowStream.streamType, flowStream.functionType),
            flowStream.startedTime, flowStream.stoppedTime)
          complete(OK, ResponseJson[StartFlinkFlowResponse](getHeader(200, session), response))
        } else {
          riderLogger.info(s"user ${session.userId} can't start flow.")
          complete(OK, setFailedResponse(session, flowStream.msg))
        }
      } else {
        riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
        complete(OK, setFailedResponse(session, "Insufficient Permission"))
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user ${session.userId} get resources for project ${projectId} failed when start new flow", ex)
        complete(OK, setFailedResponse(session, ex.getMessage))
    }
  }

  def stopFlinkRoute(route: String): Route = path(route / LongNumber / "flinkstreams" / "flows" / LongNumber / "stop") {
    (projectId, flowId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                val flowStream = Await.result(flowDal.defaultGetAll(_.id === flowId, "stop"), minTimeOut).head
                if (flowStream.msg.contains("success")) {
                  riderLogger.info(s"user ${session.userId} stop flow $flowId success.")
                  flowDal.updateTimeAndUser(Seq(flowId), session.userId)
                  complete(OK, ResponseJson[StartFlinkFlowResponse](getHeader(200, session), StartFlinkFlowResponse(flowId, flowStream.status, flowStream.disableActions, FlowUtils.getHideActions(flowStream.streamType, flowStream.functionType))))
                } else {
                  riderLogger.info(s"user ${session.userId} can't stop flow.")
                  complete(OK, setFailedResponse(session, flowStream.msg))
                }
                //                val flow = Await.result(flowDal.findById(flowId), minTimeOut).head
                //                val stream = Await.result(streamDal.findById(flow.streamId), minTimeOut).head
                //                if (!FlowUtils.getDisableActions(flow).contains("stop")) {
                //                  val status = stopFlinkFlow(stream.sparkAppid.get, FlowUtils.getFlowName(flow.sourceNs, flow.sinkNs), flow.status)
                //                  riderLogger.info(s"user ${session.userId} stop flow $flowId success.")
                //                  flowDal.updateStatusByAction(flowId, status, flow.startedTime, Option(currentSec))
                //                  complete(OK, ResponseJson[FlinkResponse](getHeader(200, session), FlinkResponse(flow.id, s"$START,$RENEW,$STOP", FlowUtils.getHideActions(stream.streamType))))
                //                } else {
                //                  riderLogger.info(s"user ${session.userId} can't stop flow $flowId now")
                //                  complete(OK, getHeader(406, s"stop is forbidden", session))
                //                }
              }
              else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $projectId.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }


  def getLogByFlowId(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "logs") {
    (id, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
              if (session.projectIdList.contains(id)) {
                val log = FlowUtils.getLog(flowId)
                complete(OK, ResponseJson[String](getHeader(200, session), log))
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

  def getDriftStreamsByFlowId(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "drift" / "streams") {
    (projectId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                val flowStream = Await.result(flowDal.getById(flowId), minTimeOut).head
                if (flowStream.disableActions.contains(Action.DRIFT.toString) || flowStream.hideActions.contains(Action.DRIFT.toString)) {
                  complete(OK, ResponseJson[String](getHeader(200, session), "it's not allowed to drift."))
                } else {
                  val driftStreams = StreamUtils.getDriftStreamsByStreamId(flowStream.streamId)
                  if (driftStreams.nonEmpty) {
                    complete(OK, ResponseSeqJson[SimpleStreamInfo](getHeader(200, session), driftStreams))
                  } else {
                    val msg = "There is no available stream to drift, please create it first."
                    complete(OK, ResponseJson[String](getHeader(200, session), msg))
                  }
                }
              } else {
                riderLogger.error(s"user ${
                  session.userId
                } doesn't have permission to access the project $projectId.")
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }

  def getDriftFlowTip(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "drift" / "tip") {
    (projectId, flowId) =>
      get {
        parameter('streamId.as[Long]) {
          streamId =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    try {
                      val preFlowStream = Await.result(flowDal.getById(flowId), minTimeOut).head
                      val tip = FlowUtils.getDriftTip(preFlowStream, streamId)
                      if (tip._1)
                        complete(OK, ResponseJson[String](getHeader(200, session), tip._2))
                      else
                        complete(OK, setFailedResponse(session, tip._2))
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"get flow $flowId drift tip failed", ex)
                        complete(OK, setFailedResponse(session, ex.getMessage))
                    }
                  } else {
                    riderLogger.error(s"user ${
                      session.userId
                    } doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient Permission"))
                  }
                }
            }
        }
      }
  }


  def driftFlow(route: String): Route = path(route / LongNumber / "flows" / LongNumber / "drift") {
    (projectId, flowId) =>
      put {
        entity(as[DriftFlowRequest]) {
          driftFlowRequest =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, setFailedResponse(session, "Insufficient Permission"))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    try {
                      val preFlowStream = Await.result(flowDal.getById(flowId), minTimeOut).head
                      val msg = FlowUtils.driftFlow(preFlowStream, driftFlowRequest, session.userId)
                      if (msg._1) {
                        riderLogger.info(s"user ${session.userId} drift flow $flowId success")
                        val flowStream = Await.result(flowDal.getById(flowId), minTimeOut).get
                        val response = DriftFlowResponse(flowStream.id, flowStream.status, flowStream.streamId, flowStream.streamStatus,
                          flowStream.disableActions, flowStream.hideActions, flowStream.startedTime, flowStream.stoppedTime, msg._2)
                        complete(OK, ResponseJson[DriftFlowResponse](getHeader(200, session), response))
                      } else {
                        complete(OK, setFailedResponse(session, msg._2))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"get flow $flowId drift tip failed", ex)
                        complete(OK, setFailedResponse(session, ex.getMessage))
                    }
                  } else {
                    riderLogger.error(s"user ${
                      session.userId
                    } doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient Permission"))
                  }
                }
            }
        }
      }
  }

}
