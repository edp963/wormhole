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
import edp.rider.rest.persistence.dal.{FlowDal, StreamDal}
import edp.rider.rest.persistence.entities._

import scala.concurrent.Await
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.{AuthorizationProvider, FlowUtils, NamespaceUtils, NsDatabaseUtils}
import edp.rider.service.util.CacheMap
import slick.jdbc.MySQLProfile.api._

import scala.util.{Failure, Success}

class FlowUserApi(flowDal: FlowDal, streamDal: StreamDal) extends BaseUserApiImpl[FlowTable, Flow](flowDal) with RiderLogger with JsonSerializer {

  override def getByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / LongNumber) {
    (projectId, streamId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                streamDal.refreshStreamStatus(Some(projectId), Some(Seq(streamId)))
                riderLogger.info(s"user ${session.userId} refresh streams.")
                onComplete(flowDal.getById(projectId, flowId).mapTo[Option[FlowStreamInfo]]) {
                  case Success(flowStreamOpt) =>
                    riderLogger.info(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId success.")
                    flowStreamOpt match {
                      case Some(flowStream) => complete(OK, ResponseJson[FlowStreamInfo](getHeader(200, session), flowStream))
                      case None => complete(OK, ResponseJson[String](getHeader(200, session), ""))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId failed", ex)
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
                            complete(OK, getHeader(451, ex.getMessage, session))
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
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }

                      case (_, _, _) =>
                        riderLogger.error(s"user ${session.userId} request url is not supported.")
                        complete(OK, getHeader(501, session))
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
                        if (Await.result(streamDal.findById(streamId), minTimeOut).head.streamType != "hdfslog")
                          Seq(Flow(0, simple.projectId, simple.streamId, simple.sourceNs.trim, simple.sinkNs.trim, simple.consumedProtocol.trim, simple.sinkConfig,
                            simple.tranConfig, "new", None, None, active = true, currentSec, session.userId, currentSec, session.userId))
                        else
                          FlowUtils.flowMatch(projectId, streamId, simple.sourceNs).map(
                            sourceNs =>
                              Flow(0, simple.projectId, simple.streamId, sourceNs, sourceNs, simple.consumedProtocol, simple.sinkConfig,
                                simple.tranConfig, "new", None, None, active = true, currentSec, session.userId, currentSec, session.userId)
                          )
                      try {
                        val flows = flowDal.insertOrAbort(flowInsertSeq)
                        riderLogger.info(s"user ${session.userId} insert flows where project id is $projectId success.")
                        onComplete(flowDal.defaultGetAll(_.id inSet flows.map(_.id)).mapTo[Seq[FlowStream]]) {
                          case Success(flowStream) =>
                            CacheMap.flowCacheMapRefresh
                            complete(OK, ResponseJson[Seq[FlowStream]](getHeader(200, session), flowStream))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} refresh flow where project id is $projectId failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      } catch {
                        case ex: Exception =>
                          riderLogger.error(s"user ${session.userId} insert flows where project id is $projectId failed", ex)
                          if (ex.getMessage.contains("Duplicate entry"))
                            complete(OK, getHeader(409, "this source to sink already exists", session))
                          else
                            complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.warn(s"user ${session.userId} insert flow failed, caused by ${checkFormat._2}")
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
                      val updateFlow = Flow(flow.id, flow.projectId, flow.streamId, flow.sourceNs.trim, flow.sinkNs.trim, flow.consumedProtocol.trim, flow.sinkConfig,
                        flow.tranConfig, flow.status, startedTime, stoppedTime, flow.active, flow.createTime, flow.createBy, currentSec, session.userId)

                      val stream = Await.result(streamDal.findById(streamId), minTimeOut).head
                      val existFlow = Await.result(flowDal.findById(flow.id), minTimeOut).head

                      onComplete(flowDal.update(updateFlow).mapTo[Int]) {
                        case Success(_) =>
                          if (streamId != flow.streamId)
                            FlowUtils.stopFlow(streamId, flow.id, session.userId, stream.streamType, existFlow.sourceNs, existFlow.sinkNs, flow.tranConfig.getOrElse(""))
                          riderLogger.info(s"user ${session.userId} update flow $updateFlow where project id is $projectId success.")
                          onComplete(flowDal.defaultGetAll(_.id === updateFlow.id, "modify").mapTo[Seq[FlowStream]]) {
                            case Success(flowStream) =>
                              riderLogger.info(s"user ${session.userId} refresh flow where project id is $projectId and flow id is ${updateFlow.id} success.")
                              complete(OK, ResponseJson[FlowStream](getHeader(200, session), flowStream.head))
                            case Failure(ex) =>
                              riderLogger.error(s"user ${session.userId} refresh flow where project id is $projectId and flow id is ${updateFlow.id} failed", ex)
                              complete(OK, getHeader(451, ex.getMessage, session))
                          }
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} update flow $updateFlow where project id is $projectId failed", ex)
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

}
