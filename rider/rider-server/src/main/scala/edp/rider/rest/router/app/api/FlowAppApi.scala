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


package edp.rider.rest.router.app.api

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import edp.rider.RiderStarter._
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.dal.{FlowDal, ProjectDal, StreamDal}
import edp.rider.rest.persistence.entities._
//import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router._
import edp.rider.rest.util.AppUtils._
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.FlowUtils._
import edp.rider.rest.util.CommonUtils._

import scala.concurrent.Await

class FlowAppApi(flowDal: FlowDal, streamDal: StreamDal, projectDal: ProjectDal) extends BaseAppApiImpl(flowDal) with RiderLogger with JsonSerializer {

  def postRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows") {
    (projectId, streamId) =>
      post {
        entity(as[AppFlow]) {
          appFlow =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "app") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, null))
                } else {
                  try {
                    prepare(None, Some(appFlow), session, projectId, Some(streamId)) match {
                      case Right(tuple) =>
                        val flow = tuple._2.get
                        try {
                          val stream = Await.result(streamDal.findById(streamId), minTimeOut).head
                          if (startFlow(stream.id, stream.name, stream.streamType, flow.id, flow.sourceNs, flow.sinkNs, flow.consumedProtocol, flow.sinkConfig.getOrElse(""), flow.tranConfig.getOrElse(""), flow.tableKeys.getOrElse(""), flow.updateBy)) {
                            riderLogger.info(s"user ${session.userId} start flow ${flow.id} success.")
                            complete(OK, ResponseJson[AppFlowResponse](getHeader(200, null), AppFlowResponse(flow.id, flow.status)))
                          } else {
                            riderLogger.error(s"user ${session.userId} send flow ${flow.id} start directive failed")
                            flowDal.updateStatusByFeedback(flow.id, "failed")
                            complete(OK, getHeader(451, null))
                          }
                        } catch {
                          case ex: Exception =>
                            riderLogger.error(s"user ${session.userId} start flow ${flow.id} failed", ex)
                            flowDal.updateStatusByFeedback(flow.id, "failed")
                            complete(OK, getHeader(451, null))
                        }
                      case Left(response) => complete(OK, response)
                    }
                  } catch {
                    case ex: Exception =>
                      riderLogger.error(s"user ${session.userId} start flow $appFlow failed", ex)
                      complete(OK, getHeader(451, null))
                  }
                }

            }
        }
      }

  }

  def stopRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / LongNumber / "actions" / "stop") {
    (projectId, streamId, flowId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "app") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, null))
            } else {
              try {
                val project = Await.result(projectDal.findById(projectId), minTimeOut)
                if (project.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request to stop project $projectId, stream $streamId, flow $flowId, but the project $projectId doesn't exist.")
                  complete(OK, getHeader(403, s"project $projectId doesn't exist", null))
                }
                val streamInfo = Await.result(streamDal.findById(streamId), minTimeOut)
                if (streamInfo.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request stop flow $flowId, but the stream $streamId doesn't exist")
                  complete(OK, getHeader(403, s"stream $streamId doesn't exist", null))
                }
                val flowSearch = Await.result(flowDal.findById(flowId), minTimeOut)
                if (flowSearch.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request stop flow $flowId, but it doesn't exist")
                  complete(OK, getHeader(403, s"flow $streamId doesn't exist", null))
                }
                val stream = streamInfo.head
                val flow = flowSearch.head
                if (flow.status != "stopped") {
                  if (stopFlow(stream.id, flowId, flow.updateBy, stream.streamType, flow.sourceNs, flow.sinkNs, flow.tranConfig.getOrElse(""))) {
                    riderLogger.info(s"user ${session.userId} stop flow $flowId success.")
                    flowDal.updateStatusByFeedback(flowId, "stopped")
                    complete(OK, ResponseJson[AppFlowResponse](getHeader(200, session), AppFlowResponse(flowId, "stopped")))
                  } else {
                    riderLogger.error(s"user ${session.userId} send flow $flowId stop directive failed")
                    complete(OK, getHeader(451, null))
                  }
                } else {
                  riderLogger.info(s"user ${session.userId} flow $flowId status is ${flow.status}, doesn't need to stop.")
                  complete(OK, ResponseJson[AppFlowResponse](getHeader(200, session), AppFlowResponse(flowId, "stopped")))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} stop flow $flowId failed", ex)
                  complete(OK, getHeader(451, null))
              }
            }
        }
      }
  }

}
