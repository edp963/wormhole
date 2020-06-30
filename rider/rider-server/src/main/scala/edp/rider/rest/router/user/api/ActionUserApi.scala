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
import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.common.{DebugRequest, RiderConfig, RiderLogger}
import edp.rider.kafka.WormholeGetOffsetUtils.{getEarliestOffset, getLatestOffset}
import edp.rider.rest.persistence.dal.{FlowDal, NamespaceDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.InstanceUtils
//import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router._
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._

import scala.util.{Failure, Success}

class ActionUserApi(streamDal: StreamDal, flowDal: FlowDal, namespaceDal: NamespaceDal) extends Directives with RiderLogger with JsonSerializer {

  def putRoute(route: String): Route = path(route / LongNumber / "actions") {
    id =>
      put {
        entity(as[ActionClass]) {
          actionClass =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    if (actionClass.flowIds != "") {
                      onComplete(flowDal.flowAction(actionClass, session.userId)) {
                        case Success(flowStreams) =>
                          riderLogger.info(s"user ${session.userId} ${actionClass.action} ${actionClass.flowIds} flow.")
                          complete(OK, ResponseSeqJson[FlowStream](getHeader(200, session), flowStreams))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} ${actionClass.action} ${actionClass.flowIds} failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.info(s"user ${session.userId} do nothing on streams and flows.")
                      complete(OK, ResponseJson[String](getHeader(200, session), ""))
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

  def topicRoute(route: String): Route = path(route / LongNumber / "topics") {
    id =>
      get {
        parameter('sourceNs.as[String].?) {
          sourceNs =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    val ns = namespaceDal.getNsDetail(sourceNs.get)
                    val (earliestOffset, latestOffset) =
                      try {
                        val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(ns._1.connConfig.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
                        (getEarliestOffset(ns._1.connUrl, ns._2.nsDatabase, inputKafkaKerberos), getLatestOffset(ns._1.connUrl, ns._2.nsDatabase, inputKafkaKerberos))
                      } catch {
                        case _: Exception =>
                          ("", "")
                      }
                    val autoTopicsResponse = Seq(TopicAllOffsets(-1, ns._2.nsDatabase, RiderConfig.flink.defaultRate, "0:", earliestOffset, latestOffset))
                    val topics = GetTopicsResponse(autoTopicsResponse, Seq())
                    complete(OK, ResponseJson[GetTopicsResponse](getHeader(200, session), topics))
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                    complete(OK, getHeader(403, session))
                  }
                }
            }
        }
      }
  }

  def debugRoute(route: String): Route = path(route / LongNumber / "debug") {
    id =>
      put {
        entity(as[DebugRequest]) {
          debugRequest =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    val actionClass = ActionClass("debug", Math.abs(debugRequest.simpleFlow.flowName.hashCode).toString)
                    onComplete(flowDal.debugFlowAction(actionClass, debugRequest, session.userId)) {
                      case Success(flowStreams) =>
                        riderLogger.info(s"user ${session.userId} ${actionClass.action} ${actionClass.flowIds} flow.")
                        complete(OK, ResponseSeqJson[(FlowStream, Option[String])](getHeader(200, session), flowStreams))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} ${actionClass.action} ${actionClass.flowIds} failed", ex)
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
