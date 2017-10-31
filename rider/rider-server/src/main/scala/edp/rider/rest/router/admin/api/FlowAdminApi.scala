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


package edp.rider.rest.router.admin.api

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.dal.{FlowDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._
import slick.jdbc.MySQLProfile.api._

import scala.util.{Failure, Success}

class FlowAdminApi(flowDal: FlowDal, streamDal: StreamDal) extends BaseAdminApiImpl(flowDal) with RiderLogger {

  override def getByAllRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                streamDal.getStreamDetail()
                riderLogger.info(s"user ${session.userId} refresh streams.")
                onComplete(flowDal.adminGetAll(visible.getOrElse(true)).mapTo[Seq[FlowStreamAdmin]]) {
                  case Success(flowStreams) =>
                    riderLogger.info(s"user ${session.userId} select all $route success.")
                    complete(OK, ResponseSeqJson[FlowStreamAdmin](getHeader(200, session), flowStreams.sortBy(_.id)))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select all $route failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }


  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "flows") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              streamDal.getStreamDetail(Some(id))
              riderLogger.info(s"user ${session.userId} refresh streams.")
              onComplete(flowDal.defaultGetAll(flow => flow.active === true && flow.projectId === id).mapTo[Seq[FlowStream]]) {
                case Success(flowStreams) =>
                  riderLogger.info(s"user ${session.userId} select all flows success where project id is $id.")
                  complete(OK, ResponseSeqJson[FlowStream](getHeader(200, session), flowStreams.sortBy(_.id)))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select all flows failed where project id is $id", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }

  }

}
