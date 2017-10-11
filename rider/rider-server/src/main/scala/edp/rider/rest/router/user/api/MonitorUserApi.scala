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
import akka.http.scaladsl.server._
import edp.rider.common.{GrafanaConnectionInfo, RiderLogger}
import edp.rider.monitor.Dashboard
import edp.rider.rest.persistence.dal.StreamDal

import edp.rider.rest.router.{ResponseJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._

class MonitorUserApi(streamDal: StreamDal) extends BaseUserApiImpl(streamDal) with RiderLogger {
  def getDashboardByProjectIdRoute(route: String): Route = path(route / LongNumber / "monitors") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                riderLogger.info(s"user ${session.userId} get project $id monitor info success.")
                complete(OK, ResponseJson[GrafanaConnectionInfo](getHeader(200, session), Dashboard.getViewerDashboardInfo(id)))
              }
              else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
     
  }
}
