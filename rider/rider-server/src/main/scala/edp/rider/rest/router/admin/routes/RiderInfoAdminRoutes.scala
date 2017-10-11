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

package edp.rider.rest.router.admin.routes

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.common.{RiderConfig, RiderInfo, RiderLogger}
import edp.rider.module._
import edp.rider.rest.router.{ResponseJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import io.swagger.annotations._
import edp.rider.rest.router.JsonProtocol._

@Api(value = "/riderInfo", consumes = "application/json", produces = "application/json")
@Path("/admin/riderInfo")
class RiderInfoAdminRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives with RiderLogger {

  lazy val routes: Route = getRiderInfo

  lazy val basePath = "riderInfo"

  @ApiOperation(value = "get rider config information", notes = "", nickname = "", httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getRiderInfo: Route = path(basePath) {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin") {
            riderLogger.warn(s"${session.userId} has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            complete(OK, ResponseJson[RiderInfo](getHeader(200, session), RiderConfig.riderInfo))
          }
      }
    }
  }
}


