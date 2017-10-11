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


package edp.rider.rest.router

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.common.RiderLogger
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule}
import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import io.swagger.annotations._

import scala.util.{Failure, Success}

@Api(value = "generateToken", consumes = "application/json", produces = "application/json")
@Path("/users")
class GenTokenRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule) extends Directives with RiderLogger {

  lazy val routes = genToken

  @Path("/token")
  @ApiOperation(value = "generate token for user", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "userInfo", value = "user name and password", required = true, dataType = "edp.rider.rest.router.LoginClass", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "generate token success"),
    new ApiResponse(code = 210, message = "Wrong password"),
    new ApiResponse(code = 404, message = "Not found"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def genToken: Route = path("users" / "token") {
    put {
      entity(as[LoginClass]) {
        login =>
          onComplete(AuthorizationProvider.createSessionClass(login, true)) {
            case Success(sessionEither) =>
              sessionEither.fold(
                authorizationError => {
                  riderLogger.error(s"${login.email} request for token rejected, ${authorizationError.getMessage}.")
                  complete(OK, getHeader(authorizationError.statusCode.intValue, null))
                },
                result => {
                  riderLogger.info(s"${login.email} request for token success.")
                  complete(OK, getHeader(200, result.session))
                }
              )
            case Failure(ex) =>
              riderLogger.error(s"${login.email} request for token failed", ex)
              complete(OK, getHeader(451, ex.getMessage, null))
          }
      }
    }
  }
}
