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
import akka.http.scaladsl.server.Directives
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule}
import edp.rider.rest.util.AuthorizationProvider
import io.swagger.annotations._
import edp.rider.rest.util.ResponseUtils._

import scala.util.{Failure, Success}
import JsonProtocol._
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.entities.User

@Api(value = "login", consumes = "application/json", produces = "application/json")
@Path("/login")
class LoginRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule) extends Directives with RiderLogger {

  lazy val routes = accessTokenRoute

  @ApiOperation(value = "Login into the server and return token", notes = "", nickname = "login", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "Login", value = "Login information", required = true, dataType = "edp.rider.rest.router.LoginClass", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 210, message = "Wrong password"),
    new ApiResponse(code = 404, message = "Not found"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def accessTokenRoute = path("login") {
    post {
      entity(as[LoginClass]) {
        login =>
          onComplete(AuthorizationProvider.createSessionClass(login)) {
            case Success(sessionEither) =>
              sessionEither.fold(
                authorizationError => {
                  riderLogger.error(s"${login.email} login rejected, ${authorizationError.getMessage}.")
                  complete(OK, getHeader(authorizationError.statusCode.intValue, null))
                },
                result => {
                  riderLogger.info(s"${login.email} login success.")
                  complete(OK, ResponseJson[User](getHeader(200, result.session), result.user))
                }
              )
            case Failure(ex) =>
              riderLogger.error(s"${login.email} login failed", ex)
              complete(OK, getHeader(451, ex.getMessage, null))
          }
      }
    }
  }

}
