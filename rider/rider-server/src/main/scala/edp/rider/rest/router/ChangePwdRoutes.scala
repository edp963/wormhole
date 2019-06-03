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
import edp.rider.rest.persistence.entities.User
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.{AuthorizationProvider, CommonUtils, PassWordNotMatch, UserNotFound}
import io.swagger.annotations._

import scala.concurrent.Await
import scala.util.{Failure, Success}

@Api(value = "/changepwd", consumes = "application/json")
@Path("/changepwd")
class ChangePwdRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule) extends Directives with RiderLogger with JsonSerializer {

  lazy val routes: Route = changeUserPwdRoute

  @ApiOperation(value = "change user's password", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "changePwd", value = "change password information", required = true, dataType = "edp.rider.rest.router.ChangeUserPwdClass", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 210, message = "Wrong password"),
    new ApiResponse(code = 204, message = "Not found"),
    new ApiResponse(code = 401, message = "Unauthorized"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")))
  def changeUserPwdRoute: Route = path("changepwd") {
    post {
      entity(as[ChangeUserPwdClass]) { changePwd =>
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            onComplete(modules.userDal.findById(changePwd.id).mapTo[Option[User]]) {
              case Success(userOpt) => userOpt match {
                case Some(user) =>
                  if (changePwd.oldPass.nonEmpty) {
                    if (user.password == changePwd.oldPass.get) {
                      onComplete(modules.userDal.update(updatePass(user, changePwd.newPass)).mapTo[Int]) {
                        case Success(_) =>
                          riderLogger.info(s"${session.userId} change password success.")
                          complete(OK, getHeader(200, session))
                        case Failure(ex) =>
                          riderLogger.error(s"${session.userId} change password failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.warn(s"${session.userId} change password failed, the old password is wrong.")
                      complete(PassWordNotMatch.statusCode, getHeader(PassWordNotMatch.statusCode.intValue, session))
                    }
                  } else {
                    Await.result(modules.userDal.update(updatePass(user, changePwd.newPass)), CommonUtils.minTimeOut)
                    complete(OK, getHeader(200, session))
                  }
                case None =>
                  riderLogger.warn(s"${session.userId} change password failed, the user doesn't exist.")
                  complete(OK, getHeader(UserNotFound.statusCode.intValue, session))
              }
              case Failure(ex) => complete(OK, getHeader(451, ex.getMessage, session))
            }
        }
      }
    }


  }

  private def updatePass(user: User, password: String): User = {
    User(user.id, user.email, password, user.name, user.roleType, user.preferredLanguage, user.active, user.createTime, user.createBy, user.updateTime, user.updateBy)
  }

}
