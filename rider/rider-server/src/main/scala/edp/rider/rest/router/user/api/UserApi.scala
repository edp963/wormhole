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
import edp.rider.rest.persistence.dal.{RelProjectUserDal, UserDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.currentSec
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._
import scala.util.{Failure, Success}

class UserApi(userDal: UserDal, relProjectUserDal: RelProjectUserDal) extends BaseUserApiImpl[UserTable, User](userDal) with RiderLogger with JsonSerializer {

  def getUserByProjectId(route: String): Route = path(route / LongNumber / "users") {
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
                onComplete(relProjectUserDal.getUserByProjectId(id).mapTo[Seq[User]]) {
                  case Success(users) =>
                    riderLogger.info(s"user ${session.userId} select users where project id is $id success.")
                    complete(OK, ResponseSeqJson[User](getHeader(200, session), users.sortBy(_.id)))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select users where project id is $id failed", ex)
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


  def getUserById(route: String): Route = path(route / LongNumber) {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(userDal.findById(id).mapTo[Option[User]]) {
                case Success(userOpt) =>
                  userOpt match {
                    case Some(user) =>
                      riderLogger.info(s"user ${session.userId} select user $id where project id is $id success.")
                      val noPwdUser = User(user.id, user.email, "", user.name, user.roleType, user.preferredLanguage, user.active, user.createTime, user.createBy,
                        user.updateTime, user.updateBy)
                      complete(OK, ResponseJson[User](getHeader(200, session), noPwdUser))
                    case None =>
                      riderLogger.info(s"user ${session.userId} select user $id where project id is $id success.")
                      complete(OK, ResponseJson[String](getHeader(200, session), ""))
                  }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select user $id where project id is $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }

  }

  def putRoute(route: String): Route = path(route / LongNumber) {
    id =>
      put {
        entity(as[User]) {
          user =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user")
                  complete(OK, getHeader(403, session))
                else {
                    val userEntity = User(user.id, user.email.trim, user.password.trim, user.name.trim, user.roleType.trim, user.preferredLanguage, user.active, user.createTime, user.createBy, currentSec, session.userId)
                    onComplete(userDal.updateWithoutPwd(userEntity)) {
                      case Success(_) =>
                        riderLogger.info(s"user ${
                          session.userId
                        } update user success.")
                        onComplete(userDal.getUserProject(_.id === id).mapTo[Seq[UserProject]]) {
                          case Success(userProject) =>
                            riderLogger.info(s"user ${
                              session.userId
                            } select user where id is ${
                              userEntity.id
                            } success.")
                            complete(OK, ResponseJson[UserProject](getHeader(200, session), userProject.head))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${
                              session.userId
                            } select user where id is ${
                              userEntity.id
                            } failed", ex)
                            complete(OK, getHeader(451, ex.toString, session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${
                          session.userId
                        } update user failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                }
            }
        }
      }
  }


}
