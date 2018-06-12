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
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.dal.{RelProjectUserDal, UserDal}
import edp.rider.rest.persistence.entities._
//import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router._
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.util.{Failure, Success}


class UserAdminApi(userDal: UserDal, relProjectUserDal: RelProjectUserDal) extends BaseAdminApiImpl(userDal) with RiderLogger with JsonSerializer {

  def getById(route: String): Route = path(route / LongNumber) {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(userDal.getUserProject(_.id === id).mapTo[Seq[UserProject]]) {
                case Success(userProjectSeq) =>
                  if (userProjectSeq.nonEmpty) {
                    riderLogger.info(s"user ${session.userId} select user $id success.")
                    complete(OK, ResponseJson[UserProject](getHeader(200, session), userProjectSeq.head))
                  } else {
                    riderLogger.info(s"user ${session.userId} select user $id success, but it doesn't exist.")
                    complete(OK, ResponseJson[String](getHeader(200, session), ""))
                  }
                case Failure(ex) =>
                  riderLogger.error(s"user ${
                    session.userId
                  } select user $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  def getByFilterRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?, 'email.as[String].?) {
        (visible, email) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${
                  session.userId
                } has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                (visible, email) match {
                  case (_, Some(userEmail)) =>
                    onComplete(userDal.findByFilter(_.email === email).mapTo[Seq[User]]) {
                      case Success(users) =>
                        if (users.isEmpty) {
                          riderLogger.info(s"user ${
                            session.userId
                          } check email $userEmail doesn't exist.")
                          complete(OK, getHeader(200, session))
                        }
                        else {
                          riderLogger.warn(s"user ${
                            session.userId
                          } check email $userEmail already exists.")
                          complete(OK, getHeader(409, s"$email already exists", session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${
                          session.userId
                        } check email $userEmail does exist failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (_, None) =>
                    onComplete(userDal.getUserProject(_.active === visible.getOrElse(true)).mapTo[Seq[UserProject]]) {
                      case Success(userProjects) =>
                        riderLogger.info(s"user ${
                          session.userId
                        } select all $route success.")
                        complete(OK, ResponseSeqJson[UserProject](getHeader(200, session), userProjects))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${
                          session.userId
                        } select all $route failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (_, _) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } request url is not supported.")
                    complete(OK, ResponseJson[String](getHeader(403, session), msgMap(403)))
                }
              }
          }
      }
    }

  }

  def postRoute(route: String): Route = path(route) {
    post {
      entity(as[SimpleUser]) {
        simple =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${
                  session.userId
                } has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val user = User(0, simple.email.trim, simple.password.trim, simple.name.trim, simple.roleType.trim, RiderConfig.riderServer.defaultLanguage, active = true, currentSec, session.userId, currentSec, session.userId)
                onComplete(userDal.insert(user).mapTo[User]) {
                  case Success(row) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } insert user success.")
                    onComplete(userDal.getUserProject(_.id === row.id).mapTo[Seq[UserProject]]) {
                      case Success(userProject) =>
                        riderLogger.info(s"user ${
                          session.userId
                        } select user where id is ${
                          row.id
                        } success.")
                        complete(OK, ResponseJson[UserProject](getHeader(200, session), userProject.head))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${
                          session.userId
                        } select user where id is ${
                          row.id
                        } failed", ex)
                        complete(OK, getHeader(451, ex.toString, session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${
                      session.userId
                    } insert user failed", ex)
                    if (ex.toString.contains("Duplicate entry"))
                      complete(OK, getHeader(409, s"${
                        simple.email
                      } already exists", session))
                    else
                      complete(OK, getHeader(451, ex.toString, session))
                }
              }
          }
      }
    }

  }


  def putRoute(route: String): Route = path(route) {
    put {
      entity(as[User]) {
        user =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin")
                complete(OK, getHeader(403, session))
              else {
                val userEntity = User(user.id, user.email.trim, user.password.trim, user.name.trim, user.roleType.trim, user.preferredLanguage, user.active, user.createTime, user.createBy, currentSec, session.userId)
                onComplete(userDal.updateWithoutPwd(userEntity)) {
                  case Success(result) =>
                    riderLogger.info(s"user ${
                      session.userId
                    } update user success.")
                    onComplete(userDal.getUserProject(_.id === userEntity.id).mapTo[Seq[UserProject]]) {
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

  def getNormalUserRoute(route: String): Route = path(route / "users") {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin") {
            riderLogger.warn(s"${
              session.userId
            } has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            onComplete(userDal.findByFilter(user => user.active === true && user.roleType =!= "admin").mapTo[Seq[User]]) {
              case Success(users) =>
                riderLogger.info(s"user ${
                  session.userId
                } select users where active is true and roleType is user success.")
                val noPwdUsers = users.map {
                  user =>
                    User(user.id, user.email, "", user.name, user.roleType, user.preferredLanguage, user.active, user.createTime, user.createBy,
                      user.updateTime, user.updateBy)
                }
                complete(OK, ResponseSeqJson[User](getHeader(200, session), noPwdUsers.sortBy(_.email)))
              case Failure(ex) =>
                riderLogger.error(s"user ${
                  session.userId
                } select users where active is true and roleType is user failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }
          }
      }
    }

  }

  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "users") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(relProjectUserDal.getUserByProjectId(id).mapTo[Seq[User]]) {
                case Success(users) =>
                  riderLogger.info(s"user ${
                    session.userId
                  } select all users where project id is $id success.")
                  complete(OK, ResponseSeqJson[User](getHeader(200, session), users.sortBy(_.email)))
                case Failure(ex) =>
                  riderLogger.error(s"user ${
                    session.userId
                  } select all users where project id is $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  override def deleteRoute(route: String): Route = path(route / LongNumber) {
    id =>
      delete {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              try {
                val result = userDal.delete(id)
                if (result._1) {
                  riderLogger.error(s"user ${session.userId} delete user $id success.")
                  complete(OK, getHeader(200, session))
                }
                else complete(OK, getHeader(412, result._2, session))
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} delete user $id failed", ex)
                  complete(OK, getHeader(451, session))
              }
            }
        }
      }
  }
}
