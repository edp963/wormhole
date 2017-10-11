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
import edp.rider.rest.persistence.dal.{ProjectDal, RelProjectUserDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._

import scala.util.{Failure, Success}

class ProjectUserApi(projectDal: ProjectDal, relProjectUserDal: RelProjectUserDal) extends BaseUserApiImpl[ProjectTable, Project](projectDal) with RiderLogger {

  override def getByIdRoute(route: String): Route = path(route / LongNumber) {
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
                onComplete(projectDal.findById(id).mapTo[Option[Project]]) {
                  case Success(projectOpt) => projectOpt match {
                    case Some(project) =>
                      riderLogger.info(s"user ${session.userId} select $route by $id success.")
                      complete(OK, ResponseJson[Project](getHeader(200, session), project))
                    case None =>
                      riderLogger.warn(s"user ${session.userId} select $route by $id, it doesn't exist.")
                      complete(OK, ResponseJson[String](getHeader(200, session), ""))
                  }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select $route by $id failed", ex)
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


  override def getByAllRoute(route: String): Route = path(route) {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "user") {
            riderLogger.warn(s"user ${session.userId} has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            onComplete(relProjectUserDal.getProjectByUserId(session.userId).mapTo[Seq[Project]]) {
              case Success(projects) =>
                riderLogger.info(s"user ${session.userId} select $route where user id is ${session.userId} success.")
                complete(OK, ResponseSeqJson[Project](getHeader(200, session), projects.sortBy(_.name)))
              case Failure(ex) =>
                riderLogger.info(s"user ${session.userId} select $route where user id is ${session.userId} failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }
          }
      }
    }
   
  }


}
