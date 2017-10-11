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
import edp.rider.rest.router.{ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._

import scala.util.{Failure, Success}

class UserApi(userDal: UserDal, relProjectUserDal: RelProjectUserDal) extends BaseUserApiImpl[UserTable, User](userDal) with RiderLogger {

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

}
