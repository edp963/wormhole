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
import edp.rider.rest.persistence.dal.NsDatabaseDal
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._

import scala.util.{Failure, Success}
import slick.jdbc.MySQLProfile.api._

class NsDatabaseUserApi(dbDal: NsDatabaseDal)
  extends BaseUserApiImpl[NsDatabaseTable, NsDatabase](dbDal) with RiderLogger {

  def getByFilterRoute(route: String): Route = path(route / LongNumber / "instances" / LongNumber / "databases") {
    (id, instanceId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                onComplete(dbDal.findByFilter(db => db.active === true && db.nsInstanceId === instanceId).mapTo[Seq[NsDatabase]]) {
                  case Success(dbs) =>
                    riderLogger.info(s"user ${session.userId} select databases where instance id is $instanceId success.")
                    complete(OK, ResponseSeqJson[NsDatabase](getHeader(200, session), dbs.sortBy(_.nsDatabase)))
                  case Failure(ex) =>
                    riderLogger.info(s"user ${session.userId} select databases where instance id is $instanceId failed", ex)
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
