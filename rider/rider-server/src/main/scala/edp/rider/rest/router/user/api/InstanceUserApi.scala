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
import edp.rider.rest.persistence.dal.RelProjectNsDal
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.router.JsonProtocol._

import scala.util.{Failure, Success}

class InstanceUserApi(relProjectNsDal: RelProjectNsDal)
  extends BaseUserApiImpl[RelProjectNsTable, RelProjectNs](relProjectNsDal) with RiderLogger {

  def getByFilterRoute(route: String): Route = path(route / LongNumber / "instances") {
    id =>
      get {
        parameter('nsSys.as[String]) {
          nsSys =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    onComplete(relProjectNsDal.getInstanceByProjectId(id, nsSys).mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        riderLogger.info(s"user ${session.userId} select instances where nsSys is kafka and project id is $id success .")
                        complete(OK, ResponseSeqJson[Instance](getHeader(200, session), instances.sortBy(_.nsInstance)))
                      case Failure(ex) =>
                        riderLogger.info(s"user ${session.userId} select instances where nsSys is kafka and project id is $id failed", ex)
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

}
