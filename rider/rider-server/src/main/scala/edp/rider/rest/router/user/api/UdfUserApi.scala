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
import edp.rider.rest.persistence.dal.{RelProjectUdfDal, UdfDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
//import edp.rider.rest.router.JsonProtocol._

import scala.util.{Failure, Success}

class UdfUserApi(udfDal: UdfDal, relProjectUdfDal: RelProjectUdfDal) extends BaseUserApiImpl[UdfTable, Udf](udfDal) with RiderLogger with JsonSerializer {

  def getUdfByProjectId(route: String): Route = path(route / LongNumber / "udfs"/Segment) {
    (id, streamType) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(id)) {
                onComplete(relProjectUdfDal.getUdfByPIdSType(id, streamType).mapTo[Seq[Udf]]) {
                  case Success(udfs) =>
                    riderLogger.info(s"user ${session.userId} select udfs where project id is $id success.")
                    complete(OK, ResponseSeqJson[Udf](getHeader(200, session), udfs.sortBy(_.functionName)))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select udfs where project id is $id failed", ex)
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

  //  def registerUdfRoute(route: String): Route = path(route / LongNumber / "udfs" / LongNumber / "register") {
  //    (projectId, udfId) =>
  //      put {
  //        authenticateOAuth2Async("rider", AuthorizationProvider.authorize) {
  //          session =>
  //            if (session.roleType != "user") {
  //              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
  //              complete(OK, getHeader(403, session))
  //            } else {
  //              registerUdfResponse(projectId, udfId, session)
  //            }
  //        }
  //      }
  //  }
  //
  //  private def registerUdfResponse(projectId: Long, udfId: Long, session: SessionClass): Route = {
  //    if (session.projectIdList.contains(projectId)) {
  //
  //    } else {
  //      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
  //      complete(OK, getHeader(403, session))
  //    }
  //  }
}
