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
import edp.rider.rest.persistence.dal.{NamespaceDal, RelProjectNsDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._

import scala.util.{Failure, Success}

class NamespaceUserApi(namespaceDal: NamespaceDal, relProjectNsDal: RelProjectNsDal)
  extends BaseUserApiImpl[NamespaceTable, Namespace](namespaceDal) with RiderLogger {

  def getNsByProjectId(route: String): Route = path(route / LongNumber / "namespaces") {
    id =>
      get {
        parameter('sourceType.as[String].?, 'sinkType.as[String].?, 'transType.as[String].?) {
          (sourceType, sinkType, transType) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(id)) {
                    onComplete(relProjectNsDal.getNsByProjectId(Some(id)).mapTo[Seq[NamespaceTopic]]) {
                      case Success(nsSeq) =>
                        riderLogger.info(s"user ${session.userId} select namespaces where project id is $id success.")
                        complete(OK, ResponseSeqJson[NamespaceTopic](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.permission))))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select namespaces where project id is $id failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  }
                  else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                    complete(OK, getHeader(501, session))
                  }
                }
            }
        }
      }
  }

  def FilterNsByProjectId(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "namespaces") {
    (projectId, streamId) =>
      get {
        parameter('sourceType.as[String].?, 'sinkType.as[String].?, 'transType.as[String].?) {
          (sourceType, sinkType, transType) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    (sourceType, sinkType, transType) match {
                      case (Some(source), None, None) =>
                        onComplete(relProjectNsDal.getSourceNamespaceByProjectId(projectId, streamId, source).mapTo[Seq[Namespace]]) {
                          case Success(nsSeq) =>
                            riderLogger.info(s"user ${session.userId} select namespaces where project id is $projectId, stream id is $streamId and nsSys is $source success.")
                            complete(OK, ResponseSeqJson[Namespace](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.permission))))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} select namespaces where project id is $projectId and nsSys is $source failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case (None, Some(sink), None) =>
                        onComplete(relProjectNsDal.getSinkNamespaceByProjectId(projectId, sink).mapTo[Seq[Namespace]]) {
                          case Success(nsSeq) =>
                            riderLogger.info(s"user ${session.userId} select namespaces where project id is $projectId and nsSys is $sink success.")
                            complete(OK, ResponseSeqJson[Namespace](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.permission))))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} select namespaces where project id is $projectId and nsSys is $sink failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case (None, None, Some(trans)) =>
                        onComplete(relProjectNsDal.getTransNamespaceByProjectId(projectId, trans).mapTo[Seq[TransNamespace]]) {
                          case Success(nsSeq) =>
                            riderLogger.info(s"user ${session.userId} select namespaces where project id is $projectId and nsSys is $trans success.")
                            complete(OK, ResponseSeqJson[TransNamespace](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase))))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} select namespaces where project id is $projectId and nsSys is $trans failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case (_, _, _) =>
                        riderLogger.error(s"user ${session.userId} request url is not supported.")
                        complete(OK, getHeader(404, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, getHeader(501, session))
                  }
                }
            }
        }
      }

  }

}
