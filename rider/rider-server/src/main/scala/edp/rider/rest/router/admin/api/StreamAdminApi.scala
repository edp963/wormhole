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
import akka.http.scaladsl.server._
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.dal.StreamDal
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.spark.SparkJobClientLog
import edp.rider.rest.router.JsonProtocol._
import scala.util.{Failure, Success}

class StreamAdminApi(streamDal: StreamDal) extends BaseAdminApiImpl(streamDal) with RiderLogger {
  override def getByAllRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(Forbidden, getHeader(403, session))
              }
              else {
                onComplete(streamDal.adminGetAll.mapTo[Seq[StreamAdmin]]) {
                  case Success(streamAdmins) =>
                    riderLogger.info(s"stream admin response data size: ${streamAdmins.size}")
                    riderLogger.info(s"user ${session.userId} select all $route success.")
                    complete(OK, ResponseSeqJson[StreamAdmin](getHeader(200, session), streamAdmins.sortBy(_.stream.id)))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select all $route failed", ex)
                    complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }


  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "streams") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(Forbidden, getHeader(403, session))
            }
            else {
              returnStreamRes(id, None, session)
            }
        }
      }

  }

  def returnStreamRes(projectId: Long, streamId: Option[Long], session: SessionClass) = {
    onComplete(streamDal.getStreamsByProjectId(Some(projectId), streamId).mapTo[Seq[StreamSeqTopic]]) {
      case Success(streams) =>
        val allStreams: Seq[(Stream, StreamSeqTopic)] = streamDal.getUpdateStream(streams)
        val realReturns = allStreams.map(stream => stream._2)
        val realRes = realReturns.map(returnStream => streamDal.getReturnRes(returnStream))
        riderLogger.info(s"user ${session.userId} updated streams after refresh the yarn/spark rest api or log success.")
        complete(OK, ResponseSeqJson[StreamSeqTopicActions](getHeader(200, session), realRes.sortBy(_.stream.id)))
      case Failure(ex) => complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
    }
  }

  def getResourceByProjectIdRoute(route: String): Route = path(route / LongNumber / "resources") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(Forbidden, getHeader(403, session))
            }
            else {
              onComplete(streamDal.getResource(id).mapTo[Resource]) {
                case Success(resources) =>
                  riderLogger.info(s"user ${session.userId} select all resources success where project id is $id.")
                  complete(OK, ResponseJson[Resource](getHeader(200, session), resources))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select all resources failed where project id is $id", ex)
                  complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  def getLogByStreamId(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "logs") {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(Forbidden, getHeader(403, session))
            }
            else {
              onComplete(streamDal.getStreamNameByStreamID(streamId).mapTo[Stream]) {
                case Success(stream) =>
                  riderLogger.info(s"user ${session.userId} refresh stream log where stream id is $streamId success.")
                  val log = SparkJobClientLog.getLogByAppName(stream.name)
                  complete(OK, ResponseJson[String](getHeader(200, session), log))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} refresh stream log where stream id is $streamId failed", ex)
                  complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }
}
