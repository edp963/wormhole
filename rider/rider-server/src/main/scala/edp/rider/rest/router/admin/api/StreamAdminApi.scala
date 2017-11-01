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
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils._
import edp.rider.spark.SparkJobClientLog

import scala.util.{Failure, Success}

class StreamAdminApi(streamDal: StreamDal) extends BaseAdminApiImpl(streamDal) with RiderLogger with JsonSerializer {
  override def getByAllRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val streams = streamDal.getStreamDetail()
                riderLogger.info(s"user ${session.userId} select all streams success.")
                complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
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
              complete(OK, getHeader(403, session))
            }
            else {
              val streams = streamDal.getStreamDetail(Some(id))
              riderLogger.info(s"user ${session.userId} select all streams success.")
              complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
            }
        }
      }

  }

  def getResourceByProjectIdRoute(route: String): Route = path(route / LongNumber / "resources") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(streamDal.getResource(id).mapTo[Resource]) {
                case Success(resources) =>
                  riderLogger.info(s"user ${session.userId} select all resources success where project id is $id.")
                  complete(OK, ResponseJson[Resource](getHeader(200, session), resources))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select all resources failed where project id is $id", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
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
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(streamDal.getStreamNameByStreamID(streamId).mapTo[Stream]) {
                case Success(stream) =>
                  riderLogger.info(s"user ${session.userId} refresh stream log where stream id is $streamId success.")
                  val log = SparkJobClientLog.getLogByAppName(stream.name)
                  complete(OK, ResponseJson[String](getHeader(200, session), log))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} refresh stream log where stream id is $streamId failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

}
