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
import edp.rider.rest.persistence.dal.{JobDal, ProjectDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.minTimeOut
import edp.rider.rest.util.ResponseUtils._
import edp.rider.yarn.YarnClientLog

import scala.concurrent.Await
import scala.util.{Failure, Success}

class StreamAdminApi(streamDal: StreamDal, projectDal:ProjectDal, jobDal:JobDal) extends BaseAdminApiImpl(streamDal) with RiderLogger with JsonSerializer {
  override def getByAllRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                //complete(OK, getHeader(403, session))
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
              else {
                val streams = streamDal.getBriefDetail()
                riderLogger.info(s"user ${session.userId} select all streams success.")
                complete(OK, ResponseSeqJson[StreamDetail](getHeader(200, session), streams))
              }
          }
      }
    }

  }

  def getDetailRoute(route: String): Route = path(route / "detail") {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                //complete(OK, getHeader(403, session))
                complete(OK, setFailedResponse(session, "Insufficient Permission"))
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
              //complete(OK, getHeader(403, session))
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
              val streams = streamDal.getBriefDetail(Some(id))
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
              //complete(OK, getHeader(403, session))
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
                try {
                  val project: Project = Await.result(projectDal.findById(id), minTimeOut).head
                  val (projectTotalCore, projectTotalMemory) = (project.resCores, project.resMemoryG)
                  val (jobUsedCore, jobUsedMemory, jobSeq) = jobDal.getProjectJobsUsedResource(id)
                  val (streamUsedCore, streamUsedMemory, streamSeq) = streamDal.getProjectStreamsUsedResource(id)
                  val appResources = jobSeq ++ streamSeq
                  val resources = Resource(projectTotalCore, projectTotalMemory, projectTotalCore - jobUsedCore - streamUsedCore, projectTotalMemory - jobUsedMemory - streamUsedMemory, appResources)
                  riderLogger.info(s"user ${session.userId} select all resources success where project id is $id.")
                  complete(OK, ResponseJson[Resource](getHeader(200, session), resources))
                } catch {
                  case ex: Exception =>
                    riderLogger.error(s"user ${session.userId} get resources for project ${id}  failed", ex)
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
              //complete(OK, getHeader(403, session))
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
              onComplete(streamDal.getStreamNameByStreamID(streamId).mapTo[Stream]) {
                case Success(stream) =>
                  riderLogger.info(s"user ${session.userId} refresh stream log where stream id is $streamId success.")
                  val log = YarnClientLog.getLogByAppName(stream.name, stream.logPath.getOrElse(""))
                  complete(OK, ResponseJson[String](getHeader(200, session), log))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} refresh stream log where stream id is $streamId failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  override def getByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber) {
    (id, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              //complete(OK, getHeader(403, session))
              complete(OK, setFailedResponse(session, "Insufficient Permission"))
            }
            else {
               if(session.projectIdList.contains(id)){
                 val stream = streamDal.getStreamDetail(Some(id), Some(Seq(streamId))).head
                 riderLogger.info(s"user ${session.userId} select streams where project id is $id success.")
                 complete(OK, ResponseJson[StreamDetail](getHeader(200, session), stream))
               }
              else {
                 riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $id.")
                 //complete(OK, getHeader(403, session))
                 complete(OK, setFailedResponse(session, "Insufficient Permission"))
              }
            }
        }
      }
  }

}
