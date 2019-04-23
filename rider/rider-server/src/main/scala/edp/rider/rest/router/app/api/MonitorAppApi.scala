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


package edp.rider.rest.router.app.api

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, SessionClass}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.JobUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.{AuthorizationProvider, StreamUtils}
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.JsonUtils

import scala.concurrent.Await
import scala.util.{Failure, Success}

class MonitorAppApi(flowDal: FlowDal, projectDal: ProjectDal, streamDal: StreamDal, jobDal: JobDal, feedbackErrDal: FeedbackErrDal, feedbackOffsetDal: FeedbackOffsetDal, monitorInfoDal: MonitorInfoDal) extends BaseAppApiImpl(flowDal) with RiderLogger with JsonSerializer {

  def getFlowHealthByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "flows" / LongNumber / "health") {
    (projectId, streamId, flowId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "app") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, null))
            } else {
              val project = Await.result(projectDal.findById(projectId), minTimeOut)
              if (project.isEmpty) {
                riderLogger.error(s"user ${session.userId} request to get project $projectId, stream $streamId, flow $flowId health information, but the project $projectId doesn't exist.")
                complete(OK, getHeader(403, s"project $projectId doesn't exist", null))
              }
              val streamInfo = Await.result(streamDal.findById(streamId), minTimeOut)
              if (streamInfo.isEmpty) {
                riderLogger.error(s"user ${session.userId} request to get flow $flowId health information, but the stream $streamId doesn't exist")
                complete(OK, getHeader(403, s"stream $streamId doesn't exist", null))
              }
              onComplete(flowDal.getById(flowId).mapTo[Option[FlowStreamInfo]]) {
                case Success(flowStreamOpt) =>
                  riderLogger.info(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId success.")
                  flowStreamOpt match {
                    case Some(flowStream) =>
                      try {
                        flowDal.updateStatusByFeedback(flowStream.id, flowStream.status)
                        val maxWatermark =
                          Await.result(feedbackErrDal.getSinkErrorMaxWatermark(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse("")
                        val minWatermark = Await.result(feedbackErrDal.getSinkErrorMinWatermark(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse("")
                        val errorCount = Await.result(feedbackErrDal.getSinkErrorCount(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse(0L)
                        val fLatestWatermark = if (RiderConfig.monitor.databaseType.trim.equalsIgnoreCase("es")) ElasticSearch.queryESFlowMax(projectId, streamId, flowId, "dataGeneratedTs")._2
                        else Await.result(monitorInfoDal.queryESFlowLastestTs(projectId, streamId, flowId), maxTimeOut).getOrElse("")
                        val hdfsFlow = flowDal.getByNsOnly(flowStream.sourceNs, flowStream.sourceNs)
                        val hdfsLatestWatermark =
                          if (hdfsFlow.isEmpty) ""
                          else {
                            val hdfsStream = Await.result(streamDal.findById(hdfsFlow.head.streamId), minTimeOut)
                            val streamDuration =
                              if (hdfsStream.nonEmpty) StreamUtils.getDuration(hdfsStream.head.launchConfig)
                              else 10
                            val esFlowTs = if (RiderConfig.monitor.databaseType.trim.equalsIgnoreCase("es")) ElasticSearch.queryESFlowMax(projectId, hdfsFlow.head.streamId, hdfsFlow.head.id, "dataGeneratedTs")._2
                            else Await.result(monitorInfoDal.queryESFlowLastestTs(projectId, hdfsFlow.head.streamId, hdfsFlow.head.id), maxTimeOut).getOrElse("")
                            yyyyMMddHHmmss(dt2long(esFlowTs) - streamDuration * 1000 * 1000)
                          }
                        riderLogger.error(s"user ${session.userId} request for flow $flowId health where project id is $projectId success")
                        complete(OK, ResponseJson[FlowHealth](getHeader(200, null),
                          FlowHealth(flowStream.status, formatWaterMark(fLatestWatermark),
                            formatWaterMark(hdfsLatestWatermark),
                            formatWaterMark(minWatermark),
                            formatWaterMark(maxWatermark),
                            errorCount)))
                      } catch {
                        case ex: Exception =>
                          riderLogger.error(s"user ${session.userId} request for flow $flowId health where project id is $projectId failed", ex)
                          complete(OK, getHeader(451, null))
                      }
                    case None => complete(OK, getHeader(404, "this flow doesn't exist", null))
                  }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} request for flow $flowId health where project id is $projectId failed", ex)
                  complete(OK, getHeader(451, null))
              }
            }
        }
      }
  }

  def getStreamHealthByIdRoute(route: String): Route = path(route / LongNumber / "streams" / LongNumber / "health") {
    (projectId, streamId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "app") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, null))
            } else {
              onComplete(streamDal.findById(streamId).mapTo[Option[Stream]]) {
                case Success(streamOpt) =>
                  streamOpt match {
                    case Some(stream) =>
                      onComplete(projectDal.findById(projectId).mapTo[Option[Project]]) {
                        case Success(projectOpt) =>
                          projectOpt match {
                            case Some(_) =>
                              try {
                                val streamInfo = Await.result(streamDal.findById(streamId), minTimeOut).head
                                val sparkApplicationId = streamInfo.sparkAppid.get
                                val sLatestWatermark = if (RiderConfig.monitor.databaseType.trim.equalsIgnoreCase("es")) ElasticSearch.queryESStreamMax(projectId, streamId, "dataGeneratedTs")._2
                                else Await.result(monitorInfoDal.queryESStreamLastestTs(projectId, streamId), minTimeOut).getOrElse("")
                                val launchConfig = JsonUtils.json2caseClass[LaunchConfig](stream.launchConfig)
                                val batchThreshold = launchConfig.maxRecords.toInt
                                val batchDuration = launchConfig.durations.toInt
                                val topics = streamDal.getStreamTopicsMap(streamId, streamInfo.name)
                                val topicOffset = (topics.autoRegisteredTopics ++: topics.userDefinedTopics).map(
                                  topic =>
                                    TopicOffset(topic.name, topic.consumedLatestOffset, topic.kafkaEarliestOffset, topic.kafkaLatestOffset))
                                complete(OK, ResponseJson[StreamHealth](getHeader(200, null),
                                  StreamHealth(streamInfo.status, sparkApplicationId,
                                    formatWaterMark(sLatestWatermark), batchThreshold, batchDuration, topicOffset)))
                              } catch {
                                case ex: Exception =>
                                  riderLogger.error(s"user ${session.userId} request for stream $streamId health where project id is $projectId failed", ex)
                                  complete(OK, getHeader(451, null))
                              }
                            case None =>
                              riderLogger.error(s"user ${session.userId} request for stream $streamId health where project id is $projectId failed, project doesn't exist")
                              complete(OK, getHeader(404, s"this project doesn't exist", null))
                          }
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} select project where project id is $projectId failed", ex)
                          complete(OK, getHeader(451, null))
                      }
                    case None =>
                      riderLogger.error(s"user ${session.userId} request for stream $streamId health where project id is $projectId failed, stream doesn't exist")
                      complete(OK, getHeader(404, "this stream doesn't exist", null))
                  }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} request for stream $streamId health where project id is $projectId failed", ex)
                  complete(OK, getHeader(451, null))
              }
            }
        }
      }
  }

  def getJobHealthByIdRoute(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "health") {
    (projectId, jobId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "app") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, null))
            } else {
              val project = Await.result(projectDal.findById(projectId), minTimeOut)
              if (project.isEmpty) {
                riderLogger.error(s"user ${session.userId} request to get project $projectId, job $jobId health information, but the project $projectId doesn't exist.")
                complete(OK, getHeader(403, s"project $projectId doesn't exist", null))
              }
              onComplete(jobDal.findById(jobId).mapTo[Option[Job]]) {
                case Success(jobOpt) => jobOpt match {
                  case Some(job) =>
                    refreshJob(job.id)
                    riderLogger.info(s"user ${session.userId} request for job $jobId where project id is $projectId success")
                    complete(OK, ResponseJson[JobHealth](getHeader(200, null), JobHealth(job.status, job.sparkAppid.getOrElse(""))))
                  case None =>
                    riderLogger.error(s"user ${session.userId} request for job $jobId where project id is $projectId failed, job doesn't exist")
                    complete(OK, getHeader(404, "this job doesn't exist", null))
                }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} request for job $jobId where project id is $projectId failed", ex)
                  complete(OK, getHeader(451, null))
              }
            }
        }
      }
  }

  def formatWaterMark(waterMark: String): String = {
    if (waterMark == "") "" else yyyyMMddHHmmss(waterMark)
  }
}
