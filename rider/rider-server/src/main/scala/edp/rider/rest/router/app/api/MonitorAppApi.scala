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
import edp.rider.common.RiderLogger
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseJson, SessionClass}
import edp.rider.rest.util.{AuthorizationProvider, StreamUtils}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.JobUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.service.util.FeedbackOffsetUtil
import edp.wormhole.common.util.JsonUtils
import edp.rider.rest.router.JsonProtocol._
import edp.wormhole.common.util.DateUtils._

import scala.concurrent.Await
import scala.util.{Failure, Success}

class MonitorAppApi(flowDal: FlowDal, projectDal: ProjectDal, streamDal: StreamDal, jobDal: JobDal, feedbackFlowErrDal: FeedbackFlowErrDal, feedbackOffsetDal: FeedbackOffsetDal) extends BaseAppApiImpl(flowDal) with RiderLogger {

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
              val streamInfo = streamDal.getStreamDetail(Some(projectId), Some(streamId))
              if (streamInfo.isEmpty) {
                riderLogger.error(s"user ${session.userId} request to get flow $flowId health information, but the stream $streamId doesn't exist")
                complete(OK, getHeader(403, s"stream $streamId doesn't exist", null))
              }
              onComplete(flowDal.getById(projectId, flowId).mapTo[Option[FlowStreamInfo]]) {
                case Success(flowStreamOpt) =>
                  riderLogger.info(s"user ${session.userId} select flow where project id is $projectId and flow id is $flowId success.")
                  flowStreamOpt match {
                    case Some(flowStream) =>
                      try {
                        flowDal.updateFlowStatus(flowStream.id, flowStream.status)
                        val maxWatermark =
                          Await.result(feedbackFlowErrDal.getSinkErrorMaxWatermark(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse("")
                        val minWatermark = Await.result(feedbackFlowErrDal.getSinkErrorMinWatermark(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse("")
                        val errorCount = Await.result(feedbackFlowErrDal.getSinkErrorCount(streamId, flowStream.sourceNs, flowStream.sinkNs), maxTimeOut).getOrElse(0L)
                        val fLatestWatermark = ElasticSearch.queryESFlowMax(projectId, streamId, flowId, "dataGeneratedTs")._2
                        val hdfsFlow = flowDal.getByNsOnly(flowStream.sourceNs, flowStream.sourceNs)
                        val hdfsLatestWatermark =
                          if (hdfsFlow.isEmpty) ""
                          else {
                            val hdfsStream = Await.result(streamDal.findById(hdfsFlow.head.streamId), minTimeOut)
                            val streamDuration =
                              if (hdfsStream.nonEmpty) StreamUtils.getDuration(hdfsStream.head.launchConfig)
                              else 10
                            val esFlowTs = ElasticSearch.queryESFlowMax(projectId, hdfsFlow.head.streamId, hdfsFlow.head.id, "dataGeneratedTs")._2
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
                            case Some(project) =>
                              try {
                                val streamInfo = streamDal.getStreamDetail(Some(projectId), Some(streamId)).head.stream
                                val sparkApplicationId = streamInfo.sparkAppid.get
                                val sLatestWatermark = ElasticSearch.queryESStreamMax(projectId, streamId, "dataGeneratedTs")._2
                                val launchConfig = JsonUtils.json2caseClass[LaunchConfig](stream.launchConfig)
                                val batchThreshold = launchConfig.maxRecords.toInt
                                val batchDuration = launchConfig.durations.toInt
                                onComplete(feedbackOffsetDal.getDistinctStreamTopicList(streamId)) {
                                  case Success(topics) =>
                                    val topicList: Seq[TopicOffset] = FeedbackOffsetUtil.getLatestTopicOffset(topics).map(
                                      topic => TopicOffset(topic.topicName, topic.partitionOffsets)
                                    )
                                    riderLogger.info(s"user ${session.userId} request for stream $streamId health where project id is $projectId success")
                                    complete(OK, ResponseJson[StreamHealth](getHeader(200, null), StreamHealth(streamInfo.status, sparkApplicationId, formatWaterMark(sLatestWatermark), batchThreshold, batchDuration, topicList)))
                                  case Failure(ex) =>
                                    riderLogger.error(s"user ${session.userId} request for stream $streamId health where project id is $projectId failed", ex)
                                    complete(OK, ResponseJson[StreamHealth](getHeader(200, null), StreamHealth(streamInfo.status, sparkApplicationId, formatWaterMark(sLatestWatermark), batchThreshold, batchDuration, Seq(TopicOffset("", "")))))
                                }
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

  def formatWaterMark(waterMark: String) = {
    if (waterMark == "") "" else yyyyMMddHHmmss(waterMark)
  }
}
