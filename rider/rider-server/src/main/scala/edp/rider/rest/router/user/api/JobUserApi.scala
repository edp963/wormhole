package edp.rider.rest.router.user.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.{AppInfo, RiderLogger}
import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.dal.{JobDal, ProjectDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.spark.SubmitSparkJob
import edp.rider.rest.util.JobUtils
import edp.rider.rest.util.AppUtils.prepare
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.{currentSec, maxTimeOut, minTimeOut}
import edp.rider.rest.util.JobUtils.startJob
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.rest.router.JsonProtocol._
import edp.rider.service.util.CacheMap

import scala.concurrent.Await
import scala.util.{Failure, Success}

class JobUserApi(jobDal: JobDal, projectDal: ProjectDal) extends BaseUserApiImpl[JobTable, Job](jobDal) with RiderLogger {
  def postRoute(route: String): Route = path(route / LongNumber / "jobs") {
    projectId =>
      post {
        entity(as[SimpleJob]) {
          simple =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                } else {
                  if (session.projectIdList.contains(projectId)) {
                    val jobInsert = Job(0, simple.name, projectId, simple.sourceNs, simple.sinkNs, simple.sourceType, simple.sparkConfig, simple.startConfig, simple.eventTsStart, simple.eventTsEnd,
                      simple.sourceConfig, simple.sinkConfig, simple.tranConfig, "new", Some(""), Some(SubmitSparkJob.getLogPath(simple.name)), null, null, currentSec, session.userId, currentSec, session.userId)
                    try {
                      onComplete(jobDal.insert(jobInsert)) {
                        case Success(job) =>
                          riderLogger.info(s"user ${session.userId} inserted job where project id is $projectId success.")
                          CacheMap.flowCacheMapRefresh
                          complete(OK, ResponseJson[Job](getHeader(200, session), job))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} refresh job where project id is $projectId failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } catch {
                      case ex: Exception =>
                        riderLogger.error(s"user ${session.userId} inserted job where project id is $projectId failed", ex)
                        if (ex.getMessage.contains("Duplicate entry"))
                          complete(OK, getHeader(409, "this source to sink already exists", session))
                        else
                          complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, getHeader(403, session))
                  }
                }
            }
        }
      }
  }


  def makeJob(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "start") {
    (projectId, jobId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(projectId)) {
                riderLogger.info("@@@@@@@@@@@@@@@@@@@@@@")
                val jobOut = Await.result(jobDal.findById(jobId), minTimeOut)
                jobOut match {
                  case Some(job) =>
                    val updateJob = Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.sourceType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd,
                      job.sourceConfig, job.sinkConfig, job.tranConfig, job.status, job.sparkAppid, job.logPath, Some(currentSec), job.stoppedTime, job.createTime, job.createBy, currentSec, session.userId)
                    Await.result(jobDal.update(updateJob), minTimeOut)
                    JobUtils.startJob(job)
                    val projectName = jobDal.adminGetRow(job.projectId)
                    complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(updateJob, projectName)))
                  case None => complete(OK, getHeader(200, session))
                }
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }

      }
  }


}
