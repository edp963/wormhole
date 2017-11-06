package edp.rider.rest.router.user.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.dal.{JobDal, ProjectDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, SessionClass}
import edp.rider.spark.{SparkStatusQuery, SubmitSparkJob}
import edp.rider.rest.util.JobUtils
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import edp.rider.rest.util.JobUtils.killJob
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.rest.util.StreamUtils.genStreamNameByProjectName
import slick.jdbc.MySQLProfile.api._
import edp.rider.common.JobStatus
import edp.rider.rest.util.JobUtils.getDisableAction
import scala.concurrent.Await
import edp.rider.spark.SubmitSparkJob.runShellCommand
import scala.util.{Failure, Success}

class JobUserApi(jobDal: JobDal, projectDal: ProjectDal) extends BaseUserApiImpl[JobTable, Job](jobDal) with RiderLogger with JsonSerializer {
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
                    val projectName = jobDal.adminGetRow(projectId)
                    val jobInsert = Job(0, genStreamNameByProjectName(projectName, simple.name), projectId, simple.sourceNs, simple.sinkNs, simple.sourceType, simple.sparkConfig, simple.startConfig, simple.eventTsStart, simple.eventTsEnd,
                      simple.sourceConfig, simple.sinkConfig, simple.tranConfig, "new", Some(""), Some(SubmitSparkJob.getLogPath(simple.name)), null, null, currentSec, session.userId, currentSec, session.userId)
                    try {
                      onComplete(jobDal.insert(jobInsert)) {
                        case Success(job) =>
                          riderLogger.info(s"user ${session.userId} inserted job where project id is $projectId success.")
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


  def startJob(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "start") {
    (projectId, jobId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              if (session.projectIdList.contains(projectId)) {
                val jobOut = Await.result(jobDal.findById(jobId), minTimeOut) //todo update db before query for "status check"
                jobOut match {
                  case Some(job) =>
                    if (job.status != JobStatus.STARTING.toString &&
                      job.status != JobStatus.WAITING.toString &&
                      job.status != JobStatus.RUNNING.toString &&
                      job.status != JobStatus.STOPPING.toString) {
                      val updateJob = Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.sourceType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd,
                        job.sourceConfig, job.sinkConfig, job.tranConfig, JobStatus.STARTING.toString, job.sparkAppid, job.logPath, Some(currentSec), job.stoppedTime, job.createTime, job.createBy, currentSec, session.userId)
                      Await.result(jobDal.update(updateJob), minTimeOut)
                      JobUtils.startJob(job)
                      val projectName = jobDal.adminGetRow(job.projectId)
                      complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(updateJob, projectName, getDisableAction(JobStatus.jobStatus(job.status)))))
                    } else {
                      riderLogger.error(s"can not start ${job.id} at this moment, because the job is in ${job.status} status")
                      complete(OK, getHeader(403, session))
                    }
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


  override def getByIdRoute(route: String): Route = path(route / LongNumber / "jobs" / LongNumber) {
    (projectId, jobId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                riderLogger.info(s"user ${session.userId} refresh job.")
                val job = JobUtils.refreshJob(jobId)
                val projectName = jobDal.adminGetRow(job.projectId)
                complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(job, projectName, getDisableAction(JobStatus.jobStatus(job.status)))))
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }


  def getByFilterRoute(route: String): Route = path(route / LongNumber / "jobs" / "status") {
    projectId =>
      get {
        parameters('sourceNs.as[String].?, 'sinkNs.as[String].?, 'jobName.as[String].?) {
          (sourceNsOpt, sinkNsOpt, jobNameOpt) =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  if (session.projectIdList.contains(projectId)) {
                    (sourceNsOpt, sinkNsOpt, jobNameOpt) match {
                      case (Some(sourceNs), Some(sinkNs), None) =>
                        onComplete(jobDal.findByFilter(job => job.sourceNs === sourceNs && job.sinkNs === sinkNs)) {
                          case Success(jobs) =>
                            if (jobs.isEmpty) {
                              riderLogger.info(s"user ${session.userId} check job source namespace $sourceNs and sink namespace $sinkNs doesn't exist.")
                              complete(OK, getHeader(200, session))
                            }
                            else {
                              riderLogger.warn(s"user ${session.userId} check job source namespace $sourceNs and sink namespace $sinkNs already exists.")
                              complete(OK, getHeader(409, s"this source namespace $sourceNs and sink namespace $sinkNs already exists", session))
                            }
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} check job source namespace $sourceNs and sink namespace $sinkNs does exist failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case (None, None, None) =>
                        riderLogger.info(s"user ${session.userId} refresh project $projectId")
                        val jobs: Seq[Job] = jobDal.getAllJobs4Project(projectId)
                        if (jobs != null && jobs.nonEmpty) {
                          riderLogger.info(s"user ${session.userId} refresh project $projectId, and job in it is not null and not empty.")
                          val projectName = jobDal.adminGetRow(projectId)
                          val jobsNameSet = jobs.map(_.name).toSet
                          val jobList = jobs.filter(_.startedTime.isDefined)
                          val minStartTime = if (jobList.isEmpty) "" else jobList.map(_.startedTime.get).sorted.head //check null to option None todo
                          val allAppStatus = SparkStatusQuery.getAllAppStatus(minStartTime).filter(t => jobsNameSet.contains(t.appName))
                          val rst: Seq[FullJobInfo] = SparkStatusQuery.getSparkAllJobStatus(jobs, allAppStatus, projectName)
                          complete(OK, ResponseJson[Seq[FullJobInfo]](getHeader(200, session), rst.sortBy(_.job.id)))
                        } else {
                          riderLogger.info(s"user ${session.userId} refresh project $projectId, but no jobs in project.")
                          complete(OK, ResponseJson[Seq[FullJobInfo]](getHeader(200, session), Seq()))
                        }
                      case (None, None, Some(jobName)) =>
                        onComplete(jobDal.checkJobNameUnique(jobName)) {
                          case Success(jobs) =>
                            if (jobs.isEmpty) {
                              riderLogger.info(s"user ${session.userId} check job name $jobName doesn't exist success.")
                              complete(OK, ResponseJson[String](getHeader(200, session), jobName))
                            }
                            else {
                              riderLogger.warn(s"user ${session.userId} check job name $jobName already exists success.")
                              complete(OK, getHeader(409, s"$jobName already exists", session))
                            }
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} check job name $jobName does exist failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case (_, _, _) =>
                        riderLogger.error(s"user ${session.userId} request url is not supported.")
                        complete(OK, getHeader(501, session))
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


  //"/{projectId}/jobs/{jobId}/stop"
  def stopJob(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "stop") {
    (projectId, jobId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, null))
            } else {
              try {
                val project = Await.result(projectDal.findById(projectId), minTimeOut)
                if (project.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request to stop project $projectId, job $jobId, but the project $projectId doesn't exist.")
                  complete(OK, getHeader(403, s"project $projectId doesn't exist", null))
                }
                val job: Option[Job] = Await.result(jobDal.findById(jobId), minTimeOut)
                if (job.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request to stop project $projectId, job $jobId, but the job $jobId doesn't exist.")
                  complete(OK, getHeader(403, s"job $jobId doesn't exist", null))
                } else if (job.get.status == JobStatus.STARTING.toString ||
                  job.get.status == JobStatus.STOPPED.toString ||
                  job.get.status == JobStatus.DONE.toString ||
                  job.get.status == JobStatus.NEW.toString) {
                  riderLogger.warn(s"user ${session.userId} job $jobId status is ${job.get.status}, can't stop now.")
                  complete(OK, getHeader(403, s"job $jobId status is starting, can't stop now.", null))
                } else {
                  val status: String = killJob(jobId)
                  riderLogger.info(s"user ${session.userId} stop job $jobId success.")
                  val projectName = jobDal.adminGetRow(projectId)
                  val jobGet = job.get
                  val updateJob = Job(jobGet.id, jobGet.name, jobGet.projectId, jobGet.sourceNs, jobGet.sinkNs, jobGet.sourceType, jobGet.sparkConfig, jobGet.startConfig, jobGet.eventTsStart, jobGet.eventTsEnd,
                    jobGet.sourceConfig, jobGet.sinkConfig, jobGet.tranConfig, status, jobGet.sparkAppid, jobGet.logPath, jobGet.startedTime, jobGet.stoppedTime, jobGet.createTime, jobGet.createBy, jobGet.updateTime, jobGet.updateBy)
                  complete(OK, ResponseJson[FullJobInfo](getHeader(200, null), FullJobInfo(updateJob, projectName, getDisableAction(JobStatus.jobStatus(jobGet.status)))))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} stop job $jobId failed", ex)
                  complete(OK, getHeader(451, null))
              }
            }
        }
      }
  }

//{projectId}/jobs/{jobId}/delete

  def deleteJob(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "delete") {
    (projectId, jobId) =>
      put {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            if (session.projectIdList.contains(projectId)) {
              try {
                val jobOut = Await.result(jobDal.findById(jobId), minTimeOut)
                jobOut match {
                  case Some(job) =>
                    if (job.status == JobStatus.STARTING.toString) {
                      riderLogger.warn(s"user ${session.userId} job $jobId status is ${job.status}, can't stop now.")
                      complete(OK, getHeader(403, s"job $jobId status is starting, can't stop now.", null))
                    } else {
                      if (job.sparkAppid.getOrElse("") != "") {
                        runShellCommand("yarn application -kill " + job.sparkAppid.get)
                        riderLogger.info(s"user ${session.userId} stop job ${jobId} success")
                      }

                      onComplete(jobDal.deleteById(jobId)) {
                        case Success(_) =>
                          riderLogger.info(s"user ${session.userId} delete job ${jobId} where project id is $projectId success.")
                          complete(OK, getHeader(200, session))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} delete job ${jobId} where project id is $projectId failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    }
                  case None => complete(OK, getHeader(200, session))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"delete job ${jobId} failed", ex)
                  complete(OK, getHeader(451, null))
              }
            } else {
              riderLogger.error(s"user ${session.userId} doesn't have permission to access the project ${projectId}.")
              complete(OK, getHeader(403, session))
            }
        }
      }
  }


}
