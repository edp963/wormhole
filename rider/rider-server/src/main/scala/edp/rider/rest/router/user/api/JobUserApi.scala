package edp.rider.rest.router.user.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.{JobStatus, RiderLogger}
import edp.rider.rest.persistence.dal.{JobDal, ProjectDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, SessionClass}
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import edp.rider.rest.util.JobUtils.{getDisableAction, killJob}
import edp.rider.rest.util.ResponseUtils.{getHeader, _}
import edp.rider.rest.util.StreamUtils.genStreamNameByProjectName
import edp.rider.rest.util.{AuthorizationProvider, JobUtils, NamespaceUtils, StreamUtils}
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.yarn.YarnClientLog
import edp.wormhole.util.JsonUtils
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}

class JobUserApi(jobDal: JobDal, projectDal: ProjectDal, streamDal: StreamDal) extends BaseUserApiImpl[JobTable, Job](jobDal) with RiderLogger with JsonSerializer {
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
                    val jobInsert = Job(0, genStreamNameByProjectName(projectName, simple.name), projectId, simple.sourceNs, JobUtils.getJobSinkNs(simple.sourceNs, simple.sinkNs, simple.jobType), simple.jobType, simple.sparkConfig, simple.startConfig, simple.eventTsStart, simple.eventTsEnd,
                      simple.sourceConfig, simple.sinkConfig, simple.tranConfig, simple.tableKeys, simple.desc, "new", None, Some(""), None, None, UserTimeInfo(currentSec, session.userId, currentSec, session.userId))
                    riderLogger.info(s"user ${session.userId} inserted job where project id is $projectId . insertdata is $jobInsert")
                    try {
                      if (StreamUtils.checkYarnAppNameUnique(jobInsert.name, projectId)) {
                        onComplete(jobDal.insert(jobInsert)) {
                          case Success(job) =>
                            riderLogger.info(s"user ${session.userId} inserted job where project id is $projectId success.")
                            val projectName = jobDal.adminGetRow(job.projectId)
                            complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(job, projectName, getDisableAction(job))))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} inserted job where project id is $projectId failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      } else {
                        riderLogger.warn(s"user ${session.userId} check stream name ${jobInsert.name} already exists success.")
                        complete(OK, getHeader(409, s"${jobInsert.name} already exists", session))
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
                val jobOut = Await.result(jobDal.findById(jobId), minTimeOut)
                jobOut match {
                  case Some(job) =>
                    if (job.status != JobStatus.STARTING.toString &&
                      job.status != JobStatus.WAITING.toString &&
                      job.status != JobStatus.RUNNING.toString &&
                      job.status != JobStatus.STOPPING.toString) {
                      try {
                        val project: Project = Await.result(projectDal.findById(projectId), minTimeOut).head
                        val (projectTotalCore, projectTotalMemory) = (project.resCores, project.resMemoryG)
                        val (jobUsedCore, jobUsedMemory, _) = jobDal.getProjectJobsUsedResource(projectId)
                        val (streamUsedCore, streamUsedMemory, _) = streamDal.getProjectStreamsUsedResource(projectId)
                        val currentConfig = JsonUtils.json2caseClass[StartConfig](job.startConfig)
                        val currentNeededCore = currentConfig.driverCores + currentConfig.executorNums * currentConfig.perExecutorCores
                        val currentNeededMemory = currentConfig.driverMemory + currentConfig.executorNums * currentConfig.perExecutorMemory
                        if ((projectTotalCore - jobUsedCore - streamUsedCore - currentNeededCore) < 0 || (projectTotalMemory - jobUsedMemory - streamUsedMemory - currentNeededMemory) < 0) {
                          riderLogger.warn(s"user ${session.userId} start job $jobId failed, caused by resource is not enough")
                          complete(OK, getHeader(507, "resource is not enough", session))
                        } else {
                          val logPath = JobUtils.getLogPath(job.name)
                          val (result, pid) = JobUtils.startJob(job, logPath)
                          val status =
                            if (result) JobStatus.STARTING.toString
                            else JobStatus.FAILED.toString
                          val updateJob = Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.jobType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd,
                            job.sourceConfig, job.sinkConfig, job.tranConfig, job.tableKeys, job.desc, status, pid, Option(logPath), Some(currentSec), None, job.userTimeInfo)
                          Await.result(jobDal.update(updateJob), minTimeOut)
                          val projectName = jobDal.adminGetRow(job.projectId)
                          complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(updateJob, projectName, getDisableAction(updateJob))))
                        }
                      } catch {
                        case ex: Exception =>
                          riderLogger.error(s"user ${session.userId} start job failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.error(s"can not start ${job.id} at this moment, because the job is in ${job.status} status")
                      complete(OK, getHeader(406, s"can not start ${job.id} at this moment, because the job is in ${job.status} status", session))
                    }
                  case None => complete(OK, ResponseJson[String](getHeader(200, session), ""))
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
                val jobOut = Await.result(jobDal.findById(jobId), minTimeOut)
                jobOut match {
                  case Some(_) =>
                    riderLogger.info(s"user ${session.userId} refresh job.")
                    val jobFind = JobUtils.refreshJob(jobId)
                    val job = JobUtils.hidePid(jobFind)
                    val projectName = jobDal.adminGetRow(job.projectId)
                    complete(OK, ResponseJson[JobTopicInfo](getHeader(200, session), JobTopicInfo(job, projectName, NamespaceUtils.getTopic(job.sourceNs), getDisableAction(job))))
                  case None =>
                    complete(OK, ResponseJson[String](getHeader(200, session), ""))

                }
              } else {
                riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                complete(OK, getHeader(403, session))
              }
            }
        }
      }
  }


  def getByFilterRoute(route: String): Route = path(route / LongNumber / "jobs") {
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
                        val jobsFind: Seq[Job] = jobDal.getAllJobs4Project(projectId)
                        val jobs = JobUtils.hidePid(jobsFind)
                        if (jobs != null && jobs.nonEmpty) {
                          val projectName = jobDal.adminGetRow(projectId)
                          //check null to option None todo
                          val rst: Seq[FullJobInfo] = jobs.map(job => {
                            FullJobInfo(job, projectName, getDisableAction(job))
                          })
                          complete(OK, ResponseJson[Seq[FullJobInfo]](getHeader(200, session), rst.sortBy(_.job.id)))
                        } else {
                          riderLogger.info(s"user ${session.userId} refresh project $projectId, but no jobs in project.")
                          complete(OK, ResponseJson[Seq[FullJobInfo]](getHeader(200, session), Seq()))
                        }
                      case (None, None, Some(jobName)) =>
                        if (StreamUtils.checkYarnAppNameUnique(jobName, projectId)) {
                          riderLogger.info(s"user ${session.userId} check stream name $jobName doesn't exist success.")
                          complete(OK, ResponseJson[String](getHeader(200, session), jobName))
                        } else {
                          riderLogger.warn(s"user ${session.userId} check stream name $jobName already exists success.")
                          complete(OK, getHeader(409, s"$jobName already exists", session))
                        }
                      case (_, _, _) =>
                        riderLogger.error(s"user ${session.userId} request url is not supported.")
                        complete(OK, ResponseJson[String](getHeader(403, session), msgMap(403)))
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
              complete(OK, getHeader(403, session))
            } else {
              try {
                val project = Await.result(projectDal.findById(projectId), minTimeOut)
                if (project.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request to stop project $projectId, job $jobId, but the project $projectId doesn't exist.")
                  complete(OK, getHeader(403, s"project $projectId doesn't exist", session))
                }
                val job: Option[Job] = Await.result(jobDal.findById(jobId), minTimeOut)
                if (job.isEmpty) {
                  riderLogger.error(s"user ${session.userId} request to stop project $projectId, job $jobId, but the job $jobId doesn't exist.")
                  complete(OK, getHeader(403, s"job $jobId doesn't exist", session))
                } else if (job.get.status == JobStatus.STARTING.toString ||
                  job.get.status == JobStatus.STOPPED.toString ||
                  job.get.status == JobStatus.DONE.toString ||
                  job.get.status == JobStatus.NEW.toString) {
                  riderLogger.warn(s"user ${session.userId} job $jobId status is ${job.get.status}, can't stop now.")
                  complete(OK, getHeader(406, s"job $jobId status is starting, can't stop now.", session))
                } else {
                  val (status, stopSuccess) = killJob(jobId)
                  riderLogger.info(s"user ${session.userId} stop job $jobId ${stopSuccess.toString}.")
                  if (stopSuccess) {
                    val projectName = jobDal.adminGetRow(projectId)
                    val jobGet = job.get
                    val updateJob = Job(jobGet.id, jobGet.name, jobGet.projectId, jobGet.sourceNs, jobGet.sinkNs, jobGet.jobType, jobGet.sparkConfig, jobGet.startConfig, jobGet.eventTsStart, jobGet.eventTsEnd,
                      jobGet.sourceConfig, jobGet.sinkConfig, jobGet.tranConfig, jobGet.tableKeys, jobGet.desc, status, jobGet.sparkAppid, jobGet.logPath, jobGet.startedTime, jobGet.stoppedTime, jobGet.userTimeInfo)
                    complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(updateJob, projectName, getDisableAction(updateJob))))
                  } else complete(OK, getHeader(400, s"job $jobId stop failed.", session))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} stop job $jobId failed", ex)
                  complete(OK, getHeader(451, session))
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
                      complete(OK, getHeader(406, s"job $jobId status is starting, can't stop now.", session))
                    } else {
                      if (job.sparkAppid.getOrElse("") != "" && (job.status == JobStatus.RUNNING.toString || job.status == JobStatus.WAITING.toString || job.status == JobStatus.STOPPING.toString)) {
                        val stopSuccess = runYarnKillCommand("yarn application -kill " + job.sparkAppid.get)
                        riderLogger.info(s"user ${session.userId} stop job ${jobId} ${stopSuccess.toString}")
                        if (!stopSuccess)
                          complete(OK, getHeader(400, s"job stop failed, can not delete", session))
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
                  case None => complete(OK, getHeader(200, s"this job ${jobId} does not exist", session))
                }
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"delete job ${jobId} failed", ex)
                  complete(OK, getHeader(451, session))
              }
            } else {
              riderLogger.error(s"user ${session.userId} doesn't have permission to access the project ${projectId}.")
              complete(OK, getHeader(403, session))
            }
        }
      }
  }


  def getLogByJobId(route: String): Route = path(route / LongNumber / "jobs" / LongNumber / "logs") {
    (projectId, jobId) =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "user") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              if (session.projectIdList.contains(projectId)) {
                onComplete(jobDal.findById(jobId)) {
                  case Success(job) =>
                    if (job.isDefined) {
                      riderLogger.info(s"user ${session.userId} refresh job log where job id is $jobId success.")
                      val log = YarnClientLog.getLogByAppName(job.get.name, job.get.logPath.getOrElse(""))
                      complete(OK, ResponseJson[String](getHeader(200, session), log))
                    } else {
                      riderLogger.error(s"user ${session.userId} refresh job log where job id is $jobId, but job does not exist")
                      complete(OK, getHeader(200, s"job id is $jobId, but job does not exists", session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} refresh job log where job id is $jobId failed", ex)
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


  def putRoute(route: String): Route = path(route / LongNumber / "jobs") {
    projectId =>
      put {
        entity(as[Job]) {
          updatedJob =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                } else {
                  if (session.projectIdList.contains(projectId)) {
                    val newJob = Job(updatedJob.id, updatedJob.name, updatedJob.projectId, updatedJob.sourceNs, updatedJob.sinkNs, updatedJob.jobType, updatedJob.sparkConfig, updatedJob.startConfig, updatedJob.eventTsStart, updatedJob.eventTsEnd,
                      updatedJob.sourceConfig, updatedJob.sinkConfig, updatedJob.tranConfig, updatedJob.tableKeys, updatedJob.desc, updatedJob.status, updatedJob.sparkAppid, updatedJob.logPath, if (updatedJob.startedTime.isEmpty || updatedJob.startedTime.get == null || updatedJob.startedTime.get.trim.isEmpty) null else updatedJob.startedTime, if (updatedJob.stoppedTime.isEmpty || updatedJob.stoppedTime.get.trim.isEmpty) null else updatedJob.stoppedTime, UserTimeInfo(updatedJob.userTimeInfo.createTime, updatedJob.userTimeInfo.createBy, currentSec, session.userId))
                    onComplete(jobDal.update(newJob)) {
                      case Success(_) =>
                        riderLogger.info(s"user ${session.userId} update job where project id is $projectId success.")
                        val projectName = jobDal.adminGetRow(newJob.projectId)
                        complete(OK, ResponseJson[FullJobInfo](getHeader(200, session), FullJobInfo(newJob, projectName, getDisableAction(newJob))))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} update job where project id is $projectId failed", ex)
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


  def getDataVersions(route: String): Route = path(route / LongNumber / "jobs" / "dataversions") {
    projectId =>
      get {
        parameter('namespace.as[String].?) {
          namespace =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  try {
                    if (session.projectIdList.contains(projectId)) {
                      namespace match {
                        case Some(sourceNamespace) =>
                          val dataVersions = JobUtils.getHdfsDataVersions(sourceNamespace)
                          complete(OK, ResponseJson[String](getHeader(200, session), dataVersions))
                        case None =>
                          riderLogger.error(s"user ${session.userId} request url is not supported.")
                          complete(OK, getHeader(404, session))
                      }
                    } else {
                      riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                      complete(OK, getHeader(403, session))
                    }
                  } catch {
                    case ex: Exception =>
                      riderLogger.error(s"user ${session.userId} get hdfs data version where project id is $projectId failed", ex)
                      complete(OK, getHeader(451, ex.getMessage, session))
                  }
                }
            }
        }
      }
  }

}
