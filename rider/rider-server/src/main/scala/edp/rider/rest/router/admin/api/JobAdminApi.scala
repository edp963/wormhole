package edp.rider.rest.router.admin.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.{AppResult, JobStatus, RiderLogger}
import edp.rider.rest.persistence.dal.JobDal
import edp.rider.rest.persistence.entities.{FullJobInfo, Job, JobTopicInfo}
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.{AuthorizationProvider, JobUtils, NamespaceUtils}
import edp.rider.rest.util.JobUtils.getDisableAction
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.yarn.{YarnClientLog, YarnStatusQuery}
import edp.rider.rest.util.CommonUtils.minTimeOut

import scala.concurrent.Await
import scala.util.{Failure, Success}

class JobAdminApi(jobDal: JobDal) extends BaseAdminApiImpl(jobDal) with RiderLogger with JsonSerializer {
  override def getByAllRoute(route: String): Route = path(route) {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin") {
            riderLogger.warn(s"${session.userId} has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            val jobs = jobDal.getAllJobs
            val uniqueProjectIds = jobs.map(_.projectId).distinct
            val projectIdAndName: Map[Long, String] = jobDal.getAllUniqueProjectIdAndName(uniqueProjectIds)
            if (jobs != null && jobs.nonEmpty) {
              //check null to option None todo
              val jobsGroupByProjectId: Map[Long, Seq[Job]] = jobs.groupBy(_.projectId)
              val rst = jobsGroupByProjectId.flatMap { case (projectId, jobSeq) =>
                jobSeq.map(job => {
                  FullJobInfo(job, projectIdAndName(projectId), getDisableAction(job))
                })
              }.toSeq.sortBy(_.job.id)
              riderLogger.info(s"user ${session.userId} select all jobs success.")
              complete(OK, ResponseSeqJson[FullJobInfo](getHeader(200, session), rst))
            } else {
              riderLogger.info(s"user ${session.userId} admin refresh jobs, but no jobs here.")
              complete(OK, ResponseJson[Seq[FullJobInfo]](getHeader(200, session), Seq()))
            }
          }
      }
    }
  }


  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "jobs") {
    projectId =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              riderLogger.info(s"user ${session.userId} refresh project $projectId")
              val jobs: Seq[Job] = jobDal.getAllJobs4Project(projectId)
              if (jobs != null && jobs.nonEmpty) {
                riderLogger.info(s"user ${session.userId} refresh project $projectId, and job in it is not null and not empty.")
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
            }
        }
      }

  }


  def getLogByJobId(route: String): Route = path(route / LongNumber / "logs") {
    jobId =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(jobDal.findById(jobId)) {
                case Success(job) =>
                  if (job.isDefined) {
                    riderLogger.info(s"user ${session.userId} refresh job log where job id is $jobId success.")
                    val log = YarnClientLog.getLogByAppName(job.get.name, job.get.logPath.getOrElse(""))
                    complete(OK, ResponseJson[String](getHeader(200, session), log))
                  } else {
                    riderLogger.error(s"user ${session.userId} refresh job log where job id is $jobId, but job do not exist")
                    complete(OK, getHeader(451, s"job id is $jobId, but job do not exists", session))
                  }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} refresh job log where job id is $jobId failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }


  def getByJobIdRoute(route: String): Route = path(route / LongNumber) {
    jobId =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              val jobOut = Await.result(jobDal.findById(jobId), minTimeOut)
              jobOut match {
                case Some(_) =>
                  riderLogger.info(s"user ${session.userId} refresh job.")
                  val job = JobUtils.refreshJob(jobId)
                  val projectName = jobDal.adminGetRow(job.projectId)
                  complete(OK, ResponseJson[JobTopicInfo](getHeader(200, session), JobTopicInfo(job, projectName, NamespaceUtils.getTopic(job.sourceNs), getDisableAction(job))))
                case None =>
                  complete(OK, getHeader(200, s"this job ${jobId} does not exist", session))

              }
            }
        }
      }
  }

}
