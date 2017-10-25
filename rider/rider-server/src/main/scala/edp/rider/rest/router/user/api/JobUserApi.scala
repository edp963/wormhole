package edp.rider.rest.router.user.api

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.{AppInfo, RiderLogger}
import edp.rider.rest.persistence.dal.{JobDal, ProjectDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseJson, SessionClass}
import edp.rider.rest.router.app.api.BaseAppApiImpl
import edp.rider.rest.util.AppUtils.prepare
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils.currentSec
import edp.rider.rest.util.JobUtils.startJob
import edp.rider.rest.util.ResponseUtils.getHeader
import edp.rider.rest.router.JsonProtocol._
import edp.rider.service.util.CacheMap

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
                    val jobInsert = Job(0, simple.name, simple.projectId, simple.sourceNs, simple.sinkNs, simple.sourceType, simple.consumedProtocol, simple.eventTsStart, simple.eventTsEnd,
                      simple.sourceConfig, simple.sinkConfig, simple.tranConfig, simple.jobConfig, "new", Some(""), Some(""), null, null, currentSec, session.userId, currentSec, session.userId)
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
}


//               sourceType: String,//1
//               consumedProtocol: String,// 1
//               eventTsStart: String,
//               eventTsEnd: String,
//               sourceConfig: Option[String],// 1
//               sinkConfig: Option[String],// 1
//               tranConfig: Option[String],// 1
//               jobConfig: Option[String],// 1
//               status: String,
//               sparkAppid: Option[String] = None,
//               logPath: Option[String] = None,
//               startedTime: Option[String] = None,
//               stoppedTime: Option[String] = None,
//               createTime: String,
//               createBy: Long,
//               updateTime: String,
//               updateBy: Long