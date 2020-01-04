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


package edp.rider.rest.persistence.dal

import edp.rider.RiderStarter.modules
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.common.{AppInfo, AppResult}
import edp.rider.rest.util.JobUtils
import edp.rider.yarn.YarnClientLog.getAppStatusByLog
import edp.rider.yarn.{ShellUtils, SubmitYarnJob, YarnStatusQuery}
import edp.rider.yarn.YarnStatusQuery.getAppStatusByRest
import edp.wormhole.util.JsonUtils
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.{Await, Future}

class JobDal(jobTable: TableQuery[JobTable], projectTable: TableQuery[ProjectTable]) extends BaseDalImpl[JobTable, Job](jobTable) {

  def updateJobStatus(jobId: Long, appInfo: AppInfo, logPath: String): Int = {
    Await.result(db.run(jobTable.filter(_.id === jobId).map(c => (c.sparkAppid, c.status, c.logPath, c.startedTime, c.stoppedTime, c.updateTime))
      .update(Option(appInfo.appId), appInfo.appState, Option(logPath), Option(appInfo.startedTime), Option(appInfo.finishedTime), currentSec)), minTimeOut)
  }

  def updateJobStatus(jobId: Long, status: String): Int = {
    Await.result(db.run(jobTable.filter(_.id === jobId).map(c => (c.status, c.updateTime))
      .update(status, currentSec)), minTimeOut)
  }

  //  def getJobNameByJobID(jobId: Long): Future[Job] = {
  //    db.run(jobTable.filter(_.id === jobId).result.head)
  //  }


  //def updateJobStatusList(appInfoSeq: Seq[(Int, AppInfo)]) = appInfoSeq.foreach { case (jobId, appInfo) => updateJobStatus(jobId, appInfo) }


  def adminGetRow(projectId: Long): String = {
    Await.result(db.run(projectTable.filter(_.id === projectId).result), maxTimeOut).head.name
  }

  def getAllJobs4Project(projectId: Long): Seq[Job] = {
    Await.result(db.run(jobTable.filter(_.projectId === projectId).result), maxTimeOut)
  }

  def checkJobNameUnique(jobName: String): Future[Seq[Job]] = {
    db.run(jobTable.filter(_.name === jobName).result)
  }

  def getAllJobs: Seq[Job] = {
    Await.result(super.findAll, minTimeOut)
  }

  def getAllUniqueProjectIdAndName(uniqueProjectIds: Seq[Long]): Map[Long, String] = {
    Await.result(db.run(projectTable.filter(_.id inSet uniqueProjectIds).result), maxTimeOut).map(p => (p.id, p.name)).toMap
  }


  def getProjectJobsUsedResource(projectId: Long) = {
    val jobSeq: Seq[Job] = Await.result(super.findByFilter(job => job.projectId === projectId && (job.status === "running" || job.status === "waiting" || job.status === "starting" || job.status === "stopping")), minTimeOut)
    var usedCores = 0
    var usedMemory = 0
    val jobResources: Seq[AppResource] = jobSeq.map(
      job => {
        val config = JsonUtils.json2caseClass[StartConfig](job.startConfig)
        usedCores += config.driverCores + config.executorNums * config.perExecutorCores
        usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
        AppResource(job.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
      }
    )
    (usedCores, usedMemory, jobResources)
  }

  def updateJobStatusByYarn(jobs: Seq[Job], appInfoMap: Map[String, AppResult]): Unit = {
    if (jobs != null && jobs.nonEmpty) {
      jobs.map(job => {
        val appInfo = JobUtils.mappingSparkJobStatus(job, appInfoMap)
        if((appInfo.appId, appInfo.appState, JobUtils.getJobTime(Option(appInfo.startedTime)) , JobUtils.getJobTime(Option(appInfo.finishedTime))) != (job.sparkAppid.getOrElse(""), job.status, JobUtils.getJobTime(job.startedTime), JobUtils.getJobTime(job.stoppedTime))) {
          /*if (job.status == "starting" && (appInfo.appState == "running" || appInfo.appState == "waiting" || appInfo.appState == "failed"))
            SubmitYarnJob.killPidCommand(job.sparkAppid, job.name)*/
          modules.jobDal.updateJobStatus(job.id, appInfo, job.logPath.getOrElse(""))
        }
      })
    }
  }
}
