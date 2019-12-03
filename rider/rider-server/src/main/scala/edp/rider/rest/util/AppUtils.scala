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


package edp.rider.rest.util

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.common.DbPermission._
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{ResponseHeader, SessionClass}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.JobUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await

object AppUtils extends RiderLogger {
  def prepare(appJob: Option[AppJob], appFlow: Option[AppFlow], session: SessionClass, projectId: Long, streamId: Option[Long] = None): Either[ResponseHeader, (Option[Job], Option[Flow])] = {
    val project = Await.result(modules.projectDal.findById(projectId), minTimeOut)
    if (project.isEmpty) {
      riderLogger.error(s"user ${session.userId} request to start project $projectId, stream $streamId flow/job, but the project $projectId doesn't exist.")
      return Left(ResponseHeader(404, s"project $projectId doesn't exist"))
    } else {
      if (!modules.relProjectUserDal.isAvailable(projectId, session.userId)) {
        riderLogger.error(s"user ${session.userId} request to start project $projectId, stream $streamId flow/job, but user ${session.userId} doesn't have permission to access project $projectId, please contact admin.")
        return Left(ResponseHeader(403, s"user ${session.userId} doesn't have permission to access project $projectId, please contact admin"))
      }
    }
    if (streamId.nonEmpty) {
      val stream = Await.result(modules.streamDal.findByFilter(stream => stream.id === streamId.get && stream.projectId === projectId), minTimeOut)
      if (stream.isEmpty) {
        riderLogger.error(s"user ${session.userId} request to start project $projectId, stream $streamId flow, but the $streamId doesn't exist.")
        return Left(ResponseHeader(404, s"stream ${streamId.get} doesn't exist"))
      } else {
        val streamProject = Await.result(modules.streamDal.findByFilter(stream => stream.id === streamId.get && stream.projectId === projectId), minTimeOut)
        if (streamProject.isEmpty) {
          riderLogger.error(s"user ${session.userId} request to start project $projectId, stream $streamId flow, but the $streamId doesn't belong to the project.")
          return Left(ResponseHeader(403, s"stream ${streamId.get} doesn't belong to project $projectId"))
        }
      }
    }
    searchSourceNs(appJob, appFlow, session, projectId) match {
      case Right(sourceNamespace) =>
        searchSinkNs(appJob, appFlow, session, projectId) match {
          case Right(sinkNamespace) =>
            val sourceNs = generateStandardNs(sourceNamespace)
            val sinkNs = generateStandardNs(sinkNamespace)
            val updateResult =
              if (isAppJob(appJob, appFlow)) Await.result(db.run(modules.jobQuery.filter(job => job.sourceNs === sourceNs && job.sinkNs === sinkNs && (job.status === "stopped" || job.status === "failed" || job.status === "done"))
                .map(job => job.status).update("starting")).mapTo[Int], minTimeOut)
              else Await.result(db.run(modules.flowQuery.filter(flow => flow.sourceNs === sourceNs && flow.sinkNs === sinkNs && (flow.status === "stopped" || flow.status === "failed"))
                .map(flow => flow.status).update("starting")).mapTo[Int], minTimeOut)
            if (updateResult != 0) {
              updateSinkAppNs(appJob, appFlow, sinkNamespace, session, projectId)
              if (appJob.nonEmpty) {
                if (!isJson(appJob.get.sinkConfig.getOrElse("")))
                  return Left(ResponseHeader(400, s"job sinkConfig ${appJob.get.sinkConfig} is not json type"))
              }
              if (appFlow.nonEmpty) {
                if (!isJson(appFlow.get.sinkConfig))
                  return Left(ResponseHeader(400, s"flow sinkConfig ${appFlow.get.sinkConfig} is not json type"))
              }
              Right(insertOrUpdate(appJob, appFlow, sourceNs, sinkNs, session, projectId, streamId))
            }
            else {
              val status =
                if (isAppJob(appJob, appFlow)) {
                  val jobSearch = Await.result(modules.jobDal.findByFilter(job => job.sourceNs === sourceNs && job.sinkNs === sinkNs), minTimeOut)
                  if (jobSearch.isEmpty) None else Some(jobSearch.head.status)
                } else {
                  val flowSearch = Await.result(modules.flowDal.findByFilter(flow => flow.sourceNs === sourceNs && flow.sinkNs === sinkNs), minTimeOut)
                  if (flowSearch.isEmpty) None else Some(flowSearch.head.status)
                }
              if (status.nonEmpty) {
                riderLogger.error(s"user ${session.userId} project $projectId request source namespace dataSys $sourceNs to $sinkNs is already starting or running.")
                Left(getHeader(403, s"Duplicate ${status.get}", null))
              } else {
                updateSinkAppNs(appJob, appFlow, sinkNamespace, session, projectId)
                Right(insertOrUpdate(appJob, appFlow, sourceNs, sinkNs, session, projectId, streamId))
              }
            }
          case Left(response) => Left(response)
        }
      case Left(response) => Left(response)
    }
  }

  def insertOrUpdate(appJob: Option[AppJob], appFlow: Option[AppFlow], sourceNs: String, sinkNs: String, session: SessionClass, projectId: Long, streamId: Option[Long]): (Option[Job], Option[Flow]) = {
    if (appJob.nonEmpty) {
      val jobSearch = Await.result(modules.jobDal.findByFilter(job => job.sourceNs === sourceNs && job.sinkNs === sinkNs), minTimeOut).headOption
      val job =
        if (jobSearch.nonEmpty) {
          val startedTime = Some(currentSec)
          val stoppedTime = if (jobSearch.get.stoppedTime.getOrElse("") == "") null else jobSearch.get.stoppedTime
          val jobUpdate = Job(jobSearch.get.id, jobSearch.get.name, projectId, sourceNs, sinkNs, jobSearch.get.jobType, jobSearch.get.sparkConfig, jobSearch.get.startConfig,
            appJob.get.eventTsStart.getOrElse(""), appJob.get.eventTsEnd.getOrElse(""), jobSearch.get.sourceConfig, appJob.get.sinkConfig, Some(genJobTranConfigByColumns(jobSearch.get.tranConfig.getOrElse(""), appJob.get.sinkColumns.getOrElse(""))),
            Some(appJob.get.sinkKeys), None,"starting", None, jobSearch.get.logPath, startedTime, stoppedTime, UserTimeInfo(jobSearch.get.userTimeInfo.createTime, jobSearch.get.userTimeInfo.createBy, currentSec, session.userId))
          Await.result(modules.jobDal.update(jobUpdate), minTimeOut)
          riderLogger.info(s"user ${session.userId} project $projectId update job success.")
          jobUpdate
        } else {
          val jobInsert = Job(0, appJob.get.name.getOrElse(genJobName(projectId, sourceNs, sinkNs)), projectId, sourceNs, getJobSinkNs(sourceNs, sinkNs, appJob.get.jobType), appJob.get.jobType,
            SparkConfig(Some(RiderConfig.spark.driverExtraConf), Some(RiderConfig.spark.executorExtraConf), Some(RiderConfig.spark.sparkConfig)), "", appJob.get.eventTsStart.getOrElse(""), appJob.get.eventTsEnd.getOrElse(""), None, appJob.get.sinkConfig,
            Some(genJobTranConfigByColumns(appJob.get.tranConfig.getOrElse(""), appJob.get.sinkColumns.getOrElse(""))),
            Some(appJob.get.sinkKeys), None, "starting", None, Some(""), Some(currentSec), None,
            UserTimeInfo(currentSec, session.userId, currentSec, session.userId))
          val result = Await.result(modules.jobDal.insert(jobInsert), minTimeOut)
          riderLogger.info(s"user ${session.userId} project $projectId insert job success.")
          result
        }
      (Some(job), None)
    }

    else {
      val flowSearch = Await.result(modules.flowDal.findByFilter(flow => flow.sourceNs === sourceNs && flow.sinkNs === sinkNs), minTimeOut).headOption
      val flow =
        if (flowSearch.nonEmpty) {
          val startedTime = if (flowSearch.get.startedTime.getOrElse("") == "") Some(currentSec) else flowSearch.get.startedTime
          val stoppedTime = if (flowSearch.get.stoppedTime.getOrElse("") == "") null else flowSearch.get.stoppedTime
          val consumedProtocol = if (appFlow.get.consumedProtocol.getOrElse("") == "") flowSearch.get.consumedProtocol else appFlow.get.consumedProtocol.get
          val flowUpdate = Flow(flowSearch.get.id, flowSearch.get.flowName, projectId, streamId.get, 0L, sourceNs, sinkNs, flowSearch.get.config, consumedProtocol, Some(appFlow.get.sinkConfig), Some(genFlowTranConfigByColumns(flowSearch.get.tranConfig.getOrElse(""), appFlow.get.sinkColumns)), Some(appFlow.get.sinkKeys), None, "updating", startedTime, stoppedTime, flowSearch.get.logPath, active = true, flowSearch.get.createTime, flowSearch.get.createBy, currentSec, session.userId)
          Await.result(modules.flowDal.update(flowUpdate), minTimeOut)
          riderLogger.info(s"user ${session.userId} project $projectId update flow success.")
          flowUpdate
        } else {
          val flowInsert = Flow(0, "flowApp", projectId, streamId.get, 0L, sourceNs, sinkNs, appFlow.get.config, appFlow.get.consumedProtocol.getOrElse("all"), Some(appFlow.get.sinkConfig), Some(genFlowTranConfigByColumns(sinkColumns = appFlow.get.sinkColumns)), Some(appFlow.get.sinkKeys), None, "starting", Some(currentSec), None, None, active = true, currentSec, session.userId, currentSec, session.userId)
          val result = Await.result(modules.flowDal.insert(flowInsert), minTimeOut)
          riderLogger.info(s"user ${session.userId} project $projectId insert flow success.")
          result
        }
      (None, Some(flow))
    }
  }

  def searchSourceNs(appJob: Option[AppJob], appFlow: Option[AppFlow], session: SessionClass, projectId: Long): Either[ResponseHeader, Namespace] = {
    val (sourceSys, sourceDatabase, sourceTable) =
      if (appJob.nonEmpty) (appJob.get.sourceSys, appJob.get.sourceDatabase, appJob.get.sourceTable)
      else (appFlow.get.sourceSys, appFlow.get.sourceDatabase, appFlow.get.sourceTable)
    val nsOpt = modules.namespaceDal.getNamespaceByNs(sourceSys, sourceDatabase, sourceTable)
    nsOpt match {
      case Some(ns) =>
        if (modules.relProjectNsDal.isAvailable(projectId, ns.id)) {
          riderLogger.info(s"user ${session.userId} project $projectId request for source namespace dataSys $sourceSys, database $sourceDatabase, table $sourceTable success.")
          Right(ns)
        }
        else {
          riderLogger.error(s"user ${session.userId} project $projectId doesn't have permission to access the source namespace dataSys $sourceSys, database $sourceDatabase, table $sourceTable.")
          Left(getHeader(403, s"user ${session.userId} project $projectId doesn't have permission to access the source table, please contact admin", null))
        }
      case None =>
        riderLogger.error(s"user ${session.userId} request for post job/flow, but source namespace dataSys $sourceSys, database $sourceDatabase, table $sourceTable doesn't exist.")
        Left(getHeader(404, "the source table doesn't exist now, please contact admin", null))
    }
  }

  def searchSinkNs(simpleJob: Option[AppJob], simpleFlow: Option[AppFlow], session: SessionClass, projectId: Long): Either[ResponseHeader, Namespace] = {
    val (sinkSys, sinkInstance, sinkDatabase, sinkTable) =
      if (simpleJob.nonEmpty) (simpleJob.get.sinkSys, simpleJob.get.sinkInstance, simpleJob.get.sinkDatabase, simpleJob.get.sinkTable)
      else (simpleFlow.get.sinkSys, simpleFlow.get.sinkInstance, simpleFlow.get.sinkDatabase, simpleFlow.get.sinkTable)
    val nsOpt = modules.namespaceDal.getSinkNamespaceByNs(sinkSys, sinkInstance, sinkDatabase, sinkTable)
    nsOpt match {
      case Some(ns) =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase, table $sinkTable success.")
        if (modules.relProjectNsDal.isAvailable(projectId, ns.id))
          riderLogger.info(s"user ${session.userId} project $projectId request for source namespace dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase, table $sinkTable success.")
        else
          Await.result(modules.relProjectNsDal.insert(RelProjectNs(0, projectId, ns.id, active = true, currentSec, session.userId, currentSec, session.userId)), minTimeOut)
        Right(ns)
      case None =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase, table $sinkTable doesn't exist, insert it.")
        insertSinkAppNs(simpleJob, simpleFlow, session, projectId)
    }
  }

  private def isAppJob(appJob: Option[AppJob], appFlow: Option[AppFlow]) = appJob.nonEmpty

  def insertSinkAppNs(appJob: Option[AppJob], appFlow: Option[AppFlow], session: SessionClass, projectId: Long): Either[ResponseHeader, Namespace] = {
    val (sinkSys, sinkInstance, sinkDatabase, sinkTable, sinkKeys) =
      if (isAppJob(appJob, appFlow)) (appJob.get.sinkSys, appJob.get.sinkInstance, appJob.get.sinkDatabase, appJob.get.sinkTable, appJob.get.sinkKeys)
      else (appFlow.get.sinkSys, appFlow.get.sinkInstance, appFlow.get.sinkDatabase, appFlow.get.sinkTable, appFlow.get.sinkKeys)
    val instanceSearchOpt = Await.result(modules.instanceDal.findByFilter(_.nsInstance === sinkInstance), minTimeOut).headOption
    val instance = instanceSearchOpt match {
      case Some(instanceSearch) =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance success.")
        instanceSearch
      case None =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance, instance $sinkInstance doesn't exist.")
        return Left(getHeader(404, s"the sink instance $sinkInstance doesn't exist now, please contact admin", null))
    }
    val databaseSearchOpt = Await.result(modules.databaseDal.findByFilter(db => db.nsDatabase === sinkDatabase), minTimeOut).headOption
    val database = databaseSearchOpt match {
      case Some(databaseSearch) =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase success.")
        databaseSearch
      case None =>
        riderLogger.info(s"user ${session.userId} project $projectId request sink namespace dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase, database $sinkDatabase doesn't exist.")
        return Left(getHeader(404, s"the sink database $sinkDatabase, permission ReadWrite doesn't exist now, please contact admin", null))
    }
    val nsInsert = Namespace(0, sinkSys, sinkInstance, sinkDatabase, sinkTable, "*", "*", "*",
      Some(sinkKeys), None, None, database.id, instance.id, active = true, currentSec, session.userId, currentSec, session.userId)
    val ns = Await.result(modules.namespaceDal.insert(nsInsert), minTimeOut)
    riderLogger.info(s"user ${session.userId} project $projectId insert namespace success.")
    val rel = RelProjectNs(0, projectId, ns.id, active = true, currentSec, session.userId, currentSec, session.userId)
    Await.result(modules.relProjectNsDal.insert(rel), minTimeOut)
    riderLogger.info(s"user ${session.userId} project $projectId insert request sink namespace $ns for dataSys $sinkSys, instance $sinkInstance, database $sinkDatabase, table $sinkTable success.")
    Right(ns)
  }

  def updateSinkAppNs(appJob: Option[AppJob], appFlow: Option[AppFlow], sinkNs: Namespace, session: SessionClass, projectId: Long): Namespace = {
    val sinkKeys = if (appJob.nonEmpty) appJob.get.sinkKeys else appFlow.get.sinkKeys
    if (sinkKeys != "" && sinkKeys != null) {
      if (sinkKeys == sinkNs.keys.getOrElse("")) sinkNs
      else {
        modules.namespaceDal.updateKeys(sinkNs.id, sinkKeys)
        Namespace(sinkNs.id, sinkNs.nsSys, sinkNs.nsInstance, sinkNs.nsDatabase, sinkNs.nsTable, sinkNs.nsVersion, sinkNs.nsDbpar, sinkNs.nsTablepar,
          Some(sinkKeys), sinkNs.sourceSchema, sinkNs.sinkSchema, sinkNs.nsDatabaseId, sinkNs.nsInstanceId, active = true, sinkNs.createTime, sinkNs.createBy, currentSec, session.userId)
      }
    } else sinkNs
  }

  def genJobTranConfigByColumns(tranConfig: String = null, sinkColumns: String = null): String = {
    if (tranConfig != null && tranConfig != "") {
      if (JSON.parseObject(tranConfig).containsKey("projection")) {
        val tranConfigUpdate = if (sinkColumns != "" && sinkColumns != null)
          JSON.parseObject(tranConfig).fluentPut("projection", sinkColumns)
        else JSON.parseObject(tranConfig).fluentRemove("projection")
        if (tranConfigUpdate.size != 0) tranConfigUpdate.toString else ""
      }
      else
        tranConfig
    }
    else if (sinkColumns != "" && sinkColumns != null) {
      s"""{"projection": "$sinkColumns"}"""
    }
    else "{}"
  }

  def genFlowTranConfigByColumns(tranConfig: String = null, sinkColumns: String = null): String = {
    if (tranConfig != null && tranConfig != "") {
      if (JSON.parseObject(tranConfig).containsKey("output")) {
        val tranConfigUpdate = if (sinkColumns != "" && sinkColumns != null)
          JSON.parseObject(tranConfig).fluentPut("output", sinkColumns)
        else JSON.parseObject(tranConfig).fluentRemove("output")
        if (tranConfigUpdate.size != 0) tranConfigUpdate.toString else ""
      }
      else
        tranConfig
    }
    else if (sinkColumns != "" && sinkColumns != null) {
      s"""{"projection": "$sinkColumns"}"""
    }
    else "{}"
  }
}
