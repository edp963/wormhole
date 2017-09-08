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
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.entities.{Instance, Job, NsDatabase, StartConfig}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.FlowUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.spark.SubmitSparkJob._
import edp.rider.spark.SparkStatusQuery.getSparkJobStatus
import edp.rider.wormhole._
import edp.wormhole.common.util.CommonUtils._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.common.{ConnectionConfig, KVConfig}

import scala.concurrent.Await

object JobUtils extends RiderLogger {

  def getBatchJobConfigConfig(job: Job) =
    BatchJobConfig(getSourceConfig(job.sourceNs, job.eventTsStart, job.eventTsEnd, job.sourceType, job.consumedProtocol),
      getTranConfig(job.tranConfig.getOrElse("")),
      getSinkConfig(job.sinkNs, job.sinkConfig.getOrElse("")),
      getJobConfig(job.name, job.jobConfig.getOrElse("")))

  def getSourceConfig(sourceNs: String, eventTsStart: String = null, eventTsEnd: String = null, sourceType: String = null, consumptionProtocol: String = null) = {
    val eventTsStartFinal = if (eventTsStart != null && eventTsStart != "") eventTsStart else "19700101000000"
    val eventTsEndFinal = if (eventTsEnd != null && eventTsEnd != "") eventTsEnd else "30000101000000"
    val sourceTypeFinal = if (sourceType != null && sourceType != "") sourceType else "hdfs_txt"
    val specialConfig = if (consumptionProtocol != null && consumptionProtocol != "") Some(base64byte2s(getConsumptionProtocol(consumptionProtocol).trim.getBytes())) else None
    val (instance, db, _) = modules.namespaceDal.getNsDetail(sourceNs)
    SourceConfig(eventTsStartFinal, eventTsEndFinal, sourceNs, getConnConfig(instance, db, sourceType), getSourceProcessClass(sourceTypeFinal), specialConfig)
  }

  def getSinkConfig(sinkNs: String, sinkConfig: String) = {
    val (instance, db, ns) = modules.namespaceDal.getNsDetail(sinkNs)
    val maxRecord =
      if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("maxRecordPerPartitionProcessed"))
        JSON.parseObject(sinkConfig).getIntValue("maxRecordPerPartitionProcessed")
      else modules.config.getInt("spark.config.job.default.sink.per.partition.max.record")
    val specialConfig =
      if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_specific_config"))
        Some(base64byte2s(JSON.parseObject(sinkConfig).getString("sink_specific_config").trim.getBytes()))
      else None
    SinkConfig(sinkNs, getConnConfig(instance, db), maxRecord, Some(getSinkProcessClass(ns.nsSys)), specialConfig, ns.keys)
  }

  def getTranConfig(tranConfig: String) = {
    if (tranConfig != "" && tranConfig != null) {
      val tranClass = json2caseClass[TransformationConfig](tranConfig)
      val action = if (tranClass.action.nonEmpty) Some(base64byte2s(tranClass.action.get.trim.getBytes)) else None
      Some(TransformationConfig(action, tranClass.projection, tranClass.specialConfig))
    }
    else None
  }

  def getJobConfig(name: String, jobConfig: String) = {
    val sqlShufflePartition =
      if (jobConfig != "" && jobConfig != null) {
        if (JSON.parseObject(jobConfig).containsKey("spark.sql.shuffle.partitions"))
          Some(JSON.parseObject(jobConfig).getIntValue("spark.sql.shuffle.partitions"))
        else None
      } else None
    JobConfig(name, "yarn-cluster", sqlShufflePartition)
  }

  def getSourceProcessClass(sourceType: String): String = {
    sourceType match {
      case "hdfs_txt" => "edp.wormhole.batchjob.source.SourceHdfs"
      case _ =>
        riderLogger.error(s"this sourceType $sourceType isn't supported now")
        throw new Exception(s"this sourceType $sourceType isn't supported now")
    }
  }

  def getConsumptionProtocol(protocol: String): String = {
    protocol match {
      case "increment" => "{\"initial\": false, \"increment\": true}"
      case "initial" => "{\"initial\": true, \"increment\": false}"
      case "all" => "{\"initial\": true, \"increment\": true}"
    }
  }


  def getConnConfig(instance: Instance, db: NsDatabase, sourceType: String = null): ConnectionConfig = {
    val connUrl =
      if (sourceType == null || sourceType == "" || !sourceType.contains("hdfs")) getConnUrl(instance, db)
      else modules.config.getString("spark.submit.stream.hdfs.address")
    if (db.config.getOrElse("") != "")
      ConnectionConfig(connUrl, db.user, db.pwd, json2caseClass[Option[Seq[KVConfig]]](db.config.get))
    else
      ConnectionConfig(connUrl, db.user, db.pwd, None)
  }

  def startJob(job: Job) = {
    runShellCommand(generateStreamStartSh(s"'''${base64byte2s(caseClass2json(getBatchJobConfigConfig(job)).trim.getBytes)}'''", job.name,
      StartConfig(modules.config.getInt("spark.config.driver.cores"),
        modules.config.getInt("spark.config.driver.memory"),
        modules.config.getInt("spark.config.num.executors"),
        modules.config.getInt("spark.config.per.executor.memory"),
        modules.config.getInt("spark.config.per.executor.cores")),
      Seq(modules.config.getString("spark.config.driver.conf"), modules.config.getString("spark.config.executor.conf")).mkString(","),
      "job"
    ))
  }

  def genJobName(projectId: Long, sourceNs: String, sinkNs: String) = {
    val projectName = Await.result(modules.projectDal.findById(projectId), minTimeOut).head.name
    s"wormhole_${projectName}_job_${sourceNs}_${sinkNs}_$currentyyyyMMddHHmmss"
  }

  def refreshJob(id: Long) = {
    val job = Await.result(modules.jobDal.findById(id), minTimeOut).head
    val appInfo = getSparkJobStatus(job)
    modules.jobDal.updateJobStatus(job.id, appInfo)
    val startedTime = if(appInfo.startedTime != null) Some(appInfo.startedTime) else Some("")
    val stoppedTime = if(appInfo.finishedTime != null) Some(appInfo.finishedTime) else Some("")
    Job(job.id, job.name, job.projectId, job.sourceType, job.sinkNs, job.sourceType, job.consumedProtocol, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
      job.sinkConfig, job.tranConfig, job.jobConfig, appInfo.appState, Some(appInfo.appId), job.logPath, startedTime, stoppedTime, job.createTime, job.createBy, job.updateTime, job.updateBy)
  }

  def killJob(id: Long): String = {
    try {
      val job = refreshJob(id)
      try {
        if (job.status != "failed" && job.status != "stopped" && job.status != "done") {
          runShellCommand(s"yarn application -kill ${job.sparkAppid}")
          modules.jobDal.updateJobStatus(job.id, "stopping")
          "stopping"
        } else job.status
      }
      catch {
        case ex: Exception =>
          riderLogger.error(s"job $id kill failed", ex)
          throw ex
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"job $id kill failed", ex)
        throw ex
    }
  }
}
