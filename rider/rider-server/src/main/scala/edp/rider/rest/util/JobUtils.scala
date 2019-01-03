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

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.rider.RiderStarter.modules
import edp.rider.common._
import edp.rider.rest.persistence.entities.{Instance, Job, NsDatabase, StartConfig}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.FlowUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.NsDatabaseUtils._
import edp.rider.yarn.YarnStatusQuery.getSparkJobStatus
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.wormhole._
import edp.wormhole.ums.UmsDataSystem
import edp.wormhole.util.JsonUtils._
import edp.wormhole.util.CommonUtils._
import edp.wormhole.util.DateUtils
import edp.wormhole.util.config.ConnectionConfig

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

object JobUtils extends RiderLogger {

  def getBatchJobConfigConfig(job: Job) =
    BatchJobConfig(getSourceConfig(job.sourceNs, job.eventTsStart, job.eventTsEnd, job.sourceConfig),
      getTranConfig(job.tranConfig.getOrElse(""), job.sinkConfig.getOrElse(""), job.sinkNs, job.jobType, job.tableKeys),
      getSinkConfig(job.sinkNs, job.sinkConfig.getOrElse(""), job.jobType, job.eventTsEnd, job.tableKeys),
      getJobConfig(job.name, job.sparkConfig.othersConfig))

  def getSourceConfig(sourceNs: String, eventTsStart: String = null, eventTsEnd: String = null, sourceConfig: Option[String]) = {
    val eventTsStartFinal = if (eventTsStart != null && eventTsStart != "") eventTsStart else "19700101000000"
    val eventTsEndFinal = if (eventTsEnd != null && eventTsEnd != "") eventTsEnd else "30000101000000"
    val sourceTypeFinal = "hdfs_txt"
    val specialConfig = if (sourceConfig.isDefined && sourceConfig.get != "" && sourceConfig.get.contains("protocol")) Some(base64byte2s(getConsumptionProtocol(sourceConfig.get).trim.getBytes())) else None
    val (instance, db, _) = modules.namespaceDal.getNsDetail(sourceNs)
    val hdfsRoot = RiderConfig.spark.remoteHdfsRoot match {
      case Some(_) => RiderConfig.spark.remoteHdfsActiveNamenodeHost.get
      case None => RiderConfig.spark.hdfsRoot
    }
    SourceConfig(eventTsStartFinal, eventTsEndFinal, sourceNs,
      ConnectionConfig(hdfsRoot, None, None, None),
      getSourceProcessClass(sourceTypeFinal), specialConfig)
  }

  def getSinkConfig(sinkNs: String, sinkConfig: String, jobType: String, eventTsEnd: String, tableKeys: Option[String]) = {
    val (instance, db, ns) = modules.namespaceDal.getNsDetail(sinkNs)

    val maxRecord =
      if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("maxRecordPerPartitionProcessed"))
        JSON.parseObject(sinkConfig).getIntValue("maxRecordPerPartitionProcessed")
      else RiderConfig.spark.jobMaxRecordPerPartitionProcessed

    val specialConfig =
      if (jobType != JobType.BACKFILL.toString) {
        if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_specific_config"))
          Some(base64byte2s(JSON.parseObject(sinkConfig).getString("sink_specific_config").trim.getBytes()))
        else None
      } else {
        val topicConfig = new JSONObject().fluentPut("topic", db.nsDatabase)
        val sinkSpecConfig = new JSONObject().fluentPut("sink_specific_config", topicConfig)
        Some(base64byte2s(sinkSpecConfig.toString.trim.getBytes))
      }

    //val sinkKeys = if (ns.nsSys == "hbase") Some(FlowUtils.getRowKey(specialConfig.get)) else ns.keys
    val sinkKeys = if (ns.nsSys == "hbase") Some(FlowUtils.getRowKey(specialConfig.get)) else tableKeys

    val projection = if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_output")) {
      Some(JSON.parseObject(sinkConfig).getString("sink_output").trim)
    } else {
      None
    }

    val sinkConnection =
      if (instance.nsSys != UmsDataSystem.PARQUET.toString)
        getConnConfig(instance, db)
      else {
        val endTs =
          if (eventTsEnd != null && eventTsEnd != "" && eventTsEnd != "30000101000000")
            eventTsEnd
          else currentNodSec
        val connUrl = getConnUrl(instance, db).stripSuffix("/") + "/" + sinkNs + "/" + endTs
        ConnectionConfig(connUrl, db.user, db.pwd, getDbConfig(instance.nsSys, db.config.getOrElse("")))
      }

    val sinkSys = if (jobType != JobType.BACKFILL.toString) ns.nsSys else UmsDataSystem.KAFKA.toString
    SinkConfig(sinkNs, sinkConnection, maxRecord, Some(getSinkProcessClass(sinkSys, ns.sinkSchema)), specialConfig, sinkKeys, projection)
  }

  def getTranConfig(tranConfig: String, sinkConfig: String, sinkNs: String, jobType: String, tableKeys: Option[String]) = {
    val sinkProtocol = getSinkProtocol(sinkConfig, jobType)
    val action =
      if (tranConfig != "" && tranConfig != null) {
        val tranClass = JSON.parseObject(tranConfig)
        if (tranClass.containsKey("action") && tranClass.getString("action").nonEmpty) {
          if (tranClass.getString("action").contains("edp.wormhole.sparkx.batchjob.transform.Snapshot"))
            tranClass.getString("action")
          else {
            if (sinkProtocol.nonEmpty && sinkProtocol.get == JobSinkProtocol.SNAPSHOT.toString)
              "custom_class = edp.wormhole.sparkx.batchjob.transform.Snapshot;".concat(tranClass.getString("action"))
            else tranClass.getString("action")
          }
        } else if (sinkProtocol.nonEmpty && sinkProtocol.get == JobSinkProtocol.SNAPSHOT.toString)
          "custom_class = edp.wormhole.sparkx.batchjob.transform.Snapshot;"
        else ""
      } else if (sinkProtocol.nonEmpty && sinkProtocol.get == JobSinkProtocol.SNAPSHOT.toString)
        "custom_class = edp.wormhole.sparkx.batchjob.transform.Snapshot;"
      else ""
    val specialConfig = setSwiftsConfig2Snapshot(sinkNs, action, tranConfig, tableKeys)
    if (action != "")
      Some(TransformationConfig(Some(base64byte2s(action.trim.getBytes)), specialConfig))
    else None
  }

  def setSwiftsConfig2Snapshot(sinkNs: String, action: String, tranConfig: String, tableKeys: Option[String]): Option[String] = {
    if (action.contains("edp.wormhole.sparkx.batchjob.transform.Snapshot")) {
      val ns = modules.namespaceDal.getNamespaceByNs(sinkNs).get
      if (tranConfig != null && tranConfig != "") {
        val tranClass = JSON.parseObject(tranConfig)
        val swiftsSpec =
          if (tranClass.containsKey("swifts_specific_config"))
            tranClass.getJSONObject("swifts_specific_config")
          else new JSONObject()
        if (!swiftsSpec.containsKey("table_keys")) {
          swiftsSpec.fluentPut("table_keys", tableKeys.getOrElse(""))
          tranClass.fluentPut("swifts_specific_config", swiftsSpec.toString)
        }
        Some(tranClass.getString("swifts_specific_config"))
      } else {
        val swiftsSpec = new JSONObject()
        swiftsSpec.fluentPut("table_keys", tableKeys.getOrElse(""))
        Some(swiftsSpec.toString)
      }
    } else None
  }

  def getJobConfig(name: String, othersConfig: Option[String]) = {
    val sqlShufflePartition =
      if (othersConfig != null && othersConfig.isDefined && othersConfig.get != "") {
        val index = othersConfig.get.indexOf("spark.sql.shuffle.partitions=")
        if (index >= 0) {
          riderLogger.info("getJobConfig contains spark.sql.shuffle.partitions=")
          val length = "spark.sql.shuffle.partitions=".size
          val lastPart = othersConfig.get.indexOf(",", index + length)
          val endIndex = if (lastPart < 0) othersConfig.get.length else lastPart
          Some(othersConfig.get.substring(index + length, endIndex).toInt)
        } else {
          riderLogger.info("getJobConfig DO NOT contains spark.sql.shuffle.partitions=")
          None
        }
      } else {
        riderLogger.info("getJobConfig is none. Do not contains spark.sql.shuffle.partitions=")
        None
      }
    JobConfig(name, "yarn-cluster", sqlShufflePartition)
  }

  def getSourceProcessClass(sourceType: String): String = {
    sourceType match {
      case "hdfs_txt" => "edp.wormhole.sparkx.batchjob.source.SourceHdfs"
      case _ =>
        riderLogger.error(s"this sourceType $sourceType isn't supported now")
        throw new Exception(s"this sourceType $sourceType isn't supported now")
    }
  }

  def getConsumptionProtocol(sourceConfig: String): String = {
    val protocol = JSON.parseObject(sourceConfig).getString("protocol")
    protocol match {
      case "increment" => "{\"initial\": false, \"increment\": true}"
      case "initial" => "{\"initial\": true, \"increment\": false}"
      case "all" => "{\"initial\": true, \"increment\": true}"
    }
  }


  def getConnConfig(instance: Instance, db: NsDatabase): ConnectionConfig = {
    ConnectionConfig(getConnUrl(instance, db), db.user, db.pwd, getDbConfig(instance.nsSys, db.config.getOrElse("")))
  }

  def startJob(job: Job, logPath: String) = {
    //    runShellCommand(s"rm -rf ${SubmitSparkJob.getLogPath(job.name)}")
    val startConfig: StartConfig = if (job.startConfig.isEmpty) null else json2caseClass[StartConfig](job.startConfig)
    val command = generateSparkStreamStartSh(s"'''${base64byte2s(caseClass2json(getBatchJobConfigConfig(job)).trim.getBytes)}'''", job.name, logPath,
      if (startConfig != null) startConfig else StartConfig(RiderConfig.spark.driverCores, RiderConfig.spark.driverMemory, RiderConfig.spark.executorNum, RiderConfig.spark.executorMemory, RiderConfig.spark.executorCores),
      job.sparkConfig.JVMDriverConfig.getOrElse(RiderConfig.spark.driverExtraConf),
      job.sparkConfig.JVMExecutorConfig.getOrElse(RiderConfig.spark.executorExtraConf),
      job.sparkConfig.othersConfig.getOrElse(""),
      "job"
    )
    riderLogger.info(s"start job ${job.id} command: $command")
    runShellCommand(command)
  }

  def genJobName(projectId: Long, sourceNs: String, sinkNs: String) = {
    val projectName = Await.result(modules.projectDal.findById(projectId), minTimeOut).head.name
    s"wormhole_${projectName}_job_${sourceNs}_${sinkNs}_$DateUtils.currentyyyyMMddHHmmss"
  }

  def refreshJob(id: Long) = {
    val job = Await.result(modules.jobDal.findById(id), minTimeOut).head
    val appInfo = getSparkJobStatus(job)
    modules.jobDal.updateJobStatus(job.id, appInfo, job.logPath.getOrElse(""))
    val startedTime = if (appInfo.startedTime != null) Some(appInfo.startedTime) else Some("")
    val stoppedTime = if (appInfo.finishedTime != null) Some(appInfo.finishedTime) else Some("")
    Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.jobType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
      job.sinkConfig, job.tranConfig, job.tableKeys, job.desc, appInfo.appState, Some(appInfo.appId), job.logPath, startedTime, stoppedTime, job.userTimeInfo)
  }

  def killJob(id: Long): String = {
    try {
      val job = refreshJob(id)
      try {
        if (job.status == "running" || job.status == "waiting") {
          val command = s"yarn application -kill ${job.sparkAppid.get}"
          riderLogger.info(s"stop job command: $command")
          runShellCommand(command)
          modules.jobDal.updateJobStatus(job.id, "stopping")
          "stopping"
        } else if (job.status == "failed") {
          modules.jobDal.updateJobStatus(job.id, "stopped")
          "stopped"
        }
        else job.status
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

  def getDisableAction(job: Job): String = {
    val projectNsSeq = modules.relProjectNsDal.getNsByProjectId(job.projectId)
    val nsSeq = new ListBuffer[String]
    nsSeq += job.sourceNs
    nsSeq += job.sinkNs
    var flag = true
    for (i <- nsSeq.indices) {
      if (!projectNsSeq.exists(_.startsWith(nsSeq(i))))
        flag = false
    }
    if (!flag) {
      if (job.status == "stopped") "modify,start,renew,stop"
      else "modify,start,renew"
    } else {
      JobStatus.jobStatus(job.status) match {
        case JobStatus.NEW => s"${Action.STOP}"
        case JobStatus.STARTING => s"${Action.START},${Action.STOP},${Action.DELETE}"
        case JobStatus.WAITING => s"${Action.START}"
        case JobStatus.RUNNING => s"${Action.START}"
        case JobStatus.STOPPING => s"${Action.START}"
        case JobStatus.FAILED => ""
        case JobStatus.STOPPED => s"${Action.STOP}"
        case JobStatus.DONE => s"${Action.STOP}"
      }
    }
  }

  def getJobSinkNs(sourceNs: String, sinkNs: String, jobType: String) = {
    if (jobType != JobType.BACKFILL.toString) sinkNs else sourceNs
  }

  def getSinkProtocol(sinkConfig: String, jobType: String): Option[String] = {
    if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_protocol"))
      Some(JSON.parseObject(sinkConfig).getString("sink_protocol"))
    else {
      //      if (jobType == JobType.BACKFILL.toString) Some(JobSinkProtocol.SNAPSHOT.toString)
      //      else
      None
    }
  }
}
