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
import edp.rider.common.StreamStatus.STARTING
import edp.rider.common._
import edp.rider.rest.persistence.entities.{Instance, Job, NsDatabase, StartConfig}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.FlowUtils._
import edp.rider.rest.util.NamespaceUtils._
import edp.rider.rest.util.NsDatabaseUtils._
import edp.rider.wormhole._
import edp.rider.yarn.ShellUtils
import edp.rider.yarn.SubmitYarnJob._
import edp.rider.yarn.YarnClientLog.getAppStatusByLog
import edp.rider.yarn.YarnStatusQuery.getAppStatusByRest
import edp.wormhole.externalclient.hadoop.HdfsUtils._
import edp.wormhole.ums.UmsDataSystem
import edp.wormhole.util.CommonUtils._
import edp.wormhole.util.JsonUtils._
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import edp.wormhole.util.{DateUtils, FileUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

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
        if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_specific_config")) {
          val sinkSpecConfig = JSON.parseObject(sinkConfig).getJSONObject("sink_specific_config")
          val sinkConfigRe = new JSONObject().fluentPut("sink_specific_config", sinkSpecConfig)
          Some(base64byte2s(sinkConfigRe.toString.trim.getBytes()))
        }
        else None
      } else {
        val sinkSpecConfig =
          if (sinkConfig != "" && sinkConfig != null && JSON.parseObject(sinkConfig).containsKey("sink_specific_config")) {
            JSON.parseObject(sinkConfig).getJSONObject("sink_specific_config")
          } else {
            new JSONObject()
          }
        if(!sinkSpecConfig.containsKey("kerberos")) {
          val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(instance.connConfig.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
          sinkSpecConfig.fluentPut("kerberos", inputKafkaKerberos)
        }
        if(!sinkSpecConfig.containsKey("sink_uid")) {
          sinkSpecConfig.fluentPut("sink_uid", true)
        }
        sinkSpecConfig.fluentPut("topic", db.nsDatabase)
        val sinkConfigRe = new JSONObject().fluentPut("sink_specific_config", sinkSpecConfig)
        Some(base64byte2s(sinkConfigRe.toString.trim.getBytes))
      }

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
    SinkConfig(sinkNs, sinkConnection, maxRecord, Some(getSinkProcessClass(sinkSys, ns.sinkSchema, None)), specialConfig, sinkKeys, projection)
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

  def startJob(job: Job, logPath: String): (Boolean, Option[String]) = {
    val startConfig: StartConfig = if (job.startConfig.isEmpty) null else json2caseClass[StartConfig](job.startConfig)
    val command = generateSparkStreamStartSh(s"'''${base64byte2s(caseClass2json(getBatchJobConfigConfig(job)).trim.getBytes)}'''", job.name, logPath,
      if (startConfig != null) startConfig else StartConfig(RiderConfig.spark.driverCores, RiderConfig.spark.driverMemory, RiderConfig.spark.executorNum, RiderConfig.spark.executorMemory, RiderConfig.spark.executorCores),
      job.sparkConfig.JVMDriverConfig.getOrElse(RiderConfig.spark.driverExtraConf),
      job.sparkConfig.JVMExecutorConfig.getOrElse(RiderConfig.spark.executorExtraConf),
      job.sparkConfig.othersConfig.getOrElse(""),
      "job"
    )
    riderLogger.info(s"start job ${job.id} command: $command")
    ShellUtils.runShellCommand(command, logPath)
  }

  def getLogPath(appName: String) = s"${RiderConfig.spark.clientLogRootPath}/jobs/$appName-${CommonUtils.currentNodSec}.log"

  def genJobName(projectId: Long, sourceNs: String, sinkNs: String) = {
    val projectName = Await.result(modules.projectDal.findById(projectId), minTimeOut).head.name
    s"wormhole_${projectName}_job_${sourceNs}_${sinkNs}_$DateUtils.currentyyyyMMddHHmmss"
  }

  def refreshJob(id: Long) = {
    Await.result(modules.jobDal.findById(id), minTimeOut).head
    /*val appInfo = getSparkJobStatus(job)
    modules.jobDal.updateJobStatus(job.id, appInfo, job.logPath.getOrElse(""))
    val startedTime = if (appInfo.startedTime != null) Some(appInfo.startedTime) else Some("")
    val stoppedTime = if (appInfo.finishedTime != null) Some(appInfo.finishedTime) else Some("")
    Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.jobType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
      job.sinkConfig, job.tranConfig, job.tableKeys, job.desc, appInfo.appState, Some(appInfo.appId), job.logPath, startedTime, stoppedTime, job.userTimeInfo)*/
  }

  def killJob(id: Long): (String, Boolean) = {
    try {
      val job = refreshJob(id)
      try {
        if (job.status == "running" || job.status == "waiting" || job.status == "stopping") {
          val command = s"yarn application -kill ${job.sparkAppid.get}"
          riderLogger.info(s"stop job command: $command")
          val stopSuccess = runYarnKillCommand(command)
          if (stopSuccess) {
            modules.jobDal.updateJobStatus(job.id, "stopping")
            ("stopping", true)
          } else {
            (job.status, false)
          }
        } else if (job.status == "failed") {
          modules.jobDal.updateJobStatus(job.id, "stopped")
          ("stopped", true)
        }
        else (job.status, true)
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
    val sorceNsSeq = job.sourceNs.split("\\.")
    val sinkNsSeq = job.sinkNs.split("\\.")
    nsSeq += sorceNsSeq(0) + "." + sorceNsSeq(1) + "." + sorceNsSeq(2) + "." + sorceNsSeq(3) + ".*" + ".*" + ".*"
    nsSeq += sinkNsSeq(0) + "." + sinkNsSeq(1) + "." + sinkNsSeq(2) + "." + sinkNsSeq(3) + ".*" + ".*" + ".*"
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
      None
    }
  }

  def getHdfsDataVersions(namespace: String): String = {
    val hdfsRoot = RiderConfig.spark.remoteHdfsRoot match {
      case Some(_) => RiderConfig.spark.remoteHdfsActiveNamenodeHost.get
      case None => RiderConfig.spark.hdfsRoot
    }
    val configuration = setConfiguration(hdfsRoot, None)
    val names = namespace.split("\\.")
    val hdfsPath = hdfsRoot + "/hdfslog/" + names(0).toLowerCase + "." + names(1).toLowerCase + "." + names(2).toLowerCase + "/" + names(3).toLowerCase
    val hdfsFileList = getHdfsFileList(configuration, hdfsPath)
    if (hdfsFileList != null) hdfsFileList.map(t => t.substring(t.lastIndexOf("/") + 1).toInt).sortWith(_ > _).mkString(",")
    else ""
  }

  def getHdfsFileList(config: Configuration, hdfsPath: String): Seq[String] = {
    val fileSystem = FileSystem.newInstance(config)
    val fullPath = FileUtils.pfRight(hdfsPath)
    riderLogger.info(s"hdfs data path: $fullPath")

//    if(RiderConfig.kerberos.kafkaEnabled) {
//      UserGroupInformation.setConfiguration(config)
//      UserGroupInformation.loginUserFromKeytab(RiderConfig.kerberos.sparkPrincipal, RiderConfig.kerberos.sparkKeyTab)
//    }
    val fileList =
      if (isPathExist(config, fullPath)) fileSystem.listStatus(new Path(fullPath)).map(_.getPath.toString).toList
      else null
    fileSystem.close()
    fileList
  }

  def setConfiguration(hdfsPath: String, connectionConfig: Option[Seq[KVConfig]]): Configuration = {
    var sourceNamenodeHosts = null.asInstanceOf[String]
    var sourceNamenodeIds = null.asInstanceOf[String]
    if (connectionConfig.nonEmpty) connectionConfig.get.foreach(param => {
      if (param.key == "hdfs_namenode_hosts") sourceNamenodeHosts = param.value
      if (param.key == "hdfs_namenode_ids") sourceNamenodeIds = param.value
    })

    val hadoopHome = System.getenv("HADOOP_HOME")
    val configuration = new Configuration(false)
    configuration.addResource(new Path(s"$hadoopHome/conf/core-site.xml"))
    configuration.addResource(new Path(s"$hadoopHome/conf/hdfs-site.xml"))

    val defaultFS = configuration.get("fs.defaultFS")
    riderLogger.info(s"hadoopHome is $hadoopHome, defaultFS is $defaultFS")

    val hdfsPathGrp = hdfsPath.split("//")
    val hdfsRoot = if (hdfsPathGrp(1).contains("/")) hdfsPathGrp(0) + "//" + hdfsPathGrp(1).substring(0, hdfsPathGrp(1).indexOf("/")) else hdfsPathGrp(0) + "//" + hdfsPathGrp(1)
    configuration.set("fs.defaultFS", hdfsRoot)

//    if(RiderConfig.kerberos.kafkaEnabled) {
//      configuration.set("hadoop.security.authentication", "kerberos")
//    }

    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    //configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    if (sourceNamenodeHosts != null) {
      val clusterName = hdfsRoot.split("//")(1)
      configuration.set("dfs.nameservices", clusterName)
      configuration.set(s"dfs.ha.namenodes.$clusterName", sourceNamenodeIds)
      val namenodeAddressSeq = sourceNamenodeHosts.split(",")
      val namenodeIdSeq = sourceNamenodeIds.split(",")
      for (i <- 0 until namenodeAddressSeq.length) {
        configuration.set(s"dfs.namenode.rpc-address.$clusterName." + namenodeIdSeq(i), namenodeAddressSeq(i))
      }
      configuration.set(s"dfs.client.failover.proxy.provider.$clusterName", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    }
    configuration
  }

  def mappingSparkJobStatus(job: Job, sparkList: Map[String, AppResult]) = {
    val startedTime = job.startedTime.orNull
    val stoppedTime = job.stoppedTime.orNull
    val appStatus = getAppStatusByRest(sparkList, job.sparkAppid.getOrElse(""), job.name, job.status, startedTime, stoppedTime)

    val endAction=if (job.status == STARTING.toString) "refresh_log"
    else "refresh_spark"

    val appInfo= endAction match {
      case "refresh_log" =>
          val logInfo = getAppStatusByLog(job.name, job.status, job.logPath.getOrElse(""), job.sparkAppid.getOrElse(""))
          logInfo._2 match {
            case "starting" => getAppStatusByRest(sparkList, logInfo._1, job.name, logInfo._2, startedTime, stoppedTime)
            case "failed" => AppInfo(logInfo._1, "failed", startedTime, currentSec)
          }
      case "refresh_spark" =>
          appStatus
    }

    val result = job.status match {
      case "starting" =>
        appInfo.appState.toUpperCase match {
          case "STARTING" => AppInfo(appInfo.appId, "starting", appInfo.startedTime, appInfo.finishedTime)
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "WAITING" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FINISHED" | "FAILED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "waiting" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "WAITING" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FINISHED" | "FAILED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "running" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FAILED" | "FINISHED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "stopping" =>
        appInfo.appState.toUpperCase match {
          case "STOPPING" => AppInfo(appInfo.appId, "stopping", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FAILED" | "FINISHED" => AppInfo(appInfo.appId, "stopped", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
          case _ => AppInfo(appInfo.appId, "stopping", appInfo.startedTime, appInfo.finishedTime)
        }
      case "stopped" =>
        appInfo.appState.toUpperCase match {
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
          case _ => AppInfo(job.sparkAppid.getOrElse(""), "stopped", startedTime, stoppedTime)
        }
      case _ => AppInfo(job.sparkAppid.getOrElse(""), job.status, startedTime, stoppedTime)
    }
    result
  }

  def getJobTime(time: Option[String]) = {
    val timeValue = time.getOrElse("")
    if (timeValue.nonEmpty) timeValue.split("\\.")(0) else null
  }

  def hidePid(job: Job): Job = {
    if(job != null && job.status == "starting") {
      Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.jobType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
        job.sinkConfig, job.tranConfig, job.tableKeys, job.desc, job.status, None, job.logPath, job.startedTime, job.stoppedTime, job.userTimeInfo)
    } else job
  }

  def hidePid(jobs: Seq[Job]): Seq[Job] = {
    if(jobs != null && jobs.nonEmpty) {
      jobs.map(job => hidePid(job))
    } else jobs
  }
}
