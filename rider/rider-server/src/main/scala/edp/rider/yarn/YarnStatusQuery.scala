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


package edp.rider.yarn

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.common._
import edp.rider.rest.persistence.entities.{FlinkJobStatus, FullJobInfo, Job}
import edp.rider.rest.util.JobUtils.getDisableAction
import edp.rider.yarn.YarnClientLog._
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.DtFormat
import edp.wormhole.util.JsonUtils._
import spray.json.JsonParser

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpResponse}


object YarnStatusQuery extends RiderLogger {

  def getSparkAllJobStatus(jobs: Seq[Job], map: Map[String, AppResult], projectName: String) = jobs.map(job => {
    val appInfo = mappingSparkJobStatus(job, map)
    modules.jobDal.updateJobStatus(job.id, appInfo, job.logPath.getOrElse(""))
    //    val startedTime = if (appInfo.startedTime != null) Some(appInfo.startedTime) else Some("")
    //    val stoppedTime = if (appInfo.finishedTime != null) Some(appInfo.finishedTime) else Some("")
    val newJob = Job(job.id, job.name, job.projectId, job.sourceNs, job.sinkNs, job.jobType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
      job.sinkConfig, job.tranConfig, job.tableKeys, job.desc, appInfo.appState, Some(appInfo.appId), job.logPath, Option(appInfo.startedTime), Option(appInfo.finishedTime), job.userTimeInfo)
    FullJobInfo(newJob, projectName, getDisableAction(newJob))
  })


  def getSparkList(job: Job) = {
    if (job.startedTime.getOrElse("") != "") getAllAppStatus(job.startedTime.get, Seq(job.name)) else Map.empty[String, AppResult]
  }

  def mappingSparkJobStatus(job: Job, sparkList: Map[String, AppResult]) = {
    val startedTime = job.startedTime.orNull
    val stoppedTime = job.stoppedTime.orNull
    val appInfo = getAppStatusByRest(sparkList, job.sparkAppid.getOrElse(""), job.name, job.status, startedTime, stoppedTime)
    val result = job.status match {
      case "starting" =>
        val logInfo = getAppStatusByLog(job.name, job.status, job.logPath.getOrElse(""))
        AppInfo(logInfo._1, logInfo._2, appInfo.startedTime, appInfo.finishedTime)
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

  def getSparkJobStatus(job: Job): AppInfo = {
    val sparkList = getSparkList(job)
    mappingSparkJobStatus(job, sparkList)
  }

  def getAllAppStatus(fromTime: String, appNames: Seq[String]): Map[String, AppResult] = {
    getAllYarnAppStatus(fromTime, appNames)
  }

  def getAppStatusByRest(map: Map[String, AppResult], appId: String, appName: String, curStatus: String, startedTime: String, stoppedTime: String): AppInfo = {
    var result = AppResult(appId, appName, curStatus, "", startedTime, stoppedTime)
    if (map.contains(appName)) {
      val yarnApp = map(appName)
      if (result.startedTime == null || yyyyMMddHHmmss(yarnApp.startedTime) >= yyyyMMddHHmmss(result.startedTime))
        result = yarnApp
    } else {
      riderLogger.debug("refresh spark/yarn api response is null")
    }

    if (result.finalStatus != null && result.finalStatus == YarnAppStatus.SUCCEEDED.toString)
      AppInfo(result.appId, StreamStatus.DONE.toString, result.startedTime, result.finishedTime)
    else
      AppInfo(result.appId, result.appStatus, result.startedTime, result.finishedTime)
  }

  def getAllYarnAppStatus(fromTime: String, appNames: Seq[String]): Map[String, AppResult] = {
    val fromTimeLong =
      if (fromTime == "") 0
      else if (fromTime.length > 19) dt2long(fromTime) / 1000
      else if (fromTime.length < 19) dt2long(fromTime)
    val rmUrl = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    if (rmUrl != "") {
      val url = s"http://${rmUrl.stripPrefix("http://").stripSuffix("/")}/ws/v1/cluster/apps?states=accepted,running,killed,failed,finished&&startedTimeBegin=$fromTimeLong&&applicationTypes=spark,apache%20flink"
      riderLogger.info(s"Spark Application refresh yarn rest url: $url")
      queryAppListOnYarn(url, appNames)
    } else Map.empty[String, AppResult]
  }

  private def queryAppListOnYarn(url: String, appNames: Seq[String]): Map[String, AppResult] = {
    var resultMap = Map.empty[String, AppResult]
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      resultMap = queryAppOnYarn(response, appNames)
    } catch {
      case e: Exception =>
        riderLogger.error(s"Spark Application refresh yarn rest url $url failed", e)
    }
    resultMap
  }

  private def queryAppOnYarn(response: HttpResponse[String], appNames: Seq[String]): Map[String, AppResult] = {
    val resultMap = HashMap.empty[String, AppResult]
    try {
      val json = JsonParser.apply(response.body).toString()
      if (JSON.parseObject(json).containsKey("apps")) {
        val app = JSON.parseObject(json).getString("apps")
        if (app != "" && app != null && JSON.parseObject(app).containsKey("app")) {
          val appSeq = JSON.parseObject(app).getJSONArray("app")
          for (i <- 0 until appSeq.size()) {
            val info = appSeq.getString(i)
            val name = JSON.parseObject(info).getString("name")
            val startedTime = formatTime(JSON.parseObject(info).getLong("startedTime"))
            if (appNames.contains(name)) {
              if (resultMap.contains(name)) {
                if (startedTime > resultMap(name).startedTime) {
                  resultMap(name) = AppResult(JSON.parseObject(info).getString("id"),
                    name,
                    JSON.parseObject(info).getString("state"),
                    JSON.parseObject(info).getString("finalStatus"),
                    startedTime, formatTime(JSON.parseObject(info).getLong("finishedTime"))
                  )
                }
              } else {
                resultMap(name) = AppResult(JSON.parseObject(info).getString("id"),
                  name,
                  JSON.parseObject(info).getString("state"),
                  JSON.parseObject(info).getString("finalStatus"),
                  startedTime, formatTime(JSON.parseObject(info).getLong("finishedTime"))
                )
              }
            }
          }
        }
      }
      resultMap.toMap
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Spark Application refresh yarn rest api response failed", ex)
        resultMap.toMap
    }
  }


  private def queryAppListOnStandalone(response: HttpResponse[String]): List[AppResult] = {
    val resultList: ListBuffer[AppResult] = new ListBuffer()
    val masterStateResponse: MasterStateResponse = json2caseClass[MasterStateResponse](response.body)
    masterStateResponse.activeApps.foreach(m => resultList.append(AppResult(m.id, m.name, m.state, m.finalState, m.startTime.toString, m.duration.toString)))
    resultList.toList
  }

  def getActiveResourceManager(rm1: String, rm2: String): String = {
    if (getResourceManagerHaState(rm1).toUpperCase == "ACTIVE")
      rm1
    else if (getResourceManagerHaState(rm2).toUpperCase == "ACTIVE")
      rm2
    else {
      riderLogger.error(s"query yarn resourceManager haState failed caused by both resourceManager url is empty.")
      ""
    }
  }

  def getResourceManagerHaState(rm: String): String = {
    val url = s"http://${rm.stripPrefix("http://").stripPrefix("/")}/ws/v1/cluster/info"
    try {
      if (rm != null && rm != "") {
        val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
        JSON.parseObject(response.body).getJSONObject("clusterInfo").getString("haState")
      } else {
        riderLogger.warn(s"query yarn resourceManager haState failed caused by one resourceManager url is empty.")
        ""
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"query yarn resourceManager $url haState failed", ex)
        ""
    }
  }

  def getJobManagerAddressOnYarn(appId: String): String = {
    val activeRm = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    val url = s"http://$activeRm/proxy/$appId/jars"
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      val json = JsonParser.apply(response.body).toString()
      JSON.parseObject(json).getString("address").split("//")(1).trim
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Get Flink JobManager address failed by request url $url", ex)
        throw ex
    }
  }

  def getFlinkJobStatusOnYarn(appIds: Seq[String]): Map[String, FlinkJobStatus] = {
    val activeRm = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    val flinkJobMap = HashMap.empty[String, FlinkJobStatus]
    appIds.foreach {
      appId =>
        val url = s"http://$activeRm/proxy/$appId/jobs/overview"
        try {
          val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
          val json = JsonParser.apply(response.body).toString()
          val jobs = JSON.parseObject(json).getJSONArray("jobs")
          for (i <- 0 until jobs.size) {
            val jobJson = JSON.parseObject(jobs.getString(i))
            val jobName = jobJson.getString("name")
            val jobId = jobJson.getString("jid")
            val startTime = formatTime(jobJson.getLong("start-time"))
            val stopTime = formatTime(jobJson.getLong("end-time"))
            val state = jobJson.getString("state")
            if (flinkJobMap.contains(jobName)) {
              if (startTime > flinkJobMap(jobName).startTime)
                flinkJobMap(jobName) = FlinkJobStatus(jobName, jobId, state, startTime, stopTime)
            } else
              flinkJobMap.put(jobName, FlinkJobStatus(jobName, jobId, state, startTime, stopTime))
          }
        } catch {
          case ex: Exception =>
            riderLogger.error(s"Get Flink job status failed by request url $url", ex)
            throw ex
        }
    }
    flinkJobMap.toMap
  }

  private def formatTime(time: Long): String = {
    if (time != 0 && time != -1)
      dt2string(dt2date(time * 1000), DtFormat.TS_DASH_SEC)
    else null
  }
}
