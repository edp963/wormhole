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


package edp.rider.spark

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.common
import edp.rider.common.StreamStatus._
import edp.rider.common._
import edp.rider.rest.persistence.entities.{FullJobInfo, Job}
import edp.rider.rest.util.JobUtils.getDisableAction
import edp.rider.spark.SparkJobClientLog._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.DtFormat
import edp.wormhole.common.util.JsonUtils._
import spray.json.JsonParser

import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpResponse}


object SparkStatusQuery extends RiderLogger {

  def getSparkAllJobStatus(jobs: Seq[Job], sparkList: List[AppResult], projectName: String) = jobs.map(job => {
    val appInfo = mappingSparkJobStatus(job, sparkList)
    modules.jobDal.updateJobStatus(job.id, appInfo)
    val startedTime = if (appInfo.startedTime != null) Some(appInfo.startedTime) else Some("")
    val stoppedTime = if (appInfo.finishedTime != null) Some(appInfo.finishedTime) else Some("")
    FullJobInfo(Job(job.id, job.name, job.projectId, job.sourceType, job.sinkNs, job.sourceType, job.sparkConfig, job.startConfig, job.eventTsStart, job.eventTsEnd, job.sourceConfig,
      job.sinkConfig, job.tranConfig, appInfo.appState, Some(appInfo.appId), job.logPath, startedTime, stoppedTime, job.createTime, job.createBy, job.updateTime, job.updateBy)
    , projectName,getDisableAction(JobStatus.jobStatus(job.status)))
  })


  def getSparkList(job: Job) = {
    if (job.startedTime.getOrElse("") != "") getAllAppStatus(job.startedTime.get).sortBy(_.appId) else List()
  }

  def mappingSparkJobStatus(job: Job, sparkList: List[AppResult]) = {
    val startedTime = job.startedTime.orNull
    val stoppedTime = job.stoppedTime.orNull
    val appInfo = getAppStatusByRest(sparkList, job.name, job.status, startedTime, stoppedTime)
    val result = job.status match {
      case "starting" =>
        val logInfo = getAppStatusByLog(job.name, job.status)
        riderLogger.info(s"job log status: $logInfo._2")
        if (logInfo._2.toUpperCase == "WAITING" || logInfo._2.toUpperCase == "RUNNING" || logInfo._2.toUpperCase == "STARTING") {
          appInfo.appState.toUpperCase match {
            case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
            case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
            case "KILLED" | "FINISHED" | "FAILED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
            case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
            case "STARTING" => AppInfo(logInfo._1, logInfo._2, startedTime, appInfo.finishedTime)
            case "WAITING" => AppInfo(appInfo.appId, logInfo._2, startedTime, appInfo.finishedTime)
          }
        } else AppInfo(logInfo._1, logInfo._2, startedTime, appInfo.finishedTime)
      case "waiting" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "WAITING" => AppInfo(appInfo.appId, "waiting", startedTime, appInfo.finishedTime)
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
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FAILED" | "FINISHED" => AppInfo(appInfo.appId, "stopped", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "stopped" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
          case _ => AppInfo(job.sparkAppid.getOrElse(""), job.status, startedTime, stoppedTime)
        }
      case _ => AppInfo(job.sparkAppid.getOrElse(""), job.status, startedTime, stoppedTime)
    }
    result
  }

  def getSparkJobStatus(job: Job): AppInfo = {
    val sparkList = getSparkList(job)
    mappingSparkJobStatus(job, sparkList)
  }

  def getAllAppStatus(fromTime: String): List[AppResult] = {
    //    if ("true".equals(config.getString("spark.mode.standalone")))
    //      getAllStandaloneAppStatus
    //    else
    getAllYarnAppStatus(fromTime)
  }

  def getAppStatusByRest(appList: List[AppResult], appName: String, curStatus: String, startedTime: String, stoppedTime: String): AppInfo = {
    var result = AppResult("", appName, curStatus, "", startedTime, stoppedTime)
    appList.foreach {
      app =>
        if (app.appName == appName) {
          if (yyyyMMddHHmmss(app.startedTime) >= yyyyMMddHHmmss(result.startedTime))
            result = app
        } else {
          riderLogger.debug("refresh spark/yarn api response is null")
        }
    }
    if (result.finalStatus != null && result.finalStatus == SparkAppStatus.SUCCEEDED.toString)
      AppInfo(result.appId, StreamStatus.DONE.toString, result.startedTime, result.finishedTime)
    else
      AppInfo(result.appId, result.appStatus, result.startedTime, result.finishedTime)
  }

  //  def getAllStandaloneAppStatus: List[AppResult] = {
  //    var resultList: List[AppResult] = Nil
  //    val host = config.getString("spark.mode.master.apps")
  //    val url = s"http://$host/json"
  //    try {
  //      val response = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
  //      resultList = queryAppListOnStandalone(response)
  //    } catch {
  //      case e: Exception =>
  //        riderLogger.error(s"request yarn rest api $url failed", e)
  //    }
  //    resultList
  //  }

  def getAllYarnAppStatus(fromTime: String): List[AppResult] = {
    val fromTimeLong =
      if (fromTime == "") 0
      else if (fromTime.length > 19) dt2long(fromTime) / 1000
      else if (fromTime.length < 19) dt2long(fromTime)
    val rmUrl = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    //    riderLogger.info(s"active resourceManager: $rmUrl")
    if (rmUrl != "") {
      val url = s"http://${rmUrl.stripPrefix("http://").stripSuffix("/")}/ws/v1/cluster/apps?states=accepted,running,killed,failed,finished&&startedTimeBegin=$fromTimeLong&&applicationTags=${RiderConfig.spark.app_tags}&&applicationTypes=spark"
      riderLogger.info(s"Spark Application refresh yarn rest url: $url")
      queryAppListOnYarn(url)
    } else List()
  }

  private def queryAppListOnYarn(url: String): List[AppResult] = {
    var resultList: List[AppResult] = Nil
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      resultList = this.queryAppOnYarn(response)
    } catch {
      case e: Exception =>
        riderLogger.error(s"Spark Application refresh yarn rest url $url failed", e)
    }
    resultList
  }

  private def queryAppOnYarn(response: HttpResponse[String]): List[AppResult] = {
    val resultList = new ListBuffer[AppResult]
    try {
      val json = JsonParser.apply(response.body).toString()
      if (JSON.parseObject(json).containsKey("apps")) {
        val app = JSON.parseObject(json).getString("apps")
        if (app != "" && app != null && JSON.parseObject(app).containsKey("app")) {
          val appSeq = JSON.parseObject(app).getJSONArray("app")
          for (i <- 0 until appSeq.size()) {
            val info = appSeq.getString(i)
            val stopTime =
              if (JSON.parseObject(info).getLong("finishedTime") == 0) null
              else dt2string(dt2date(JSON.parseObject(info).getLong("finishedTime") * 1000), DtFormat.TS_DASH_SEC)
            resultList += AppResult(JSON.parseObject(info).getString("id"),
              JSON.parseObject(info).getString("name"),
              JSON.parseObject(info).getString("state"),
              JSON.parseObject(info).getString("finalStatus"),
              dt2string(dt2date(JSON.parseObject(info).getLong("startedTime") * 1000), DtFormat.TS_DASH_SEC),
              stopTime
            )
          }
        }
      }
      resultList.toList
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Spark Application refresh yarn rest api response failed", ex)
        resultList.toList
    }

    //      try {
    //        println(s"yan response: $apps")
    //        val languages = scala.util.parsing.json.JSON.parseFull(apps) match {
    //          case Some(x) =>
    //            println(s"yarn response parse: $x")
    //            val m = x.asInstanceOf[Map[String, List[Map[String, Any]]]]
    //            m("app") map {
    //              appList =>
    //                AppResult(appList("id").toString, appList("name").toString, appList("state").toString, appList("finalStatus").toString, dt2string(dt2date(BigDecimal(appList("startedTime").toString).toLong * 1000), DtFormat.TS_DASH_SEC).toString,
    //                  if (BigDecimal(appList("finishedTime").toString).toLong == 0) null
    //                  else dt2string(dt2date(BigDecimal(appList("finishedTime").toString).toLong * 1000), DtFormat.TS_DASH_SEC).toString)
    //            }
    //          case None => Nil
    //        }
    //        languages
    //      }
    //      catch {
    //        case e: Exception =>
    //          riderLogger.error(s"Spark Application refresh yarn rest api response failed", e)
    //          resultList
    //      }
    //    } else {
    //      val id = json.fields("app").asJsObject.fields("id").toString()
    //      val name = json.fields("app").asJsObject.fields("name").toString()
    //      val state = json.fields("app").asJsObject.fields("state").toString()
    //      val finalState = json.fields("app").asJsObject.fields("finalStatus").toString()
    //      val startedTime = yyyyMMddHHmmss(json.fields("app").asJsObject.fields("startedTime").toString().toLong).toString
    //      val finishedTime = yyyyMMddHHmmss(json.fields("app").asJsObject.fields("finishedTime").toString().toLong).toString
    //      resultList.::(AppResult(id, name, state, finalState, startedTime, finishedTime))
    //    }
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

}
