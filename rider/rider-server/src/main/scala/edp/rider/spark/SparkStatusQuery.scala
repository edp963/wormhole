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

import edp.rider.RiderStarter.modules.config
import edp.rider.common._
import edp.rider.rest.persistence.entities.Job
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.DtFormat
import edp.wormhole.common.util.JsonUtils._
import spray.json.JsonParser
import edp.rider.common.SparkRiderStatus._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON
import scalaj.http.{Http, HttpResponse}
import edp.rider.spark.SparkJobClientLog._


object SparkStatusQuery extends RiderLogger {

  def getSparkJobStatus(job: Job): AppInfo = {
    val sparkList = if (job.startedTime.orNull.nonEmpty) getAllAppStatus(job.startedTime.get).sortBy(_.appId) else List()
    val startedTime = job.startedTime.orNull
    val stoppedTime = job.stoppedTime.orNull
    val appInfo = getAppStatusByRest(sparkList, job.name, job.status, startedTime, job.updateTime)
    val result = job.status match {
      case "starting" =>
        val status = getAppStatusByLog(job.name, job.status)
        riderLogger.info(s"job log status: $status")
        if (status.toUpperCase == "WAITING" || status.toUpperCase == "RUNNING" || status.toUpperCase == "STARTING") {
          appInfo.appState.toUpperCase match {
            case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
            case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
            case "KILLED" | "FINISHED" | "FAILED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
            case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
            case "STARTING" | "WAITING" => AppInfo(null, status, startedTime, appInfo.finishedTime)
          }
        } else AppInfo(null, status, startedTime, appInfo.finishedTime)
      case "waiting" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "ACCEPTED" => AppInfo(appInfo.appId, "waiting", appInfo.startedTime, appInfo.finishedTime)
          case "WAITING" => AppInfo(null, "waiting", startedTime, appInfo.finishedTime)
          case "KILLED" | "FINISHED" | "FAILED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "running" =>
        appInfo.appState.toUpperCase match {
          case "RUNNING" => AppInfo(appInfo.appId, "running", appInfo.startedTime, appInfo.finishedTime)
          case "KILLED" | "FAILED" | "FINISHED" => AppInfo(appInfo.appId, "failed", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "stopping" =>
        appInfo.appState.toUpperCase match {
          case "KILLED" | "FAILED" | "FINISHED" => AppInfo(appInfo.appId, "stopped", appInfo.startedTime, appInfo.finishedTime)
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
        }
      case "stopped" =>
        appInfo.appState.toUpperCase match {
          case "DONE" => AppInfo(appInfo.appId, "done", appInfo.startedTime, appInfo.finishedTime)
          case _ => AppInfo(job.sparkAppid.getOrElse(""), job.status, startedTime, stoppedTime)
        }
      case _ => AppInfo(job.sparkAppid.getOrElse(""), job.status, startedTime, stoppedTime)
    }
    riderLogger.info(s"job get status: $result")
    result
  }

  def getAllAppStatus(fromTime: String): List[AppResult] = {
    //    if ("true".equals(config.getString("spark.mode.standalone")))
    //      getAllStandaloneAppStatus
    //    else
    getAllYarnAppStatus(fromTime)
  }

  def getAppStatusByRest(appList: List[AppResult], appName: String, curStatus: String, startedTime: String, updateTime: String): AppInfo = {
    var result = AppResult(null, appName, curStatus, startedTime, null, null)
    appList.foreach {
      app =>
        if (app.appName == appName) {
          sparkRiderStatus(curStatus) match {
            case SparkRiderStatus.STARTING | SparkRiderStatus.WAITING =>
              if (dt2long(app.startedTime) >= dt2long(updateTime)) result = app else result
            case _ => result = app
          }
        } else {
          riderLogger.debug("refresh spark/yarn api response is null")
          result
        }
    }
    if (result.finalStatus != null && result.finalStatus == SparkAppStatus.SUCCEEDED.toString)
      AppInfo(result.appId, SparkRiderStatus.DONE.toString, result.startedTime, result.finishedTime)
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
      else if (fromTime.length > 19) dt2long(fromTime)
    val url = s"http://${RiderConfig.spark.rest_api}/ws/v1/cluster/apps?states=accepted,running,killed,failed,finished&&startedTimeBegin=$fromTimeLong&&applicationTags=${RiderConfig.spark.app_tags}&&applicationTypes=spark"
    riderLogger.info(s"yarn url: $url")
    val resultList = queryAppListOnYarn(url)
    resultList
  }

  private def queryAppListOnYarn(url: String): List[AppResult] = {
    var resultList: List[AppResult] = Nil
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      resultList = this.queryAppOnYarn(response)
    } catch {
      case e: Exception =>
        riderLogger.error(s"request yarn rest api $url failed", e)
    }
    resultList
  }

  private def queryAppOnYarn(response: HttpResponse[String]): List[AppResult] = {
    val resultList: List[AppResult] = List()
    val json = JsonParser.apply(response.body).asJsObject
    if (response.body.indexOf("apps") > 0) {
      val apps: String = json.fields("apps").toString()
      try {
        val languages = JSON.parseFull(apps) match {
          case Some(x) =>
            val m = x.asInstanceOf[Map[String, List[Map[String, Any]]]]
            m("app") map {
              appList =>
                AppResult(appList("id").toString, appList("name").toString, appList("state").toString, appList("finalStatus").toString, dt2string(dt2date(BigDecimal(appList("startedTime").toString).toLong * 1000), DtFormat.TS_DASH_SEC).toString,
                  if (BigDecimal(appList("finishedTime").toString).toLong == 0) null
                  else dt2string(dt2date(BigDecimal(appList("finishedTime").toString).toLong * 1000), DtFormat.TS_DASH_SEC).toString)
            }
          case None => Nil
        }
        languages
      }
      catch {
        case e: Exception =>
          riderLogger.error(s"parse yarn rest api response failed", e)
          resultList
      }
    } else {
      val id = json.fields("app").asJsObject.fields("id").toString()
      val name = json.fields("app").asJsObject.fields("name").toString()
      val state = json.fields("app").asJsObject.fields("state").toString()
      val finalState = json.fields("app").asJsObject.fields("finalStatus").toString()
      val startedTime = yyyyMMddHHmmss(json.fields("app").asJsObject.fields("startedTime").toString().toLong).toString
      val finishedTime = yyyyMMddHHmmss(json.fields("app").asJsObject.fields("finishedTime").toString().toLong).toString
      resultList.::(AppResult(id, name, state, finalState, startedTime, finishedTime))
    }
  }


  private def queryAppListOnStandalone(response: HttpResponse[String]): List[AppResult] = {
    val resultList: ListBuffer[AppResult] = new ListBuffer()
    val masterStateResponse: MasterStateResponse = json2caseClass[MasterStateResponse](response.body)
    masterStateResponse.activeApps.foreach(m => resultList.append(AppResult(m.id, m.name, m.state, m.finalState, m.startTime.toString, m.duration.toString)))
    resultList.toList
  }

}
