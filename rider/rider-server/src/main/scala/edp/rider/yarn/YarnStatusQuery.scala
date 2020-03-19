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
import edp.rider.RiderStarter.modules._
import edp.rider.common._
import edp.rider.rest.persistence.entities.{FlinkJobStatus, Job, Stream}
import edp.rider.rest.util.CommonUtils.minTimeOut
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.DtFormat
import edp.wormhole.util.JsonUtils._
import spray.json.JsonParser
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent.Await
import scalaj.http.{Http, HttpResponse}
import slick.jdbc.MySQLProfile.api._

object YarnStatusQuery extends RiderLogger {

  def updateStatusByYarn(): Unit = {
    val streams: Seq[Stream] = streamDal.getStreamSeq(None, None)
    val streamsNameSet = if (streams != null && streams.nonEmpty) streams.map(_.name) else Seq()
    val jobs = jobDal.getAllJobs
    val jobsNameSet = if (jobs != null && jobs.nonEmpty) jobs.map(_.name) else Seq()

    val nameSet = streamsNameSet ++ jobsNameSet
    val fromTime = getYarnFromTime(streams, jobs)
    val appInfoMap: Map[String, AppResult] = if (fromTime == "") Map.empty[String, AppResult] else getAllYarnAppStatus(fromTime, nameSet)

    //riderLogger.info(s"appInfoMap $appInfoMap")
    val admin = Await.result(userDal.findByFilter(_.roleType === "admin").map(_.head.id), minTimeOut)
    val streamMap = streamDal.updateStreamStatusByYarn(streams, appInfoMap, admin)
    jobDal.updateJobStatusByYarn(jobs, appInfoMap)
    flowDal.updateFlowStatusByYarn(streamMap)
  }

  def getYarnFromTime(streams: Seq[Stream], jobs: Seq[Job]): String = {
    val jobFromTime =
      if (jobs != null && jobs.nonEmpty && jobs.exists(_.startedTime.getOrElse("") != ""))
        jobs.filter(_.startedTime.getOrElse("") != "").map(_.startedTime).min.getOrElse("")
      else ""

    val streamFromTime =
      if (streams != null && streams.nonEmpty && streams.exists(_.startedTime.getOrElse("") != ""))
        streams.filter(_.startedTime.getOrElse("") != "").map(_.startedTime).min.getOrElse("")
      else ""

    if (jobFromTime == "") streamFromTime
    else if (streamFromTime == "") jobFromTime
    else if (streamFromTime > jobFromTime) jobFromTime
    else streamFromTime
  }

  def getAllYarnAppStatus(fromTime: String, appNames: Seq[String]): Map[String, AppResult] = {
    val fromTimeLong =
      if (fromTime == "") 0
      else if (fromTime.length > 19) dt2long(fromTime) / 1000
      else if (fromTime.length < 19) dt2long(fromTime)
    val rmUrl = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    //val queueName = RiderConfig.flink.yarnQueueName
    if (rmUrl != "") {
      val url = s"http://${rmUrl.stripPrefix("http://").stripSuffix("/")}/ws/v1/cluster/apps?states=accepted,running,killed,failed,finished&startedTimeBegin=$fromTimeLong&applicationTypes=spark,apache%20flink"
      //      riderLogger.info(s"Spark Application refresh yarn rest url: $url")
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
      //riderLogger.info(s"Spark Application refresh yarn rest api response resultMap: $resultMap")
      resultMap.toMap
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Spark Application refresh yarn rest api response failed", ex)
        resultMap.toMap
    }
  }

  def getAppStatusByRest(map: Map[String, AppResult], appId: String, appName: String, curStatus: String, startedTime: String, stoppedTime: String): AppInfo = {
    var result = AppResult(appId, appName, curStatus, "", startedTime, stoppedTime)
    if (map.contains(appName)) {
      val yarnApp = map(appName)

      if (result.startedTime == null || result.startedTime == "") {
        result = yarnApp
      } else {
        val resultStartTime = dt2date(result.startedTime)
        resultStartTime.setTime(resultStartTime.getTime - 60 * 1000)
        if (yyyyMMddHHmmss(yarnApp.startedTime) >= yyyyMMddHHmmss(resultStartTime))
          result = yarnApp
      }
      //riderLogger.info(s"getAppStatusByRest appName $appName, yarnApp ${yarnApp.startedTime}, result ${result.startedTime}, result sub ${resultStartTime}")
    } else {
      riderLogger.debug("refresh spark/yarn api response is null")
    }

    if (result.finalStatus != null && result.finalStatus == YarnAppStatus.SUCCEEDED.toString)
      AppInfo(result.appId, StreamStatus.DONE.toString, result.startedTime, result.finishedTime)
    else
      AppInfo(result.appId, result.appStatus, result.startedTime, result.finishedTime)
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

  private def getAmUrl(activeRm: String): String = {
    if (RiderConfig.spark.proxyPort == 0) {
      activeRm
    } else {
      activeRm.split(":")(0) + ":" + RiderConfig.spark.proxyPort
    }
  }

  def getFlinkJobStatusOnYarn(appIds: Seq[String]): Map[String, FlinkJobStatus] = {
    val activeRm = getActiveResourceManager(RiderConfig.spark.rm1Url, RiderConfig.spark.rm2Url)
    val amUrl = getAmUrl(activeRm)
    val flinkJobMap = HashMap.empty[String, FlinkJobStatus]
    appIds.foreach {
      appId =>
        val url = s"http://$amUrl/proxy/$appId/jobs/overview"
        var retryNum = 0
        var response: HttpResponse[String] = HttpResponse("", 200, null)
        while (retryNum < 3) {
          try {
            response = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
            //            riderLogger.info(s"Get Flink job status request url $url retry num $retryNum")
            retryNum = 3
          } catch {
            case ex: Exception =>
              retryNum = retryNum + 1
              riderLogger.error(s"Get Flink job status failed by request url $url retry num $retryNum: $ex")
              if (retryNum >= 3) throw ex
          }
        }

        try {
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
            riderLogger.error(s"Get Flink job status failed by request url $url, ${response.body}: $ex")
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
