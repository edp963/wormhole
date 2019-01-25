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


package edp.rider.monitor

import java.io.IOException
import java.text.SimpleDateFormat

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri, _}
import akka.http.scaladsl.unmarshalling._
import akka.util.ByteString
import edp.rider.RiderStarter._
import edp.rider.common.{RiderConfig, RiderEs, RiderLogger, RiderMonitor}
import edp.rider.rest.persistence.entities.{Interval, MonitorInfo, MonitorInfoES}
import edp.rider.rest.util.CommonUtils
import edp.wormhole.util.JsonUtils
import org.json4s.JsonAST.{JNothing, JNull}
import org.json4s.{DefaultFormats, Formats, JValue}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object ElasticSearch extends RiderLogger {

  def initial(es: RiderEs): Unit = {
    if (es != null)
      createEsIndex()
    else
      riderLogger.warn(s"application.conf didn't config elasticsearch, so won't initial elasticsearch index, store wormhole stream and flow feedback_stats data")
//    if (grafana != null)
//      GrafanaApi.createOrUpdateDataSource(RiderConfig.grafana.url,
//        RiderConfig.grafana.adminToken,
//        RiderConfig.grafana.esDataSourceName,
//        RiderConfig.es.url,
//        RiderConfig.es.wormholeIndex,
//        RiderConfig.es.user,
//        RiderConfig.es.pwd)
//    else
//      riderLogger.warn(s"application.conf didn't config grafana, so won't initial grafana datasource, wormhole project performance will display nothing")

  }

  private def getESUrl: String = {
    try {
      s"${RiderConfig.es.url}/${RiderConfig.es.wormholeIndex}/${RiderConfig.es.wormholeType}/"
    } catch {
      case e: Exception =>
        riderLogger.error(s"ES url config get from application.conf failed", e)
        ""
    }
  }

  private def getESIndexUrl: String = {
    try {
      s"${RiderConfig.es.url}/${RiderConfig.es.wormholeIndex}/"
    } catch {
      case e: Exception =>
        riderLogger.error(s"ES index config get failed ", e)
        ""
    }
  }


  def insertFlowStatToES(stats: MonitorInfo) = {
    val url = getESUrl
    //    riderLogger.info("es url: " + url)
    val postBody: String = JsonUtils.caseClass2json(stats)
    //    riderLogger.info("es insert: " + postBody)
    asyncToES(postBody, url, HttpMethods.POST)
    //    syncToES(postBody, url, HttpMethods.POST)

  }

  def queryESFlowMax(projectId: Long, streamId: Long, flowId: Long, columnName: String): (Boolean, String) = {
    if (RiderConfig.es != null) {
      var maxValue = ""
      val postBody = ReadJsonFile.getMessageFromJson(JsonFileType.ESMAXFLOW)
        .replace("#PROJECT_ID#", projectId.toString)
        .replace("#STREAM_ID#", streamId.toString)
        .replace("#FLOW_ID#", flowId.toString)
        .replace("#COLUMN_NAME#", columnName)
      val url = getESUrl + "_search"
      riderLogger.debug(s"queryESFlowMax url $url $postBody")
      val response = syncToES(postBody, url, HttpMethods.POST, CommonUtils.minTimeOut)
      //    riderLogger.info(s"queryESFlowMax $response")
      if (response._1 == true) {
        try {
          val maxColumn = JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(response._2, "hits"), "hits"), s"_source")
          //val maxColumn = JsonUtils.getJValue(JsonUtils.getJValue(response._2, "hits"), s"_source")
          implicit val json4sFormats: Formats = DefaultFormats
          maxValue = JsonUtils.getJValue(maxColumn, s"$columnName").extract[String]
        }
        catch {
          case e: Exception =>
            riderLogger.error(s"Failed to get max flow value from ES response", e)
        }
      }

      else {
        riderLogger.error(s"Failed to get max flow value from ES response")
      }
      (response._1, maxValue)
    } else (false, "")
  }


  def queryESStreamMax(projectId: Long, streamId: Long, columnName: String): (Boolean, String) = {
    if (RiderConfig.es != null) {
      var maxValue = ""
      val postBody = ReadJsonFile.getMessageFromJson(JsonFileType.ESMAXSTREAM)
        .replace("#PROJECT_ID#", projectId.toString)
        .replace("#STREAM_ID#", streamId.toString)
        .replace("#COLUMN_NAME#", columnName)
      val url = getESUrl + "_search"
      riderLogger.debug(s"queryESStreamMax url $url $postBody")
      val response = syncToES(postBody, url, HttpMethods.POST, CommonUtils.minTimeOut)
      //    riderLogger.info(s"queryESStreamMax $response")
      if (response._1) {
        try {
          val maxColumn = JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(response._2, "hits"), "hits"), s"_source")
          implicit val json4sFormats: Formats = DefaultFormats
          maxValue = JsonUtils.getJValue(maxColumn, s"$columnName").extract[String]
        } catch {
          case e: Exception =>
            riderLogger.error(s"Failed to get max stream value from ES response", e)
        }
      } else {
        riderLogger.error(s"Failed to get max stream value from ES response")
      }
      (response._1, maxValue)
    } else (false, "")
  }

  def compactPostBody(projectId: Long, modelType:Long,content:Long,startTime: String, endTime: String):String =
    modelType match {
      case 0 =>ReadJsonFile.getMessageFromJson(JsonFileType.ESFLOW)
        .replace("#PROJECT_ID#", projectId.toString)
        .replace("#FLOW_ID#", content.toString)
        .replace("#START_TIME#", startTime)
        .replace("#END_TIME#", endTime)
      case 1 =>ReadJsonFile.getMessageFromJson(JsonFileType.ESSTREAM)
        .replace("#PROJECT_ID#", projectId.toString)
        .replace("#STREAM_ID#", content.toString)
        .replace("#START_TIME#", startTime)
        .replace("#END_TIME#", endTime)
    }


  def queryESMonitor(postBody:String)={
    val list=ListBuffer[MonitorInfo]()
    if (RiderConfig.es != null) {

      val url = getESUrl + "_search"
      riderLogger.debug(s"queryESStreamMonitor url $url $postBody")
      val response = syncToES(postBody, url, HttpMethods.POST, CommonUtils.minTimeOut)
      if (response._1) {
        val tuple = JsonUtils.getJValue(JsonUtils.getJValue(response._2, "hits"), "hits").children
        implicit val json4sFormats: Formats = DefaultFormats
        tuple.distinct.foreach(jvalue=>{
          val value=JsonUtils.getJValue(jvalue,s"_source")
          val interval=JsonUtils.getJValue(value,s"interval")

          val result=if(interval!=JNothing)JsonUtils.json2caseClass[MonitorInfo](JsonUtils.jValue2json(value))
          else changeMonitorInfoEsToMonitorInfo(JsonUtils.json2caseClass[MonitorInfoES](JsonUtils.jValue2json(value)))

          list+=result
        })
      }else{
        riderLogger.error(s"Failed to get stream info from ES response")
      }
      (response._1,list)
    }else (false, list)
  }

  def changeMonitorInfoEsToMonitorInfo(monitor:MonitorInfoES)={
    MonitorInfo(0,monitor.statsId,monitor.umsTs,monitor.projectId,monitor.streamId,monitor.streamName,monitor.flowId,monitor.flowNamespace,
      monitor.rddCount,monitor.topics,monitor.throughput,monitor.dataGeneratedTs,monitor.rddTs,monitor.directiveTs,monitor.DataProcessTs,monitor.swiftsTs,monitor.sinkTs,monitor.doneTs,
      Interval(monitor.intervalDataProcessToDataums,monitor.intervalDataProcessToRdd,monitor.intervalDataProcessToSwifts,monitor.intervalDataProcessToSink,monitor.intervalDataProcessToDone,monitor.intervalDataumsToDone,monitor.intervalRddToDone,monitor.intervalSwiftsToSink,monitor.intervalSinkToDone))
  }

  def deleteEsHistory(fromDate: String, endDate: String): Int = {
    var deleted = 0
    val postBody = ReadJsonFile.getMessageFromJson(JsonFileType.ESDELETED)
      .replace("#FROMDATE#", s""""$fromDate"""")
      .replace("#TODATE#", s""""$endDate"""")
    val url = getESUrl + "_delete_by_query"
    riderLogger.info(s"deleteEsHistory url $url $postBody")
    val response = asyncToES(postBody, url, HttpMethods.POST)
    riderLogger.info(s"deleteEsHistory response $response")
    //    if (response._1) {
    //      try {
    //        deleted = JsonUtils.jValue2json(JsonUtils.getJValue(response._2, "deleted")).toInt
    //      } catch {
    //        case e: Exception =>
    //          riderLogger.error(s"Failed to parse the response from ES when delete history data", e)
    //      }
    //    } else {
    //      riderLogger.error(s"Failed to delete history data from ES")
    //    }
    deleted
  }

  def createEsIndex() = {
    val body = ReadJsonFile.getMessageFromJson(JsonFileType.ESCREATEINDEX).replace("#ESINDEX#", s"${RiderConfig.es.wormholeIndex}")
    val url = getESIndexUrl
    val existsResponse = syncToES("", url, HttpMethods.GET)
    //    riderLogger.info(s" query index exists response $existsResponse")
    if (existsResponse._1) {
      riderLogger.info(s"ES index $url already exists")
    } else {
      riderLogger.info(s"createEsIndex url $url $body")
      asyncToES(body, url, HttpMethods.PUT)
    }
  }

  private def asyncToES(postBody: String, url: String, method: HttpMethod): Boolean = {
    var tc = false
    val uri = Uri.apply(url)
    val httpRequest: HttpRequest = HttpRequest(
      method,
      uri,
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(postBody))
    ).addCredentials(BasicHttpCredentials(RiderConfig.es.user, RiderConfig.es.pwd))
    riderLogger.debug(s"httpRequest ${
      httpRequest.toString
    }.")
    try {
      val response: Future[HttpResponse] = Http().singleRequest(httpRequest)
      //      riderLogger.info(s"es response: code: ${response.value.get.get.status}, ${response.value.get.get.entity}")
      tc = true

    } catch {
      case ste: java.net.SocketTimeoutException =>
        riderLogger.error(s"asyncToES ${
          httpRequest.toString
        } SocketTimeoutException", ste)
      case ioe: java.io.IOException =>
        riderLogger.error(s"asyncToES ${
          httpRequest.toString
        } IOException", ioe)
      case ex: Throwable =>
        riderLogger.error(s"asyncToES ${
          httpRequest.toString
        } failed", ex)
    }
    tc
  }

  private def syncToES(postBody: String, url: String, method: HttpMethod, timeOut: Duration = 10.seconds): (Boolean, JValue) = {
    var tc = false
    var responseJson: JValue = JNull
    val uri = Uri.apply(url)
    // val a = headers.`Content-Type`.apply(ContentTypes.`application/json`)
    val b = headers.Accept.apply(MediaTypes.`application/json`)
    val httpRequest: HttpRequest = HttpRequest(
      method,
      uri,
      headers = List(b),
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(postBody))
    ).addCredentials(BasicHttpCredentials(RiderConfig.es.user, RiderConfig.es.pwd))
    //    riderLogger.info(s"httpRequest ${
    //      httpRequest.toString
    //    }.")
    try {
      val response = Await.result(Http().singleRequest(httpRequest), timeOut)
      response.status match {
        case StatusCodes.OK if (response.entity.contentType == ContentTypes.`application/json`) =>
          riderLogger.debug(s"response.entity ${
            response.entity.toString
          }.")
          Await.result(
            Unmarshal(response.entity).to[String].map {
              jsonString =>
                //                riderLogger.info(s"== jsonString ${
                //                  jsonString
                //                }.")
                tc = true
                responseJson = JsonUtils.json2jValue(jsonString)
            }, Duration.Inf)
        case StatusCodes.BadRequest => {
          riderLogger.error(s"syncToES failed caused by incorrect latitude and longitude format")
        }
        case _ => Unmarshal(response.entity).to[String].flatMap {
          entity =>
            val error = s"Google GeoCoding request failed with status code ${
              response.status
            } and entity $entity"
            Future.failed(new IOException(error))
        }
      }
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to get the response from ES when syncToES", e)
    }
    //    riderLogger.info(s"====> syncToES return  $tc  $responseJson.")
    (tc, responseJson)
  }
}
