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

import akka.http.javadsl.model.headers.HttpCredentials
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri, _}
import akka.util.ByteString
import edp.rider.RiderStarter.{materializer, system, _}
import edp.rider.common.{GrafanaConnectionInfo, RiderConfig, RiderLogger}
import edp.rider.rest.util.CommonUtils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Dashboard extends RiderLogger {

  def createDashboard(projectId: Long, projectName: String, esIndex: String) = {
    try {
      val uri = Uri.apply(s"${RiderConfig.grafana.url}/api/dashboards/db")
      val userData: ByteString = ByteString(getCreateDashboardJson(projectName, projectId, esIndex))
      riderLogger.info(s"Grafana create ${projectName}_Monitor dashboard")
      grafanaPostData(RiderConfig.grafana.adminToken, uri, userData)
    } catch {
      case e: Exception =>
        riderLogger.error(s"Grafana create dashboard data failed", e)
    }
  }

  private def grafanaPostData(token: String, uri: Uri, userData: ByteString) = {
    val authorization: Authorization = headers.Authorization(HttpCredentials.createOAuth2BearerToken(token))
//    val a = headers.`Content-Type`.apply(ContentTypes.`application/json`)
//    val b = headers.Accept.apply(MediaTypes.`application/json`)
    val httpRequest: HttpRequest = HttpRequest(
      HttpMethods.POST,
      uri,
      headers = List(authorization),
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, userData)
    )
    //    riderLogger.info(s"httpRequest ${httpRequest.toString}.")
    try {
      val response: HttpResponse = Await.result(Http().singleRequest(httpRequest), CommonUtils.minTimeOut)
      if (response._1.isSuccess()) {
        //        riderLogger.info(s"response success: ${response.entity.toString}.")
        riderLogger.info(s"Grafana post data response success.")
      }
      else {
        riderLogger.error(s"Grafana post data response failed value: ${response._1.value}.")
        riderLogger.error(s"Grafana post data response failed message: ${response._1.defaultMessage}.")
        riderLogger.error(s"Grafana post data response failed message: ${response._3.withContentType(ContentTypes.`application/json`).toString}.")
      }
    }
    catch {
      case e: Exception =>
        riderLogger.error(s"Grafana post data failed to get response", e)
    }
  }

  def getAdminDashboardInfo(project_id: Long): GrafanaConnectionInfo = {
    if (RiderConfig.grafana != null)
      GrafanaConnectionInfo(getDashboardURI(project_id))
    else
      GrafanaConnectionInfo("")
  }

  def getViewerDashboardInfo(project_id: Long): GrafanaConnectionInfo = {
    getAdminDashboardInfo(project_id)
  }

  def getDashboardURI(project_id: Long): String = {
    try {
      val dashboard = generateDashboardName(project_id)
      val url = s"${RiderConfig.grafana.domain}/dashboard/db/$dashboard"
      url
    } catch {
      case e: Exception =>
        riderLogger.error(s"Grafana failed to get project $project_id dashboard url", e)
        ""
    }
  }

  private def generateDashboardName(project_id: Long): String = {
    Await.result(modules.projectDal.findById(project_id), Duration.Inf) match {
      case Some(project) => project.name + "_Monitor"
      case None => ""
    }
  }

  def getCreateDashboardJson(projectName: String, projectId: Long, esIndex: String): String = {
    val msg: String = ReadJsonFile.getMessageFromJson(JsonFileType.GRAFANACREATE)
      .replace("#EDP_DASHBOARD#", projectName + "_Monitor")
      .replace("#PROJECT_ID#", projectId.toString)
      .replace("#GRAFANA_DATASOURCE#", esIndex)
    msg
  }

}
