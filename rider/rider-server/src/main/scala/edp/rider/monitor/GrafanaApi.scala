package edp.rider.monitor

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, _}
import akka.util.ByteString
import edp.rider.RiderStarter._
import edp.rider.common.RiderLogger
import edp.rider.rest.util.CommonUtils.minTimeOut
import edp.wormhole.util.JsonUtils._

import scala.concurrent.duration._
import scala.concurrent.Await

case class DataSourceJsonData(esVersion: Int,
                              timeField: String,
                              timeInterval: String)

case class GrafanaDataSource(id: Option[Long] = None,
                             name: String,
                             `type`: String,
                             url: String,
                             access: String,
                             database: String,
                             basicAuth: Boolean,
                             basicAuthUser: Option[String] = None,
                             basicAuthPassword: Option[String] = None,
                             jsonData: Option[DataSourceJsonData])

object GrafanaApi extends RiderLogger {

  private def getDataSource(grafanaUrl: String, token: String, name: String): Option[GrafanaDataSource] = {
    try {
      val request = HttpRequest(uri = s"${grafanaUrl.stripSuffix("/")}/api/datasources/name/$name",
        headers = List(headers.Accept.apply(MediaTypes.`application/json`)))
        .addCredentials(OAuth2BearerToken(token))

//      riderLogger.info("grafana search: " + request.toString)

      val response = Await.result(Http().singleRequest(request), 10.seconds)

//      riderLogger.info("grafana search: " + response.toString)

      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
            riderLogger.info(s"search grafana wormhole es datasource $name success")
            body =>
              val data = json2jValue(body.utf8String)
              if (containsName(data, "message")) None
              else Some(GrafanaDataSource(Some(getLong(data, "id")),
                getString(data, "name"),
                getString(data, "type"),
                getString(data, "url"),
                getString(data, "access"),
                getString(data, "database"),
                getBoolean(data, "basicAuth"),
                if (containsName(data, "basicAuthUser")) Some(getString(data, "basicAuthUser")) else None,
                if (containsName(data, "basicAuthPassword")) Some(getString(data, "basicAuthPassword")) else None,
                None))
          }, minTimeOut)
        case resp@HttpResponse(code, _, _, _) =>
          riderLogger.error(s"parse search grafana wormhole es datasource $name response failed, ${code.reason}.")
          None
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"search grafana wormhole es datasource $name failed", ex)
        None
    }
  }

  private def createDataSource(grafanaUrl: String, token: String, name: String, esUrl: String, esIndex: String, user: String, pwd: String): Boolean = {
    try {
      val jsonData = Some(DataSourceJsonData(5, "rddTs", ""))
      val postData =
        if (user == "" || user == null)
          GrafanaDataSource(None, name, "elasticsearch", esUrl.stripSuffix("/"), "proxy", esIndex, false, jsonData = jsonData)
        else
          GrafanaDataSource(None, name, "elasticsearch", esUrl.stripSuffix("/"), "proxy", esIndex, false, Some(user), Some(pwd), jsonData)
      val response = Await.result(Http().singleRequest(
        HttpRequest(uri = s"${grafanaUrl.stripSuffix("/")}/api/datasources",
          method = HttpMethods.POST,
          headers = List(headers.Accept.apply(MediaTypes.`application/json`)),
          entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(caseClass2json[GrafanaDataSource](postData)))
        ).addCredentials(OAuth2BearerToken(token))), 10.seconds)
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
            riderLogger.info(s"create grafana wormhole es datasource $name success")
            body =>
              val data = json2jValue(body.utf8String)
              if (containsName(data, "message")) false
              else true
          }, minTimeOut)
        case resp@HttpResponse(code, _, _, _) =>
          riderLogger.error(s"parse create grafana wormhole es datasource $name response failed, ${code.reason}.")
          false
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"create grafana wormhole es datasource $name failed", ex)
        false
    }
  }

  private def deleteDataSource(grafanaUrl: String, token: String, id: Long) = {
    try {
      val response = Await.result(Http().singleRequest(
        HttpRequest(uri = s"${grafanaUrl.stripSuffix("/")}/api/datasources/$id",
          method = HttpMethods.DELETE,
          headers = List(headers.Accept.apply(MediaTypes.`application/json`))
        ).addCredentials(OAuth2BearerToken(token))), minTimeOut)
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
            riderLogger.info(s"delete grafana wormhole es datasource $id success")
            body =>
              val data = json2jValue(body.utf8String)
              if (containsName(data, "message")) true
              else false
          }, 10.seconds)
        case resp@HttpResponse(code, _, _, _) =>
          riderLogger.error(s"parse delete grafana wormhole es datasource $id response failed, ${code.reason}.")
          false
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete gafana wormhole es datasource $id failed", ex)
        false
    }
  }

  def createOrUpdateDataSource(grafanaUrl: String, token: String, name: String, esUrl: String, esIndex: String, user: String, pwd: String) = {
    getDataSource(grafanaUrl, token, name) match {
      case Some(esDataSource) =>
        if (esDataSource.url != esUrl || esDataSource.`type` != "elasticsearch" ||
          esDataSource.basicAuthUser.getOrElse("") != user || esDataSource.basicAuthPassword.getOrElse("") != pwd || esDataSource.database != esIndex) {
          riderLogger.info(s"grafana datasource $name already exists, but config is not latest, wormhole will delete and recreate it")
          if (deleteDataSource(grafanaUrl, token, esDataSource.id.get)) {
            createDataSource(grafanaUrl, token, name, esUrl, esIndex, user, pwd)
          }
        } else riderLogger.info(s"grafana datasource $name already exists")
      case None =>
        createDataSource(grafanaUrl, token, name, esUrl, esIndex, user, pwd)
    }
  }
}
