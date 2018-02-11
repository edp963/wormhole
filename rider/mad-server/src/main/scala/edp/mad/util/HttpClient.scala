package edp.mad.util

import java.io.IOException

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpRequest, Uri, _}
import akka.http.scaladsl.unmarshalling._
import akka.util.ByteString
import edp.mad.MadStarter._
import edp.mad.module.ModuleObj
import edp.wormhole.common.util.JsonUtils
import org.apache.log4j.Logger
import org.json4s.JValue
import org.json4s.JsonAST.JNull

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.concurrent.{Await, Future}

object HttpClient {

  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule
  implicit val system = modules.system

  def asyncClient(postBody: String, url: String, method: HttpMethod, userName: String, passwd: String, token: String): Boolean = {
    var tc = false
    val uri = Uri.apply(url)

    var httpRequest: HttpRequest =  HttpRequest(
      method,
      uri,
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(postBody))
    )

    if(token != "" && token != null)  {
      httpRequest = httpRequest.addCredentials(OAuth2BearerToken(token))
    }else if(userName != "" && userName != null && passwd != "" && passwd != null) {
      httpRequest = httpRequest.addCredentials(BasicHttpCredentials(userName, passwd))
    }

    logger.debug(s"httpRequest \n ${httpRequest.toString}. \n")

    try {
      val response: Future[HttpResponse] = Http().singleRequest(httpRequest)
      tc = true
    } catch {
      case urle: IllegalUriException =>
        logger.error(s"asyncToES \n ${httpRequest.toString} \n IllegalUriException", urle)
      case ex: Throwable =>
        logger.error(s"asyncToES \n ${httpRequest.toString} \n failed", ex)
    }
    tc
  }

  def syncClientGetJValue(postBody: String, url: String, method: HttpMethod, userName: String, passwd: String, token: String): (Boolean, JValue) = {
    var tc = false
    var responseJson: JValue = JNull
    val uri = Uri.apply(url)

    val b = headers.Accept.apply(MediaTypes.`application/json`)
    var httpRequest: HttpRequest = HttpRequest(
      method,
      uri,
      headers = List(b),
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(postBody))
    )

    if(token != "" && token != null)  {
      httpRequest = httpRequest.addCredentials(OAuth2BearerToken(token))
    }else if(userName != "" && userName != null && passwd != "" && passwd != null) {
      httpRequest = httpRequest.addCredentials(BasicHttpCredentials(userName, passwd))
    }

    logger.info(s"httpRequest \n ${httpRequest.toString}.\n")

    try {
      val response = Await.result(Http().singleRequest(httpRequest), FiniteDuration(180, SECONDS))
      logger.info(s" responseStatus: ${url}  ${response.status} \n")
      response.status match {
        case StatusCodes.OK if (response.entity.contentType == ContentTypes.`application/json`) =>
          logger.debug(s"response.entity \n ${response.entity.toString}.\n")
          Await.result(
            Unmarshal(response.entity).to[String].map {
              jsonString =>
                // logger.info(s"== jsonString \n ${jsonString}.\n ")
                tc = true
                responseJson = JsonUtils.json2jValue(jsonString)
            }, Duration.Inf)
        case StatusCodes.Created  =>
          tc = true
        case StatusCodes.BadRequest => {
          Unmarshal(response.entity).to[String].flatMap {
            entity =>
              val error = s"Google GeoCoding request failed with status code \n ${response.status} \n  and entity $entity"
              logger.error(s"syncToES failed caused by ${error}" )
              Future.failed(new IOException(error))
          }
          logger.error(s"syncToES failed caused by incorrect latitude and longitude format ${response}")
        }
        case _ => Unmarshal(response.entity).to[String].flatMap {
          entity =>
            val error = s"Google GeoCoding request failed with status code \n ${response.status} \n  and entity $entity"
            Future.failed(new IOException(error))
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get the response from ES when syncToES", e)
    }
    // logger.info(s"====> syncToES return  $tc  $responseJson.")
    (tc, responseJson)
  }


  def syncClient(postBody: String, url: String, method: HttpMethod, userName: String, passwd: String, token: String): (Boolean, String) = {
    var tc = false
    var responseJson: String = null
    val uri = Uri.apply(url)

    val b = headers.Accept.apply(MediaTypes.`application/json`)
    var httpRequest: HttpRequest = HttpRequest(
      method,
      uri,
      headers = List(b),
      protocol = HttpProtocols.`HTTP/1.1`,
      entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(postBody))
    )

    if(token != "" && token != null)  {
      httpRequest = httpRequest.addCredentials(OAuth2BearerToken(token))
    }else if(userName != "" && userName != null && passwd != "" && passwd != null) {
      httpRequest = httpRequest.addCredentials(BasicHttpCredentials(userName, passwd))
    }

    logger.info(s"httpRequest \n ${httpRequest.toString}.\n")

    try {
      val response = Await.result(Http().singleRequest(httpRequest), Duration.Inf)
      logger.info(s"responseStatus  ${url}  ${response.status}.\n")
      response.status match {
        case StatusCodes.OK if (response.entity.contentType == ContentTypes.`application/json`) =>
          logger.debug(s"response.entity \n ${response.entity.toString}.\n")
          Await.result(
            Unmarshal(response.entity).to[String].map {
              jsonString =>
                //logger.info(s"== jsonString \n ${jsonString}.\n ")
                tc = true
                responseJson = jsonString
            }, Duration.Inf)
        case StatusCodes.Created  =>
            tc = true
        case StatusCodes.BadRequest => {
          logger.error(s"syncToES failed caused by incorrect latitude and longitude format")
        }
        case _ => Unmarshal(response.entity).to[String].flatMap {
          entity =>
            val error = s"Google GeoCoding request failed with status code \n ${response.status} \n  and entity $entity"
            Future.failed(new IOException(error))
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get the response from ES when syncToES", e)
    }
    //logger.info(s"====> syncToES return  $tc  $responseJson.")
    (tc, responseJson)
  }
}