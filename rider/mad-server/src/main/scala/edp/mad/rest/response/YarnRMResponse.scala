package edp.mad.rest.response


import akka.http.scaladsl.model.HttpMethods
import edp.mad.cache._
import edp.mad.module.ModuleObj
import edp.mad.util.HttpClient
import edp.wormhole.common.util.JsonUtils
import org.apache.log4j.Logger
import org.json4s.{DefaultFormats, Formats, JNothing, JValue}
/*
import java.util.{Calendar, Date, GregorianCalendar}


import edp.wormhole.common.util.DtFormat
import edp.wormhole.common.util.DateUtils._
import edp.mad.common._
import com.alibaba.fastjson.JSON


import spray.json.JsonParser
import edp.mad.persistence.entities._
import edp.mad.common.{CacheStreamInfo, _}

import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
*/
object YarnRMResponse{
  private val logger = Logger.getLogger(this.getClass)
  implicit val json4sFormats: Formats = DefaultFormats
  val modules = ModuleObj.getModule
  lazy val baseRMSite = getActiveResourceManager( modules.hadoopYarnRMSite1, modules.hadoopYarnRMSite2)
  lazy val baseUrl = s"http://${baseRMSite.stripPrefix ("http://").stripPrefix ("/")}/ws/v1/cluster"

  def getResourceManagerHaState(rm: String): String = {
    val url = s"http://${rm.stripPrefix("http://").stripPrefix("/")}/ws/v1/cluster/info"
    var haStateValue =""
    if (rm != null && rm != "") {
      val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, "", "", "")
      if (response._1 == true) {
        try {
          val jObj = JsonUtils.getJValue(response._2, "clusterInfo")
          if (jObj != null && jObj != JNothing) {
            haStateValue = JsonUtils.getString(jObj, "haState")
          }
        }catch {
          case ex: Exception =>
            logger.error(s"query yarn resourceManager $url haState failed", ex)
        }
      }
    }
    haStateValue
  }

  def getActiveResourceManager(rm1: String, rm2: String): String = {
    if (getResourceManagerHaState(rm1).toUpperCase == "ACTIVE")
      rm1
    else if (getResourceManagerHaState(rm2).toUpperCase == "ACTIVE")
      rm2
    else {
      logger.error(s"query yarn resourceManager haState failed caused by both resourceManager url is empty.")
      ""
    }
  }

   def getActiveAppsInfo() = {
   //val url = s"${baseUrl}/apps?states=accepted,running,killed,failed,finished&&startedTimeBegin=$startedTimeBegin&&applicationTypes=spark"
    val url = s"${baseUrl}/apps?states=accepted,running&&applicationTypes=spark"
    val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, "", "", "")
    if (response._1 == true) {
      try {
        logger.info(s" response body  ${response._2} \n")
        if( JsonUtils.getJValue(response._2, "apps") != null ) {
          val appObjs = JsonUtils.getJValue( JsonUtils.getJValue(response._2, "apps"),"app")
          if( appObjs != null && appObjs != JNothing) {
            appObjs.extract[Array[JValue]].foreach { appObj =>
              logger.debug(s" ===  ${appObj}\n")
              val appId = JsonUtils.getString(appObj, "id")
              val streamName = JsonUtils.getString(appObj, "name")
              val state = JsonUtils.getString(appObj, "state")
              val finalStatus = JsonUtils.getString(appObj, "finalStatus")
              val user = JsonUtils.getString(appObj, "user")
              val queue = JsonUtils.getString(appObj, "queue")
              val startedTime = JsonUtils.getLong(appObj, "startedTime")
              logger.debug(s" Application Map ${appId}   ${streamName} \n")
              modules.applicationMap.set(ApplicationMapKey(appId), ApplicationMapValue(streamName))
            }
          }else{
            logger.error(s" failed to get apps/app  \n")
          }
        }else{
            logger.error(s" failed to get apps \n")
        }
      }catch{
        case e:Exception =>
          logger.error(s"failed to parse response ${response._2} \n",e)
      }
    }else{
      logger.error(s"failed to get the response from yarn resource manager ${response}" )
    }
    logger.info(s"  ${modules.applicationMap.mapPrint}")
  }

}