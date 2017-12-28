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


package edp.wormhole.sinks.elasticsearchsink

import java.util.UUID

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import org.json4s.JValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaj.http.Http

case class EsConfig(`mutation_type`: Option[String],_id:Option[String]) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
}

object EsTools extends EdpLogging{
  def getEsId(tuple:Seq[String],sinkSpecificConfig: EsConfig,schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): String ={
    val _ids = ListBuffer.empty[String]
    if (sinkSpecificConfig._id.nonEmpty&&sinkSpecificConfig._id.get.nonEmpty){
      sinkSpecificConfig._id.get.toLowerCase.split(",").foreach(keyname => {
        val (index, _, _) = schemaMap(keyname)
        _ids += tuple(index)
      })
      _ids.mkString("_")
    } else UUID.randomUUID().toString
  }

  def doHttp(url: String, username: Option[String], passwd: Option[String], requestContent: String): String = {
    if (username.nonEmpty && username.get.nonEmpty && passwd.nonEmpty && passwd.get.nonEmpty) {
      Http(url).auth(username.get, passwd.get).postData(requestContent).asString.body
    } else {
      Http(url).postData(requestContent).asString.body
    }
  }

  def checkResponseSuccess(responseJson: JValue): Boolean = {
    val result = (!containsName(responseJson, "error") || !getBoolean(responseJson, "error")) && (!containsName(responseJson, "errors") || !getBoolean(responseJson, "errors"))
    if (!result) logError("batch operation has error:" + responseJson)
    result
  }

  def getAvailableConnection(cc: ConnectionConfig): ConnectionConfig = {
    def randomUrl(urlArray: Array[String]): String = {
      val index = Random.nextInt(urlArray.length)
      urlArray(index)
    }

    var urlArray = cc.connectionUrl.split(",")
    val length = urlArray.length
    var i = 0
    var availableUrl = ""
    while (i < length) {
      val url = randomUrl(urlArray)
      try {
        EsTools.doHttp(url, cc.username, cc.password, "")
        availableUrl = url
        i = length
      } catch {
        case e: Throwable =>
          i += 1
          urlArray = urlArray.filter(_ != url)
          logError("getAvailableConnection:" + url, e)
      }
    }
    ConnectionConfig(availableUrl, cc.username, cc.password, cc.parameters)
  }

  def queryVersionByEsid(esids: Seq[String],
                                 sinkNamespace: String,
                                 connectionConfig: ConnectionConfig): (Boolean, mutable.HashMap[String, Long]) = {
    val namespace = UmsNamespace(sinkNamespace)
    var queryResult = true
    val esid2VersionMap = mutable.HashMap.empty[String, Long]
    val requestContent = """{"docs":[{"_id":"""" + esids.mkString("\",\"_source\":\"" + UmsSysField.ID.toString + "\"},{\"_id\":\"") + "\",\"_source\":\"" + UmsSysField.ID.toString + "\"}]}"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl + namespace.database + "/" + namespace.table + "/_mget"
    else connectionConfig.connectionUrl + "/" + namespace.database + "/" + namespace.table + "/_mget"
    val responseContent = EsTools.doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    if (!EsTools.checkResponseSuccess(responseJson)) {
      logError("queryVersionByEsid error :" + responseContent)
      queryResult = false
    } else {
      val docsJson = getList(responseJson, "docs")
      for (doc <- docsJson) {
        val id = getString(doc, "_id")
        if (getBoolean(doc, "found")) {
          val source = getJValue(doc, "_source")
          esid2VersionMap(id) = getLong(source, UmsSysField.ID.toString)
        } else esid2VersionMap(id) = -1
      }
    }
    (queryResult, esid2VersionMap)
  }

  def write2Es(list: ListBuffer[String], connectionConfig: ConnectionConfig, sinkNamespace: String): Boolean = {
    val namespace = UmsNamespace(sinkNamespace)
    val requestContent = list.mkString("\n") + "\n"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
    else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
    logInfo("doBatch url:" + url)
    val responseContent = EsTools.doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    EsTools.checkResponseSuccess(responseJson)
  }
}
