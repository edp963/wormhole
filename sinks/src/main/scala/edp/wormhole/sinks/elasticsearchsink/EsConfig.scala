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

import java.util.Date

import edp.wormhole.util.JsonUtils._
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import edp.wormhole.util.DateUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger
import org.json4s.JValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaj.http.Http

import scala.collection.JavaConversions._


case class EsConfig(`mutation_type`: Option[String] = None, _id: Option[String] = None, index_extend_config: Option[String] = None, header_config: Option[Map[String, String]] = None) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val `_id.get` = if (_id.nonEmpty) _id.get.split(",") else Array.empty[String]
}

object EsTools {
  private lazy val logger = Logger.getLogger(this.getClass)
  //  def getEsId(tuple: Seq[String], sinkSpecificConfig: EsConfig, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): String = {
  //    val _ids = ListBuffer.empty[String]
  //    if (sinkSpecificConfig.`_id.get`.nonEmpty ) {
  //      sinkSpecificConfig.`_id.get`.foreach(keyname => {
  //        val (index, _, _) = schemaMap(keyname)
  //        _ids += tuple(index)
  //      })
  //      _ids.mkString("_")
  //    } else UUID.randomUUID().toString
  //  }

  def getFullIndexNameByExtentConfig(indexName: String, indexname_extend_config: String): String = {
    val extendContent: String = DateUtils.getDateFormat(indexname_extend_config).format(new Date())
    indexName + extendContent
  }

  def doHttp(url: String, username: Option[String], passwd: Option[String], requestContent: String, requestTimeOut: Int, sinkSpecificConfig: EsConfig): String = {
    Http(url).timeout(1000, requestTimeOut * 1000)
    val header_config = sinkSpecificConfig.header_config
    if(header_config.nonEmpty && header_config.get.nonEmpty) {
      //val connConfigGet = JSON.parseObject(header_config.get)
      val connConfigGet = header_config.get
      val headers = connConfigGet.keys.map(key => {
        (key, connConfigGet.getOrElse(key, ""))
      }).toSeq
      if (username.nonEmpty && username.get.nonEmpty && passwd.nonEmpty && passwd.get.nonEmpty) {
        Http(url).headers(headers).timeout(1000, requestTimeOut * 1000).auth(username.get, passwd.get).postData(requestContent).asString.body
      } else {
        Http(url).headers(headers).timeout(1000, requestTimeOut * 1000).postData(requestContent).asString.body
      }
    } else {
      if (username.nonEmpty && username.get.nonEmpty && passwd.nonEmpty && passwd.get.nonEmpty) {
        Http(url).timeout(1000, requestTimeOut * 1000).auth(username.get, passwd.get).postData(requestContent).asString.body
      } else {
        Http(url).timeout(1000, requestTimeOut * 1000).postData(requestContent).asString.body
      }
    }
  }

  def checkResponseSuccess(responseJson: JValue): Boolean = {
    val result = (!containsName(responseJson, "error") || !getBoolean(responseJson, "error")) && (!containsName(responseJson, "errors") || !getBoolean(responseJson, "errors"))
    if (!result) logger.error("batch operation has error:" + responseJson)
    result
  }

  def getAvailableConnection(cc: ConnectionConfig, sinkSpecificConfig: EsConfig): ConnectionConfig = {
    def randomUrl(urlArray: Array[String]): String = {
      val index = Random.nextInt(urlArray.length)
      urlArray(index)
    }

    var urlArray = cc.connectionUrl.split(",")
    val length = urlArray.length
    var i = 0
    var availableUrl = ""
    val requestTimeOut = getRequestTimeout(cc)
    while (i < length) {
      val url = randomUrl(urlArray)
      try {
        EsTools.doHttp(url, cc.username, cc.password, "", requestTimeOut, sinkSpecificConfig)
        availableUrl = url
        i = length
      } catch {
        case e: Throwable =>
          i += 1
          urlArray = urlArray.filter(_ != url)
          logger.error("getAvailableConnection:" + url, e)
      }
    }
    ConnectionConfig(availableUrl, cc.username, cc.password, cc.parameters)
  }

  def queryVersionByEsid(esids: Seq[String],
                         namespace: UmsNamespace,
                         connectionConfig: ConnectionConfig,
                         sinkIndex: String,
                         sinkSpecificConfig: EsConfig): (Boolean, mutable.HashMap[String, Long]) = {
    var queryResult = true
    val esid2VersionMap = mutable.HashMap.empty[String, Long]
    val requestContent = """{"docs":[{"_id":"""" + esids.mkString("\",\"_source\":\"" + UmsSysField.ID.toString + "\"},{\"_id\":\"") + "\",\"_source\":\"" + UmsSysField.ID.toString + "\"}]}"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl + sinkIndex + "/" + namespace.table + "/_mget"
    else connectionConfig.connectionUrl + "/" + sinkIndex + "/" + namespace.table + "/_mget"

    logger.info("query url: " + url)
    val requestTimeOut = getRequestTimeout(connectionConfig)

    val responseContent = EsTools.doHttp(url, connectionConfig.username, connectionConfig.password, requestContent, requestTimeOut, sinkSpecificConfig)
    val responseJson: JValue = json2jValue(responseContent)
    if (!EsTools.checkResponseSuccess(responseJson)) {
      logger.error("queryVersionByEsid error :" + responseContent)
      queryResult = false
    } else {
      val docsJson = getList(responseJson, "docs")
      for (doc <- docsJson) {
        val id = getString(doc, "_id")
        if (containsName(doc, "found")) {
          if (getBoolean(doc, "found")) {
            val source = getJValue(doc, "_source")
            esid2VersionMap(id) = getLong(source, UmsSysField.ID.toString)
          } else esid2VersionMap(id) = -1
        } else {
          esid2VersionMap(id) = -1
          logger.error("response doc:" + doc)
        }
      }
    }
    (queryResult, esid2VersionMap)
  }

  def write2Es(list: ListBuffer[String], connectionConfig: ConnectionConfig, namespace: UmsNamespace, sinkIndex: String, sinkSpecificConfig: EsConfig): Boolean = {
    val requestContent = list.mkString("\n") + "\n"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + sinkIndex + "/" + namespace.table + "/_bulk"
    else connectionConfig.connectionUrl.trim + "/" + sinkIndex + "/" + namespace.table + "/_bulk"
    logger.info("doBatch url:" + url)
    val requestTimeOut = getRequestTimeout(connectionConfig)
    val responseContent = EsTools.doHttp(url, connectionConfig.username, connectionConfig.password, requestContent, requestTimeOut, sinkSpecificConfig)
    val responseJson: JValue = json2jValue(responseContent)
    EsTools.checkResponseSuccess(responseJson)
  }

  def getRequestTimeout(conConfig: ConnectionConfig): Int = {
    var requestTimeOut = 5
    if (conConfig.parameters.nonEmpty && conConfig.parameters != "") {
      val paras = conConfig.parameters.get
      paras.foreach(kv => if (kv.key.trim == "requestTimeOut") requestTimeOut = kv.value.toInt)
    }
    requestTimeOut
  }
}
