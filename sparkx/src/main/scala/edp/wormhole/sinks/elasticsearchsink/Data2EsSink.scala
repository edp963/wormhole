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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.WormholeDefault._
import edp.wormhole.common.util.CommonUtils
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsActiveType, UmsFieldType, UmsNamespace, UmsOpType, UmsSysField}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.common.util.JsonUtils._
import org.json4s.JValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaj.http._

class Data2EsSink extends SinkProcessor with EdpLogging {
  val optNameUpdate = "update"
  val optNameInsert = "create"

  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logInfo("process KafkaLog2ESSnapshot")
    //  val dt1: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    val sinkSpecificConfig: EsConfig = json2caseClass[EsConfig](sinkProcessConfig.specialConfig.get)
    val dataList = ListBuffer.empty[(String, Long, String)]
    val idList = ListBuffer.empty[String]
    for (row <- tupleList) {
      val data = convertJson(row, schemaMap, sinkProcessConfig,sinkSpecificConfig)
      dataList += data
      idList += data._1
    }
    if (!doSinkProcess(sinkProcessConfig, sinkNamespace, idList, dataList, connectionConfig,sinkSpecificConfig)) {
      throw new Exception("has error row to insert or update")
    }
    // val dt2: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    //  println("elastic duration:   " + dt2 + " - "+ dt1 +" = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " seconds."))

  }

  private def convertJson(row: Seq[String],
                          schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                          sinkConfig: SinkProcessConfig,
                          sinkSpecificConfig: EsConfig): (String, Long, String) = {
    var umsid = -1l
    val json = new JSONObject
    for ((name, (index, fieldType, _)) <- schemaMap) {
      val field = row(index)
      fieldType match {
        case UmsFieldType.STRING =>
          if (name == UmsSysField.OP.toString) {
            UmsOpType.umsOpType(field) match {
              case UmsOpType.UPDATE => json.put(UmsSysField.ACTIVE.toString, UmsActiveType.ACTIVE)
              case UmsOpType.INSERT => json.put(UmsSysField.ACTIVE.toString, UmsActiveType.ACTIVE)
              case UmsOpType.DELETE => json.put(UmsSysField.ACTIVE.toString, UmsActiveType.INACTIVE)
            }
          } else json.put(name, field)
        case UmsFieldType.INT => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toInt)
        case UmsFieldType.BINARY => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, CommonUtils.base64byte2s(field.trim.getBytes()))
        case UmsFieldType.LONG =>
          if (name == UmsSysField.ID.toString) umsid = field.trim.toLong
          if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toLong)
        case UmsFieldType.FLOAT => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toFloat)
        case UmsFieldType.DOUBLE => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toDouble)
        case UmsFieldType.DECIMAL => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toDouble)
        case UmsFieldType.BOOLEAN => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim.toBoolean)
        case UmsFieldType.DATE => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim)
        case UmsFieldType.DATETIME => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim)
        case _ => if (isNull(field) || field.isEmpty) json.put(name, null) else json.put(name, field.trim)
      }
    }
    if (sinkSpecificConfig.`es.mutation_type.get` == SourceMutationType.I_U_D.toString) {
      val primaryKeys = sinkConfig.tableKeyList
      val _ids = new Array[String](primaryKeys.length)
      for (i <- primaryKeys.indices) {
        val (index, _, _) = schemaMap(primaryKeys(i))
        _ids(i) = row(index)
      }
      (_ids.mkString("_"), umsid, json.toJSONString)
    }else{
      (UUID.randomUUID().toString, umsid, json.toJSONString)
    }
  }

  private def getAvailableConnection(cc: ConnectionConfig): ConnectionConfig = {
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
        doHttp(url,cc.username,cc.password,"")
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

  private def doSinkProcess(sinkConfig: SinkProcessConfig,
                            sinkNamespace: String,
                            idList: mutable.ListBuffer[String],
                            dataList: ListBuffer[(String, Long, String)],
                            connectionConfig: ConnectionConfig,
                            sinkSpecificConfig: EsConfig): Boolean = {
    val cc = getAvailableConnection(connectionConfig)
    logInfo("random url:" + cc.connectionUrl)
    if (cc.connectionUrl.isEmpty) new Exception(connectionConfig.connectionUrl + " are all not available")

    if (sinkSpecificConfig.`es.mutation_type.get` == SourceMutationType.I_U_D.toString) {
      val (result, esid2UmsidInEsMap) = queryVersionByEsid(idList, sinkConfig, sinkNamespace, cc)
      if (!result) false
      else {
        val insertId2JsonMap = mutable.HashMap.empty[String, String]
        val updateId2JsonMap = mutable.HashMap.empty[String, String]
        dataList.foreach { case (id, umsid, json) =>
          val umsidInEs = esid2UmsidInEsMap(id)
          if (umsidInEs == -1) insertId2JsonMap(id) = json
          else if (umsidInEs < umsid) updateId2JsonMap(id) = json
        }
        val insertFlag = doBatchInsert(insertId2JsonMap, sinkConfig, sinkNamespace, cc)
        val updateFlag = doBatchUpdate(updateId2JsonMap, sinkConfig, sinkNamespace, cc)
        insertFlag | updateFlag
      }
    } else {
      val insertId2JsonMap = mutable.HashMap.empty[String, String]
      dataList.foreach { case (id, _, json) =>
        insertId2JsonMap(id) = json
      }
      val insertFlag = doBatchInsert(insertId2JsonMap, sinkConfig, sinkNamespace, cc)
      insertFlag
    }
  }

  private def doBatchInsert(insertId2JsonMap: mutable.HashMap[String, String],
                            sinkConfig: SinkProcessConfig, sinkNamespace: String,
                            connectionConfig: ConnectionConfig): Boolean = {
    val namespace = UmsNamespace(sinkNamespace)
    if (insertId2JsonMap.nonEmpty) {
      val insertList = ListBuffer.empty[String]
      insertId2JsonMap.foreach(item => {
        insertList += s"""{ "$optNameInsert" : {"_id" : "${item._1}" }}"""
        insertList += item._2
      })
      val requestContent = insertList.mkString("\n") + "\n"
      val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
      else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
      logInfo("doBatch url:" + url)
      val responseContent = doHttp(url,connectionConfig.username,connectionConfig.password,requestContent)
      val responseJson: JValue = json2jValue(responseContent)
      checkResponseSuccess(responseJson)
    } else true
  }

  private def doHttp(url:String,username: Option[String],passwd:Option[String],requestContent:String):String={
    if(username.nonEmpty&&username.get.nonEmpty&&passwd.nonEmpty&&passwd.get.nonEmpty){
      Http(url).auth(username.get,passwd.get).postData(requestContent).asString.body
    }else{
      Http(url).postData(requestContent).asString.body
    }
  }

  private def doBatchUpdate(updateId2JsonMap: mutable.HashMap[String, String],
                            sinkConfig: SinkProcessConfig, sinkNamespace: String,
                            connectionConfig: ConnectionConfig): Boolean = {
    val namespace = UmsNamespace(sinkNamespace)
    if (updateId2JsonMap.nonEmpty) {
      val updateList = ListBuffer.empty[String]
      updateId2JsonMap.foreach(item => {
        updateList += s"""{ "$optNameUpdate" : {"_id" : "${item._1}" }}"""
        updateList += "{\"doc\":" + item._2 + "}"
      })
      val requestContent = updateList.mkString("\n") + "\n"
      val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
      else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
      logInfo("doBatch url:" + url)
      val responseContent = doHttp(url,connectionConfig.username,connectionConfig.password,requestContent)
      val responseJson: JValue = json2jValue(responseContent)
      checkResponseSuccess(responseJson)
    } else true
  }

  private def checkResponseSuccess(responseJson: JValue): Boolean = {
    val result = (!containsName(responseJson, "error") || !getBoolean(responseJson, "error")) && (!containsName(responseJson, "errors") || !getBoolean(responseJson, "errors"))
    if (!result) logError("batch operation has error:" + responseJson)
    result
  }

  private def queryVersionByEsid(esids: Seq[String],
                                 sinkConfig: SinkProcessConfig,
                                 sinkNamespace: String,
                                 connectionConfig: ConnectionConfig): (Boolean, mutable.HashMap[String, Long]) = {
    val namespace = UmsNamespace(sinkNamespace)
    var queryResult = true
    val esid2VersionMap = mutable.HashMap.empty[String, Long]
    val requestContent = """{"docs":[{"_id":"""" + esids.mkString("\",\"_source\":\"" + UmsSysField.ID.toString + "\"},{\"_id\":\"") + "\",\"_source\":\"" + UmsSysField.ID.toString + "\"}]}"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl + namespace.database + "/" + namespace.table + "/_mget"
    else connectionConfig.connectionUrl + "/" + namespace.database + "/" + namespace.table + "/_mget"
    val responseContent = doHttp(url,connectionConfig.username,connectionConfig.password,requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    if(!checkResponseSuccess(responseJson)){
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
}
