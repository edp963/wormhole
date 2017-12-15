package edp.wormhole.sinks.elasticsearchJsonSink

import java.util.UUID

import edp.wormhole.common.util.JsonUtils._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.SourceMutationType.INSERT_ONLY
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.json4s.JValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaj.http.Http

class DataJson2EsSink extends SinkProcessor with EdpLogging {
  val optNameUpdate = "update"
  val optNameInsert = "create"

  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val sinkMap = schemaMap.map { case (name, (index, umsType, nullable)) =>
      if (name == UmsSysField.OP.toString) (UmsSysField.ACTIVE.toString, (index, UmsFieldType.INT, nullable))
      else (name, (index, umsType, nullable))
    }.toMap
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    val cc = getAvailableConnection(connectionConfig)
    logInfo("random url:" + cc.connectionUrl)
    if (cc.connectionUrl.isEmpty) new Exception(connectionConfig.connectionUrl + " are all not available")
    val sinkSpecificConfig = json2caseClass[EsJsonConfig](sinkProcessConfig.specialConfig.get)
    val namespace: UmsNamespace = UmsNamespace(sinkNamespace)
    SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`) match {
      case INSERT_ONLY =>
        val result = insertOnly(tupleList, targetSchemaArr, sinkMap, namespace, cc)
        if (!result) throw new Exception("has error row for insert only")
      case _ =>
        val result = insertOrUpdate(tupleList, targetSchemaArr, sinkMap, sinkProcessConfig, sinkNamespace, cc)
        if (!result) throw new Exception("has error row for insert or update")
    }
  }

  private def insertOrUpdate(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)], sinkConfig: SinkProcessConfig, sinkNamespace: String, cc: ConnectionConfig): Boolean = {
    val dataList = ListBuffer.empty[(String, Long, String)]
    for (row <- tupleList) {
      val jsonData = jsonObjHelper(row, sinkMap, targetSchemaArr)
      val umsId = jsonData.getLong(UmsSysField.ID.toString)
      val data = jsonData.toJSONString
      val _ids = ListBuffer.empty[String]
      sinkConfig.tableKeyList.foreach(keyname => {
        val (index, _, _) = sinkMap(keyname)
        _ids += row(index)
      })
      dataList.append((_ids.mkString("_"), umsId, data))
    }


    val (result, esid2UmsidInEsMap) = {
      val idList = dataList.map(_._1)
      queryVersionByEsid(idList, sinkConfig, sinkNamespace, cc)
    }

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
      val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
      val responseJson: JValue = json2jValue(responseContent)
      checkResponseSuccess(responseJson)
    } else true
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
      val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
      val responseJson: JValue = json2jValue(responseContent)
      checkResponseSuccess(responseJson)
    } else true
  }


  private def insertOnly(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)], namespace: UmsNamespace, connectionConfig: ConnectionConfig): Boolean = {
    val insertList = ListBuffer.empty[String]
    if (insertList.nonEmpty) {
      for (row <- tupleList) {
        val data = jsonObjHelper(row, sinkMap, targetSchemaArr).toJSONString
        val uuid = UUID.randomUUID().toString
        insertList += s"""{ "$optNameInsert" : {"_id" : "${uuid}" }}"""
        insertList += data
      }
      doBatchInsert(insertList, connectionConfig, namespace)
    } else true
  }

  private def doBatchInsert(insertList: ListBuffer[String], connectionConfig: ConnectionConfig, namespace: UmsNamespace): Boolean = {
    val requestContent = insertList.mkString("\n") + "\n"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
    else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
    logInfo("doBatch url:" + url)
    val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    checkResponseSuccess(responseJson)
  }

  private def jsonObjHelper(tuple: Seq[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], subFields: JSONArray): JSONObject = {
    val outputJson = new JSONObject()
    val size = subFields.size()
    for (i <- 0 until size) {
      val jsonObj = subFields.getJSONObject(i)
      val name = jsonObj.getString("name")
      val dataType = jsonObj.getString("type")
      if (schemaMap.contains(name)) {
        val subFields: Option[JSONArray] = if (jsonObj.containsKey("sub_fields")) Some(jsonObj.getJSONArray("sub_fields")) else None
        val value = str2Json(name, tuple(schemaMap(name)._1), dataType, subFields)
        outputJson.put(name, value)
      } else {
        assert(dataType == "jsonobject", "name: " + name + " not found, it should be jsonobject, but it is " + dataType)
        val subFields: JSONArray = jsonObj.getJSONArray("sub_fields")
        val subJsonObj: JSONObject = jsonObjHelper(tuple, schemaMap, subFields)
        outputJson.put(name, subJsonObj)
      }
    }
    outputJson
  }

  private def str2Json(outerName: String, data: String, dataType: String, subFieldsOption: Option[JSONArray]): Any = {
    if (dataType == "jsonobject") {
      val jsonData = JSON.parseObject(data)
      val outputJson = new JSONObject()
      val subFields = subFieldsOption.get
      val size = subFields.size()
      for (i <- 0 until size) {
        val jsonObj = subFields.getJSONObject(i)
        val name = jsonObj.getString("name")
        val subDataType = jsonObj.getString("type")
        val subData = jsonData.getString(name)
        val subSubFields = if (jsonObj.containsKey("sub_fields")) Some(jsonObj.getJSONArray("sub_fields")) else None
        val subResult: Any = str2Json(name, subData, subDataType, subSubFields)
        outputJson.put(name, subResult)
      }
      outputJson
    } else if (dataType == "jsonarray") {
      val jsonArray = JSON.parseArray(data)
      val jsonArraySubFields = subFieldsOption.get
      val dataSize = jsonArray.size()
      val schemaSize = jsonArraySubFields.size()
      val result = new JSONArray()
      for (i <- 0 until dataSize) {
        val outputJson = new JSONObject()
        for (j <- 0 until schemaSize) {
          val schemaObj = jsonArraySubFields.getJSONObject(j)
          val value = str2Json(schemaObj.getString("name"), jsonArray.getJSONObject(i).get(schemaObj.getString("name")).toString, schemaObj.getString("type"), if (schemaObj.containsKey("sub_fields")) Some(schemaObj.getJSONArray("sub_fields")) else None)
          outputJson.put(schemaObj.getString("name"), value)
        }
        result.add(outputJson)
      }
      result
    } else if (dataType.endsWith("array")) {
      JSON.parseArray(data)
    } else {
      if (outerName == UmsSysField.ACTIVE.toString)
        data match {
          case "i" | "u" => "1"
          case "d" => "0"
          case _ => "-1"
        } else {
        data
      }
    }
  }

  private def checkResponseSuccess(responseJson: JValue): Boolean = {
    val result = (!containsName(responseJson, "error") || !getBoolean(responseJson, "error")) && (!containsName(responseJson, "errors") || !getBoolean(responseJson, "errors"))
    if (!result) logError("batch operation has error:" + responseJson)
    result
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
        doHttp(url, cc.username, cc.password, "")
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

  private def doHttp(url: String, username: Option[String], passwd: Option[String], requestContent: String): String = {
    println("+++++++++++++++++requestContent++++++++++++++++++++++")
    println(requestContent)
    println("=======================================")
    if (username.nonEmpty && username.get.nonEmpty && passwd.nonEmpty && passwd.get.nonEmpty) {
      Http(url).auth(username.get, passwd.get).postData(requestContent).asString.body
    } else {
      Http(url).postData(requestContent).asString.body
    }
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
    val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    if (!checkResponseSuccess(responseJson)) {
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
