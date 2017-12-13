package edp.wormhole.sinks.elasticsearchJsonSink

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.util.JsonUtils.{containsName, getBoolean, json2jValue}
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.json4s.JValue

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaj.http.Http

class DataJson2EsSink extends SinkProcessor with EdpLogging {
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
    for (row <- tupleList) {


      val data = jsonObjHelper(row, sinkMap, targetSchemaArr).toJSONString


      val namespace = UmsNamespace(sinkNamespace)

      val insertList = ListBuffer.empty[String]

      insertList += s"""{ "create" : {"_id" : "1" }}"""
      insertList += data

      val requestContent = insertList.mkString("\n") + "\n"
      val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
      else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
      logInfo("doBatch url:" + url)
      val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
      val responseJson: JValue = json2jValue(responseContent)
      println(checkResponseSuccess(responseJson))


    }
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
          val value = str2Json(schemaObj.getString("name"), jsonArray.getJSONObject(i).get(schemaObj.getString("name")).toString,schemaObj.getString("type"), if (schemaObj.containsKey("sub_fields")) Some(schemaObj.getJSONArray("sub_fields")) else None)
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

}
