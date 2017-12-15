package edp.wormhole.sinks.jsonsink.elasticsearchjsonsink

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.sinks.SourceMutationType.INSERT_ONLY
import edp.wormhole.sinks.jsonsink.JsonParseHelper
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
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
    SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`) match {
      case INSERT_ONLY =>
        logInfo("insert only process")
        val result = insertOnly(tupleList, targetSchemaArr, sinkMap, sinkNamespace, cc)
        if (!result) throw new Exception("has error row for insert only")
      case _ =>
        logInfo("insert and update process")
        val result = insertOrUpdate(tupleList, targetSchemaArr, sinkMap, sinkProcessConfig, sinkNamespace, cc)
        if (!result) throw new Exception("has error row for insert or update")
    }
  }

  private def insertOrUpdate(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)], sinkConfig: SinkProcessConfig, sinkNamespace: String, cc: ConnectionConfig): Boolean = {
    val dataList = ListBuffer.empty[(String, Long, String)]
    for (row <- tupleList) {
      val jsonData = JsonParseHelper.jsonObjHelper(row, sinkMap, targetSchemaArr)
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
    if (insertId2JsonMap.nonEmpty) {
      val insertList = ListBuffer.empty[String]
      insertId2JsonMap.foreach(item => {
        insertList += s"""{ "$optNameInsert" : {"_id" : "${item._1}" }}"""
        insertList += item._2
      })
      write2Es(insertList, connectionConfig, sinkNamespace)
    } else true
  }




  private def doBatchUpdate(updateId2JsonMap: mutable.HashMap[String, String],
                            sinkConfig: SinkProcessConfig, sinkNamespace: String,
                            connectionConfig: ConnectionConfig): Boolean = {
    if (updateId2JsonMap.nonEmpty) {
      val updateList = ListBuffer.empty[String]
      updateId2JsonMap.foreach(item => {
        updateList += s"""{ "$optNameUpdate" : {"_id" : "${item._1}" }}"""
        updateList += "{\"doc\":" + item._2 + "}"
      })
      write2Es(updateList, connectionConfig, sinkNamespace)
    } else true
  }


  private def insertOnly(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)], sinkNamespace: String, connectionConfig: ConnectionConfig): Boolean = {
    val insertList = ListBuffer.empty[String]
    if (tupleList.nonEmpty) {
      for (row <- tupleList) {
        val data = JsonParseHelper.jsonObjHelper(row, sinkMap, targetSchemaArr).toJSONString
        val uuid = UUID.randomUUID().toString
        insertList += s"""{ "$optNameInsert" : {"_id" : "${uuid}" }}"""
        insertList += data
      }
      write2Es(insertList, connectionConfig, sinkNamespace)
    } else true
  }

  private def write2Es(list: ListBuffer[String], connectionConfig: ConnectionConfig, sinkNamespace: String): Boolean = {
    val namespace = UmsNamespace(sinkNamespace)
    val requestContent = list.mkString("\n") + "\n"
    val url = if (connectionConfig.connectionUrl.trim.endsWith("/")) connectionConfig.connectionUrl.trim + namespace.database + "/" + namespace.table + "/_bulk"
    else connectionConfig.connectionUrl.trim + "/" + namespace.database + "/" + namespace.table + "/_bulk"
    logInfo("doBatch url:" + url)
    val responseContent = doHttp(url, connectionConfig.username, connectionConfig.password, requestContent)
    val responseJson: JValue = json2jValue(responseContent)
    checkResponseSuccess(responseJson)
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
