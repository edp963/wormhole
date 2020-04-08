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

import com.alibaba.fastjson.{JSON, JSONArray}
import edp.wormhole.common.json.JsonParseHelper
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.SourceMutationType.INSERT_ONLY
import edp.wormhole.sinks.{SourceMutationType, _IDHelper}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataJson2EsSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)
  val optNameUpdate = "update"
  val optNameInsert = "create"

  override def process(sourceNamespace: String,
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
    val sinkSpecificConfig: EsConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        JsonUtils.json2caseClass[EsConfig](sinkProcessConfig.specialConfig.get)
      else EsConfig()
    val cc = EsTools.getAvailableConnection(connectionConfig, sinkSpecificConfig)
    logger.info("random url:" + cc.connectionUrl)
    if (cc.connectionUrl.isEmpty) new Exception(connectionConfig.connectionUrl + " are all not available")


    val namespace = UmsNamespace(sinkNamespace)
    val indexName = if (sinkSpecificConfig.index_extend_config.nonEmpty) EsTools.getFullIndexNameByExtentConfig(namespace.database, sinkSpecificConfig.index_extend_config.get)
    else namespace.database

    SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`) match {
      case INSERT_ONLY =>
        logger.info("insert only process")
        val result = insertOnly(tupleList, targetSchemaArr, sinkMap, namespace, cc, sinkSpecificConfig, schemaMap,indexName)
        if (!result) throw new Exception("has error row for insert only")
      case _ =>
        logger.info("insert and update process")
        val result = insertOrUpdate(tupleList, targetSchemaArr, sinkMap, sinkSpecificConfig, namespace, cc, schemaMap,indexName)
        if (!result) throw new Exception("has error row for insert or update")
    }
  }

  private def insertOrUpdate(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                             sinkSpecificConfig: EsConfig, sinkNamespace: UmsNamespace, cc: ConnectionConfig,
                             schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],indexName:String): Boolean = {
    val dataList = ListBuffer.empty[(String, Long, String)]
    for (row <- tupleList) {
      val jsonData = JsonParseHelper.jsonObjHelper(row, sinkMap, targetSchemaArr)
      val umsId = jsonData.getLong(UmsSysField.ID.toString)
      val data = jsonData.toJSONString
      val _ids = _IDHelper.get_Ids(row, sinkSpecificConfig.`_id.get`, schemaMap)
      dataList.append((_ids, umsId, data))
    }

    val (result, esid2UmsidInEsMap) = {
      val idList = dataList.map(_._1)
      EsTools.queryVersionByEsid(idList, sinkNamespace, cc,indexName, sinkSpecificConfig)
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
      val insertFlag = doBatchInsert(insertId2JsonMap, sinkNamespace, cc,indexName, sinkSpecificConfig)
      val updateFlag = doBatchUpdate(updateId2JsonMap, sinkNamespace, cc,indexName, sinkSpecificConfig)
      insertFlag | updateFlag
    }
  }

  private def doBatchInsert(insertId2JsonMap: mutable.HashMap[String, String],
                            sinkNamespace: UmsNamespace,
                            connectionConfig: ConnectionConfig,
                            indexName:String,
                            sinkSpecificConfig: EsConfig): Boolean = {
    if (insertId2JsonMap.nonEmpty) {
      val insertList = ListBuffer.empty[String]
      insertId2JsonMap.foreach(item => {
        insertList += s"""{ "$optNameInsert" : {"_id" : "${item._1}" }}"""
        insertList += item._2
      })
      EsTools.write2Es(insertList, connectionConfig, sinkNamespace,indexName, sinkSpecificConfig)
    } else true
  }

  private def doBatchUpdate(updateId2JsonMap: mutable.HashMap[String, String],
                            sinkNamespace: UmsNamespace,
                            connectionConfig: ConnectionConfig,
                            indexName:String,
                            sinkSpecificConfig: EsConfig): Boolean = {
    if (updateId2JsonMap.nonEmpty) {
      val updateList = ListBuffer.empty[String]
      updateId2JsonMap.foreach(item => {
        updateList += s"""{ "$optNameUpdate" : {"_id" : "${item._1}" }}"""
        updateList += "{\"doc\":" + item._2 + "}"
      })
      EsTools.write2Es(updateList, connectionConfig, sinkNamespace,indexName,sinkSpecificConfig: EsConfig)
    } else true
  }


  private def insertOnly(tupleList: Seq[Seq[String]], targetSchemaArr: JSONArray, sinkMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                         sinkNamespace: UmsNamespace, connectionConfig: ConnectionConfig, sinkSpecificConfig: EsConfig,
                         schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                         indexName:String): Boolean = {
    val insertList = ListBuffer.empty[String]
    if (tupleList.nonEmpty) {
      for (row <- tupleList) {
        val data = JsonParseHelper.jsonObjHelper(row, sinkMap, targetSchemaArr).toJSONString
        val _id = _IDHelper.get_Ids(row, sinkSpecificConfig.`_id.get`, schemaMap)

        insertList += s"""{ "$optNameInsert" : {"_id" : "${_id}" }}"""
        insertList += data
      }
      EsTools.write2Es(insertList, connectionConfig, sinkNamespace,indexName, sinkSpecificConfig)
    } else true
  }


}
