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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.json.JsonParseHelper
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.{SourceMutationType, _IDHelper}
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger
import edp.wormhole.util.JsonUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data2EsSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)
  val optNameUpdate = "update"
  val optNameInsert = "create"

  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logger.info("process KafkaLog2ESSnapshot")
    val sinkSpecificConfig: EsConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        json2caseClass[EsConfig](sinkProcessConfig.specialConfig.get)
      else EsConfig()
    val dataList = ListBuffer.empty[(String, Long, String)]
    for (row <- tupleList) {
      val data = convertJson(row, schemaMap, sinkProcessConfig, sinkSpecificConfig)
      dataList += data
    }
    val namespace = UmsNamespace(sinkNamespace)
    if (!doSinkProcess(sinkProcessConfig, namespace, schemaMap, dataList, connectionConfig, sinkSpecificConfig)) {
      throw new Exception("has error row to insert or update")
    }
  }

  private def convertJson(row: Seq[String],
                          schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                          sinkConfig: SinkProcessConfig,
                          sinkSpecificConfig: EsConfig): (String, Long, String) = {
    val json = new JSONObject
    val umsid =
      if (sinkSpecificConfig.`mutation_type.get` == SourceMutationType.I_U_D.toString) row(schemaMap(UmsSysField.ID.toString)._1).toLong
      else 1L
    for ((name, (index, fieldType, _)) <- schemaMap) {
      val field = row(index)
      val (cname, cvalue) = JsonParseHelper.parseData2CorrectType(fieldType, field: String, name)
      json.put(cname, cvalue)
    }
    val _ids = _IDHelper.get_Ids(row, sinkSpecificConfig.`_id.get`, schemaMap)
    (_ids, umsid, json.toJSONString)
  }

  private def doSinkProcess(sinkConfig: SinkProcessConfig,
                            sinkNamespace: UmsNamespace,
                            schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                            dataList: ListBuffer[(String, Long, String)],
                            connectionConfig: ConnectionConfig,
                            sinkSpecificConfig: EsConfig): Boolean = {
    val cc = EsTools.getAvailableConnection(connectionConfig)
    logger.info("random url:" + cc.connectionUrl)
    if (cc.connectionUrl.isEmpty) new Exception(connectionConfig.connectionUrl + " are all not available")

    val indexName = if (sinkSpecificConfig.index_extend_config.nonEmpty) EsTools.getFullIndexNameByExtentConfig(sinkNamespace.database, sinkSpecificConfig.index_extend_config.get)
    else sinkNamespace.database

    logger.info("index name: " + indexName)

    if (sinkSpecificConfig.`mutation_type.get` == SourceMutationType.I_U_D.toString) {

      val (result, esid2UmsidInEsMap) = {
        val idList = dataList.map(_._1)
        EsTools.queryVersionByEsid(idList, sinkNamespace, cc, indexName)
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
        val insertFlag = doBatchInsert(insertId2JsonMap, sinkConfig, sinkNamespace, cc, indexName)
        val updateFlag = doBatchUpdate(updateId2JsonMap, sinkConfig, sinkNamespace, cc, indexName)
        insertFlag | updateFlag
      }
    } else {
      val insertId2JsonMap = mutable.HashMap.empty[String, String]
      dataList.foreach { case (id, _, json) =>
        insertId2JsonMap(id) = json
      }
      val insertFlag = doBatchInsert(insertId2JsonMap, sinkConfig, sinkNamespace, cc,indexName)
      insertFlag
    }
  }

  private def doBatchInsert(insertId2JsonMap: mutable.HashMap[String, String],
                            sinkConfig: SinkProcessConfig, sinkNamespace: UmsNamespace,
                            connectionConfig: ConnectionConfig,
                            indexName: String): Boolean = {
    if (insertId2JsonMap.nonEmpty) {
      val insertList = ListBuffer.empty[String]
      insertId2JsonMap.foreach(item => {
        insertList += s"""{ "$optNameInsert" : {"_id" : "${item._1}" }}"""
        insertList += item._2
      })
      EsTools.write2Es(insertList, connectionConfig, sinkNamespace, indexName)
    } else true
  }


  private def doBatchUpdate(updateId2JsonMap: mutable.HashMap[String, String],
                            sinkConfig: SinkProcessConfig, sinkNamespace: UmsNamespace,
                            connectionConfig: ConnectionConfig,
                            indexName: String): Boolean = {
    if (updateId2JsonMap.nonEmpty) {
      val updateList = ListBuffer.empty[String]
      updateId2JsonMap.foreach(item => {
        updateList += s"""{ "$optNameUpdate" : {"_id" : "${item._1}" }}"""
        updateList += "{\"doc\":" + item._2 + "}"
      })
      EsTools.write2Es(updateList, connectionConfig, sinkNamespace, indexName)
    } else true
  }

}
