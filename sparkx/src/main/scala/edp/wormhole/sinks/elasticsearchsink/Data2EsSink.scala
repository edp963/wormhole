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
import edp.wormhole.common.{ConnectionConfig, JsonParseHelper}
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsSysField
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.common.util.JsonUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    val sinkSpecificConfig: EsConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        json2caseClass[EsConfig](sinkProcessConfig.specialConfig.get)
      else EsConfig()
    val dataList = ListBuffer.empty[(String, Long, String)]
    for (row <- tupleList) {
      val data = convertJson(row, schemaMap, sinkProcessConfig, sinkSpecificConfig)
      dataList += data
    }
    if (!doSinkProcess(sinkProcessConfig, sinkNamespace, schemaMap, dataList, connectionConfig, sinkSpecificConfig)) {
      throw new Exception("has error row to insert or update")
    }
  }

  private def convertJson(row: Seq[String],
                          schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                          sinkConfig: SinkProcessConfig,
                          sinkSpecificConfig: EsConfig): (String, Long, String) = {
    val json = new JSONObject
    val umsid = row(schemaMap(UmsSysField.ID.toString)._1).toLong
    for ((name, (index, fieldType, _)) <- schemaMap) {
      val field = row(index)
      val (cname, cvalue) = JsonParseHelper.parseData2CorrectType(fieldType, field: String, name)
      json.put(cname, cvalue)
    }
    val _ids = EsTools.getEsId(row, sinkSpecificConfig, schemaMap)
    (_ids, umsid, json.toJSONString)
  }

  private def doSinkProcess(sinkConfig: SinkProcessConfig,
                            sinkNamespace: String,
                            schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                            dataList: ListBuffer[(String, Long, String)],
                            connectionConfig: ConnectionConfig,
                            sinkSpecificConfig: EsConfig): Boolean = {
    val cc = EsTools.getAvailableConnection(connectionConfig)
    logInfo("random url:" + cc.connectionUrl)
    if (cc.connectionUrl.isEmpty) new Exception(connectionConfig.connectionUrl + " are all not available")

    if (sinkSpecificConfig.`mutation_type.get` == SourceMutationType.I_U_D.toString) {

      val (result, esid2UmsidInEsMap) = {
        val idList = dataList.map(_._1)
        EsTools.queryVersionByEsid(idList, sinkNamespace, cc)
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
    if (insertId2JsonMap.nonEmpty) {
      val insertList = ListBuffer.empty[String]
      insertId2JsonMap.foreach(item => {
        insertList += s"""{ "$optNameInsert" : {"_id" : "${item._1}" }}"""
        insertList += item._2
      })
      EsTools.write2Es(insertList, connectionConfig, sinkNamespace)
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
      EsTools.write2Es(updateList, connectionConfig, sinkNamespace)
    } else true
  }

}
