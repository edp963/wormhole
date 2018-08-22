/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole.flinkx.swifts


import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.sinks.kudu.KuduConnection
import edp.wormhole.swifts.{ConnectionMemoryStorage, DbType}
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.kudu.Type
import org.apache.kudu.client.KuduTable
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LookupKuduHelper extends java.io.Serializable {

  private val logger = Logger.getLogger(this.getClass)


  def getFieldsArray(fields: String): Array[(String, String)] = {
    fields.split(",").map(f => {
      val trimF = f.trim
      val lowerF = trimF.toLowerCase
      val asPosition = lowerF.indexOf(" as ")
      if (asPosition > 0) (trimF.substring(0, asPosition).trim, trimF.substring(asPosition + 4).trim)
      else (trimF, trimF)
    })
  }

  def getDbOutPutSchemaMap(swiftsSql: SwiftsSql,tableSchema: mutable.Map[String, String]): Map[String, (String, Int, String)] = {
    //rename, (type, index, name)
    var fieldIndex: Int = -1
    swiftsSql.fields.get.split(",").map(str => {
      val trimF = str.trim
      val lowerF = trimF.toLowerCase
      val asPosition = lowerF.indexOf(" as ")
      fieldIndex += 1
      if (asPosition > 0) {
        val renameField: String = trimF.substring(asPosition + 4).trim
        val nameField: String = trimF.substring(0, asPosition).trim
        val fieldType: String = tableSchema(nameField).toString.toUpperCase()
        (renameField, (DbType.convert(fieldType), fieldIndex, nameField))
      }
      else (trimF, (DbType.convert(tableSchema(trimF).toString.toUpperCase()), fieldIndex, trimF))
    }).toMap
  } //order is not same as input order !!!


  def getLookupSchemaMap(preSchemaMap: Map[String, (TypeInformation[_], Int)], swiftsSql: SwiftsSql, tableSchema: mutable.Map[String, String]): Map[String, (TypeInformation[_], Int)] = {
    val lookupSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    val dbOutPutSchemaMap: Map[String, (String, Int, String)] = getDbOutPutSchemaMap(swiftsSql, tableSchema)
    preSchemaMap.foreach(entry => {
      lookupSchemaMap += entry._1 -> entry._2
    })
    dbOutPutSchemaMap.foreach(entry => {
      val fieldName = entry._1
      val fieldType = FlinkSchemaUtils.s2FlinkType(entry._2._1)
      val fieldIndex = entry._2._2 + preSchemaMap.size
      lookupSchemaMap += fieldName -> (fieldType, fieldIndex)
    })
    lookupSchemaMap.toMap
  }


  def covertResultSet2Map(swiftsSql: SwiftsSql,
                          row: Row,
                          preSchemaMap: Map[String, (TypeInformation[_], Int)],
                          tableSchemaInKudu: mutable.HashMap[String, Type],
                          dataStoreConnectionsMap: Map[String, ConnectionConfig]): mutable.HashMap[String, ListBuffer[Array[Any]]] = {

    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)

    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[Any]]]
    try {
      //connect
      val database = swiftsSql.lookupNamespace.get.split("\\.")(2)
      val fromIndex = swiftsSql.sql.indexOf(" from ")
      val afterFromSql = swiftsSql.sql.substring(fromIndex + 6).trim
      val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
      val tableName = KuduConnection.getTableName(tmpTableName, database)
      logger.info("tableName:" + tableName)
      KuduConnection.initKuduConfig(connectionConfig)

      val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
      val table: KuduTable = client.openTable(tableName)
      val dataJoinNameArray = swiftsSql.sourceTableFields.get

      //value
      val sourceTableFields: Array[String] = if (swiftsSql.sourceTableFields.isDefined) swiftsSql.sourceTableFields.get else null
      val lookupTableFields = if (swiftsSql.lookupTableFields.isDefined) swiftsSql.lookupTableFields.get else null
      val joinFieldsValueArray: Array[Any] = joinFieldsInRow(row, lookupTableFields, sourceTableFields, preSchemaMap)
      val joinFieldsValueString: Array[String] = joinFieldsValueArray.map(value => value.toString)
      val selectFieldNewNameArray: Seq[String] = getFieldsArray(swiftsSql.fields.get).map(_._1).toList
      val queryResult: (String, Map[String, (Any, String)]) = KuduConnection.doQueryByKey(lookupTableFields, joinFieldsValueString.toList, tableSchemaInKudu, client, table, selectFieldNewNameArray)
      //keyStr
      val joinFieldsAsKey: String = queryResult._1
      val queryFieldsResultMap: Map[String, (Any, String)] = queryResult._2
      //output fields
      val tableSchema: mutable.Map[String, String] = KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)
      val dbOutPutSchemaMap = getDbOutPutSchemaMap(swiftsSql, tableSchema)

      val arrayBuf: Array[Any] = Array.fill(dbOutPutSchemaMap.size) {
        ""
      }
      dbOutPutSchemaMap.foreach { case (rename, (dataType, index, name)) =>
        val value = if(queryFieldsResultMap.contains(name)) queryFieldsResultMap(name)._1 else null.asInstanceOf[String]
        //val value = queryFieldsResultMap(name)._1
        arrayBuf(index) = if (value != null) {
          if (dataType == UmsFieldType.BINARY.toString) CommonUtils.base64byte2s(value.asInstanceOf[Array[Byte]])
          else FlinkSchemaUtils.object2TrueValue(FlinkSchemaUtils.s2FlinkType(dataType), value)
        } else null
      }

      if (!dataTupleMap.contains(joinFieldsAsKey)) {
        dataTupleMap(joinFieldsAsKey) = ListBuffer.empty[Array[Any]]
      }
      dataTupleMap(joinFieldsAsKey) += arrayBuf
      KuduConnection.closeClient(client)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
    dataTupleMap
  }

  def joinFieldsInRow(row: Row,
                      lookupTableFields: Array[String],
                      sourceTableFields: Array[String],
                      preSchemaMap: Map[String, (TypeInformation[_], Int)]): Array[Any] = {
    val fieldContent = sourceTableFields.map(fieldName => {
      val value = FlinkSchemaUtils.object2TrueValue(preSchemaMap(fieldName)._1, row.getField(preSchemaMap(fieldName)._2))
      if (value != null) value else "N/A"
    })
    if (!fieldContent.contains("N/A")) {
      fieldContent
    } else Array.empty[Any]
  }
}


