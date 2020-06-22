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

package edp.wormhole.flinkx.swifts.custom

import edp.wormhole.flinkx.swifts.LookupHelper
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.kuduconnection.KuduConnection
import edp.wormhole.swifts.ConnectionMemoryStorage
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.kudu.client.KuduTable
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

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


  def covertResultSet2Map(swiftsSql: SwiftsSql,
                          row: Row,
                          preSchemaMap: Map[String, (TypeInformation[_], Int)],
                          dataStoreConnectionsMap: Map[String, ConnectionConfig],
                          selectFields: List[String]): mutable.HashMap[String, ListBuffer[Array[Any]]] = {

    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)

    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[Any]]]
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    try {
      //connect
      val database = swiftsSql.lookupNamespace.get.split("\\.")(2)
      val fromIndex = swiftsSql.sql.indexOf(" from ")
      val afterFromSql = swiftsSql.sql.substring(fromIndex + 6).trim
      val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
      val tableName = KuduConnection.getTableName(tmpTableName, database)
      logger.info("tableName:" + tableName)

      val table: KuduTable = client.openTable(tableName)

      //value
      val sourceTableFields: Array[String] = if (swiftsSql.sourceTableFields.isDefined) swiftsSql.sourceTableFields.get else null
      val lookupTableFields = if (swiftsSql.lookupTableFields.isDefined) swiftsSql.lookupTableFields.get else null
      val joinFieldsValueArray: Array[Any] = LookupHelper.joinFieldsInRow(row, lookupTableFields, sourceTableFields, preSchemaMap, false)
      val joinFieldsValueString: Array[String] = joinFieldsValueArray.map(value => value.toString)
      val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
      val queryResult: (String, Map[String, (Any, String)]) = KuduConnection.doQueryByKey(lookupTableFields, joinFieldsValueString.toList, tableSchemaInKudu, client, table, selectFields)
      //keyStr
      val joinFieldsAsKey: String = queryResult._1
      val queryFieldsResultMap: Map[String, (Any, String)] = queryResult._2
      //output fields
      val dbOutPutSchemaMap = LookupHelper.getDbOutPutSchemaMap(swiftsSql)

      val arrayBuf: Array[Any] = Array.fill(dbOutPutSchemaMap.size) {
        ""
      }
      dbOutPutSchemaMap.foreach { case (name, (_, dataType, index)) =>
        val value = if (queryFieldsResultMap.nonEmpty && queryFieldsResultMap.contains(name)) queryFieldsResultMap(name)._1 else null.asInstanceOf[String]
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
      logger.info(s"query data from table $tableName success")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    } finally {
      KuduConnection.closeClient(client)
    }
    dataTupleMap
  }
}


