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

import java.sql.{Connection, ResultSet}

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.swifts.{ConnectionMemoryStorage, SwiftsConstants}
import edp.wormhole.ums.{UmsDataSystem, UmsFieldType}
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.apache.log4j.Logger

import scala.collection.{mutable,Map}
import scala.collection.mutable.ListBuffer

object LookupHelper extends java.io.Serializable {

  private val logger = Logger.getLogger(this.getClass)

  def getDbOutPutSchemaMap(swiftsSql: SwiftsSql): Map[String, (String, String, Int)] = {
    var fieldIndex: Int = -1
    swiftsSql.fields.get.split(",").map(field => {
      fieldIndex += 1
      val fields = field.split(":")
      val fields1trim = fields(1).trim.toLowerCase()
      val fieldTuple = if (fields1trim.contains(" as ")) {
        val asIndex = fields1trim.indexOf(" as ")
        val fieldType = fields1trim.substring(0, asIndex).trim
        val newName = fields1trim.substring(asIndex + 4).trim
        (fields(0).trim, (newName, fieldType, fieldIndex))
      } else {
        (fields(0).trim, (fields(0).trim, fields(1).trim, fieldIndex))
      }
      fieldTuple
    }).toMap
  } //order is not same as input order !!!

  def getLookupSchemaMap(preSchemaMap: Map[String, (TypeInformation[_], Int)], swiftsSql: SwiftsSql): Map[String, (TypeInformation[_], Int)] = {
    val lookupSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    val dbOutPutSchemaMap: Map[String, (String, String, Int)] = getDbOutPutSchemaMap(swiftsSql)
    preSchemaMap.foreach(entry => {
      lookupSchemaMap += entry._1 -> entry._2
    })
    dbOutPutSchemaMap.foreach(entry => {
      val fieldName = entry._2._1
      val fieldType = FlinkSchemaUtils.s2FlinkType(entry._2._2)
      val fieldIndex = entry._2._3 + preSchemaMap.size
      lookupSchemaMap += fieldName -> (fieldType, fieldIndex)
    })
    lookupSchemaMap.toMap
  }

  def covertResultSet2Map(swiftsSql: SwiftsSql,
                          row: Row,
                          preSchemaMap: Map[String, (TypeInformation[_], Int)],
                          dataStoreConnectionsMap: Map[String, ConnectionConfig]): mutable.HashMap[String, ListBuffer[Array[Any]]] = {


    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    var conn: Connection = null
    var rs: ResultSet = null
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)
    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[Any]]]
    try {
      conn = DbConnection.getConnection(connectionConfig)
      val statement = conn.createStatement()
      val executeSql = getExecuteSql(swiftsSql, row, preSchemaMap)
      logger.info(executeSql + "lookup sql")
      rs = statement.executeQuery(executeSql)
      val lookupTableFieldsAlias: Array[String] = if (swiftsSql.lookupTableFieldsAlias.isDefined) swiftsSql.lookupTableFieldsAlias.get else null
      val dbOutPutSchemaMap = getDbOutPutSchemaMap(swiftsSql)
      while (rs != null && rs.next) {
        val tmpMap = mutable.HashMap.empty[String, Any]
        val arrayBuf: Array[Any] = Array.fill(dbOutPutSchemaMap.size) {
          ""
        }
        dbOutPutSchemaMap.foreach { case (_, (name, dataType, index)) =>
          val value = rs.getObject(name)
          arrayBuf(index) = if (value != null) {
            if (dataType == UmsFieldType.BINARY.toString) CommonUtils.base64byte2s(value.asInstanceOf[Array[Byte]])
            else FlinkSchemaUtils.object2TrueValue(FlinkSchemaUtils.s2FlinkType(dataType), value)
          } else null
          tmpMap(name) = arrayBuf(index)
        }

        val joinFieldsAsKey = lookupTableFieldsAlias.map(name => {
          if (tmpMap.contains(name)) tmpMap(name) else rs.getObject(name).toString
        }).mkString("_")
        if (!dataTupleMap.contains(joinFieldsAsKey)) {
          dataTupleMap(joinFieldsAsKey) = ListBuffer.empty[Array[Any]]
        }
        dataTupleMap(joinFieldsAsKey) += arrayBuf
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    } finally {
      if (null != conn)
        conn.close()
    }
    dataTupleMap
  }

  private def getExecuteSql(swiftsSql: SwiftsSql,
                            row: Row,
                            preSchemaMap: Map[String, (TypeInformation[_], Int)]): String = {
    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val dataSystem = lookupNamespace.substring(0, lookupNamespace.indexOf(".")).toLowerCase()
    val sourceTableFields: Array[String] = if (swiftsSql.sourceTableFields.isDefined) swiftsSql.sourceTableFields.get else null
    val lookupTableFields = if (swiftsSql.lookupTableFields.isDefined) swiftsSql.lookupTableFields.get else null
    val sql = swiftsSql.sql
    val joinFieldsValueArray: Array[Any] = joinFieldsInRow(row, lookupTableFields, sourceTableFields, preSchemaMap, true)
    UmsDataSystem.dataSystem(dataSystem) match {
      case UmsDataSystem.CASSANDRA => getCassandraSql(joinFieldsValueArray, sql, lookupTableFields)
      case _ => getRmdbSql(joinFieldsValueArray, sql, lookupTableFields)
    }
  }

  def joinFieldsInRow(row: Row,
                      lookupTableFields: Array[String],
                      sourceTableFields: Array[String],
                      preSchemaMap: Map[String, (TypeInformation[_], Int)],
                      exeSql: Boolean): Array[Any] = {
    val fieldContent = sourceTableFields.map(fieldName => {
      var value = FlinkSchemaUtils.object2TrueValue(preSchemaMap(fieldName.trim)._1, row.getField(preSchemaMap(fieldName.trim)._2))
      value =if (value != null) value else "N/A"
      if (exeSql && (preSchemaMap(fieldName)._1 == Types.STRING || preSchemaMap(fieldName)._1 == Types.SQL_TIMESTAMP || preSchemaMap(fieldName)._1 == Types.SQL_DATE))
        "'" + value + "'"
      else value
    })
    if (!fieldContent.contains("N/A")) {
      fieldContent
    } else Array.empty[Any]
  }

  private def getRmdbSql(joinFieldsValueArray: Array[Any],
                         sql: String,
                         lookupTableFields: Array[String]): String = {
    if (joinFieldsValueArray.nonEmpty) {
      if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + s" = ${joinFieldsValueArray.mkString("")}")
      else {
        var index = -1
        val whereClause = lookupTableFields.map(field => {
          index += 1
          s"""$field = ${joinFieldsValueArray(index)}"""
        }).mkString(" AND ")
        sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, whereClause)
      }
    } else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
  }

  private def getCassandraSql(joinFieldsValueArray: Array[Any],
                              sql: String,
                              lookupTableFields: Array[String]
                             ): String = {
    if (lookupTableFields.length == 1)
      sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in (" + joinFieldsValueArray.mkString(",") + ")")
    else
      sql
  }
}
