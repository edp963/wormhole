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


package edp.wormhole.sinks.cassandrasink

import com.alibaba.fastjson.JSON
import com.datastax.driver.core.{BatchStatement, BoundStatement, PreparedStatement}
import com.datastax.driver.core.exceptions.{InvalidTypeException, NoHostAvailableException}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.memorystorage.ConfMemoryStorage._
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.sinks.SinkProcessor
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.{UmsFieldType, UmsOpType}
import edp.wormhole.ums.UmsSysField._
import java.lang.{Double, Float, Long}
import edp.wormhole.common.util.DateUtils._


class Data2CassandraSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig:ConnectionConfig) = {
    val schemaStringAndColumnNumber = getSchemaStringAndColumnNumber(schemaMap) //return format : ("(_ums_id_,key,value1,value2)", number)  Tuple2[String, Int]
    val schemaString: String = schemaStringAndColumnNumber._1
    val columnNumber: Int = schemaStringAndColumnNumber._2
    val valueStrByPlaceHolder: String = getStrByPlaceHolder(columnNumber) //format (?,?,?,?,?)
   // val connectionConfig = getDataStoreConnectionsMap(sinkNamespace)


    val authentication = sinkProcessConfig.specialConfig.get
    var user: String = null
    var password: String = null
    if (authentication.length != 0) {
      val json = JSON.parseObject(authentication)
      if (json.containsKey("user") && json.containsKey("password")) {
        user = json.getString("user")
        password = json.getString("password")
      }
    }
    val sortedAddressList = CassandraConnection.getSortedAddress(connectionConfig.connectionUrl)
    val keyspace = sinkNamespace.split("\\.")(2) //use sinkNamespace(2)
    logInfo("keyspace: @@@" + keyspace)
    val table = sinkNamespace.split("\\.")(3) // use sinkConfig.sinknamespace
    val prepareStatement: String = getPrepareStatement(keyspace, table, schemaString, valueStrByPlaceHolder)
    //INSERT INTO keyspace.table (a, b, c, d, e) VALUES(?, ?, ?, ?, ?) USING TIMESTAMP ?;
    val session = CassandraConnection.getSession(sortedAddressList, user, password)
    var prepareSchema: PreparedStatement = null
    try {
      prepareSchema = session.prepare(prepareStatement)
    } catch {
      case e0: NoHostAvailableException => logError("cassandra InShotFile prepare error:", e0)
    }
    val batch = new BatchStatement()
    for (tuple <- tupleList) {
      val bound: BoundStatement = prepareSchema.bind()
      val umsOpValue: String = tuple(schemaMap(OP.toString)._1)
      val umsIdValue: Long = tuple(schemaMap(ID.toString)._1).toLong
      schemaMap.keys.foreach { column =>
        if (!Set(OP.toString).contains(column)) {
          val (index, fieldType, _) = schemaMap(column)
          val valueString = tuple(index)
          if (valueString == null) {
            bound.setToNull(column)
          } else {
            try {
              bindWithDifferentTypes(bound, column, fieldType, valueString)
            } catch {
              case e: Throwable => logError("bindWithDifferentTypes:", e)
            }
          }
        }
      }

      if (UmsOpType.DELETE.toString == umsOpValue.toLowerCase) {
        bound.setBool(columnNumber - 1, java.lang.Boolean.valueOf("false")) //not active--d--false
      } else {
        bound.setBool(columnNumber - 1, java.lang.Boolean.valueOf("true")) // active--i,u--true
      }
      bound.setLong(columnNumber, umsIdValue) //set ID
      if (batch.size() >= 100) {
        session.execute(batch)
        batch.clear()
      }
      batch.add(bound)
    }
    session.execute(batch)

  }

  private def getSchemaStringAndColumnNumber(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]) = {
    var columnCounter: Int = 0
    val strBuilder = StringBuilder.newBuilder
    strBuilder.append("(")
    schemaMap.keys.foreach { column =>
      if (!Set(OP.toString).contains(column)) {
        columnCounter += 1
        strBuilder.append(column)
        strBuilder.append(", ")
      }
    }
    strBuilder.append("ums_active_)")
    columnCounter += 1 // for "active"
    (strBuilder.toString(), columnCounter)
  }

  private def getStrByPlaceHolder(columnNumber: Int): String = {
    val strBuilder = StringBuilder.newBuilder
    strBuilder.append("VALUES(")
    var counter = 1
    while (counter < columnNumber) {
      strBuilder.append("?, ")
      counter += 1
    }
    strBuilder.append("?)")
    strBuilder.toString()
  }

  private def getPrepareStatement(keyspace: String, table: String, schemaString: String, valueStrByPlaceHolder: String): String = {
    val strBuilder = StringBuilder.newBuilder
    strBuilder.append("INSERT INTO ")
    strBuilder.append(keyspace)
    strBuilder.append(".")
    strBuilder.append(table)
    strBuilder.append(" ")
    strBuilder.append(schemaString)
    strBuilder.append(" ")
    strBuilder.append(valueStrByPlaceHolder)
    strBuilder.append(" USING TIMESTAMP ?;")
    val temp = strBuilder.toString()
    println(temp)
    temp
  }

  private def bindWithDifferentTypes(bound: BoundStatement, columnName: String, fieldType: UmsFieldType, value: String): Unit = fieldType match {
    case UmsFieldType.STRING => bound.setString(columnName, value.trim)
    case UmsFieldType.INT => bound.setInt(columnName, Integer.valueOf(value.trim))
    case UmsFieldType.LONG => bound.setLong(columnName, Long.valueOf(value.trim))
    case UmsFieldType.FLOAT => bound.setFloat(columnName, Float.valueOf(value.trim))
    case UmsFieldType.DOUBLE => bound.setDouble(columnName, Double.valueOf(value.trim))
    case UmsFieldType.BOOLEAN => bound.setBool(columnName, java.lang.Boolean.valueOf(value.trim))
    case UmsFieldType.DATE => bound.setTimestamp(columnName, dt2date(value.trim))
    case UmsFieldType.DATETIME => bound.setTimestamp(columnName, dt2date(value.trim))
    case UmsFieldType.DECIMAL => bound.setDecimal(columnName, new java.math.BigDecimal(value.trim).stripTrailingZeros())
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
  }
}
