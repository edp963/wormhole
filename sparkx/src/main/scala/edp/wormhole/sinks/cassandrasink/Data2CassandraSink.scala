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

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions. NoHostAvailableException
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.{UmsFieldType, UmsOpType}
import edp.wormhole.ums.UmsSysField._
import java.lang.{Double, Float, Long}

import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.common.util.DateUtils._

import scala.collection.mutable



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
    val tableKeys=sinkProcessConfig.tableKeyList
    val tableKeysIndex =tableKeys.map(key=>schemaMap(key)._1)
    // val connectionConfig = getDataStoreConnectionsMap(sinkNamespace)
    val cassandraSpecialConfig =json2caseClass[CassandraConfig](sinkProcessConfig.specialConfig.get)
    val user: String = if (connectionConfig.username.isDefined) connectionConfig.username.get else null
    val password: String = if (connectionConfig.password.isDefined) connectionConfig.password.get else null
    //    if (authentication.length != 0) {
    //      val json = JSON.parseObject(authentication)
    //      if (json.containsKey("user") && json.containsKey("password")) {
    //        user = json.getString("user")
    //        password = json.getString("password")
    //      }
    //    }
    val sortedAddressList = CassandraConnection.getSortedAddress(connectionConfig.connectionUrl)
    val keyspace = sinkNamespace.split("\\.")(2) //use sinkNamespace(2)
    val table = sinkNamespace.split("\\.")(3) // use sinkConfig.sinknamespace
    val prepareStatement: String = getPrepareStatement(keyspace, table, schemaString, valueStrByPlaceHolder)
    //INSERT INTO keyspace.table (a, b, c, d, e) VALUES(?, ?, ?, ?, ?) USING TIMESTAMP ?;
    val session = CassandraConnection.getSession(sortedAddressList, user, password)
    val tupleFilterList: Seq[Seq[String]] =SourceMutationType.sourceMutationType(cassandraSpecialConfig.`mutation_type.get`) match {
      case SourceMutationType.I_U_D =>
        val filterableStatement=checkTableBykey(keyspace,table,tableKeys,tableKeysIndex,tupleList)
        val filterRes=session.execute(filterableStatement).all()
        val dataMap = mutable.HashMap.empty[String,Long]
        import collection.JavaConversions._
        filterRes.foreach(row=>{
          val umsId=row.getLong("ums_id_")
          val mapKey=tableKeys.map(key=>row.getObject(key).toString).mkString("_")
          if (dataMap.contains(mapKey)) dataMap(mapKey)=if(dataMap(mapKey)>=umsId) dataMap(mapKey) else umsId
          else dataMap(mapKey)=umsId
        })
        if (dataMap.nonEmpty){
          tupleList.filter(tuple=>{
            val umsIdValue: Long = tuple(schemaMap(ID.toString)._1).toLong
            val tableKeyVal=tableKeys.map(key=>tuple(schemaMap(key)._1).toString).mkString("_")
            !dataMap.contains(tableKeyVal)||umsIdValue>dataMap(tableKeyVal)
          })}
        tupleList

      case SourceMutationType.INSERT_ONLY =>
        logInfo("cassandra insert_only:")
        tupleList
    }

    var prepareSchema: PreparedStatement = null
    try {
      prepareSchema = session.prepare(prepareStatement)
    } catch {
      case e0: NoHostAvailableException => logError("cassandra InShotFile prepare error:", e0)
    }
    val batch = new BatchStatement()
    for (tuple <- tupleFilterList) {
      //      val umsIdValue: Long = tuple(schemaMap(ID.toString)._1).toLong
      val umsTsLong=dt2long(tuple(schemaMap(TS.toString)._1).split("\\+")(0).replace("T"," "))
      val bound: BoundStatement = prepareSchema.bind()
      val umsOpValue: String = tuple(schemaMap(OP.toString)._1)
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
      bound.setLong(columnNumber, umsTsLong) //set TS
      if (batch.size() >= cassandraSpecialConfig.`cassandra.batchSize.get`) {
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
    strBuilder.append("ums_op_)")
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
  private def checkTableBykey(keyspace: String, table: String,tableKeys:List[String],valueList:List[Int],tupleList:Seq[Seq[String]])={
    val firstPk=tableKeys.head
    val firstPkValues=tupleList.map(tuple=>tuple(valueList.head)).mkString("(",",",")")
    val selectColumns=tableKeys.mkString(",")+",ums_id_"
    val tableKeySize=tableKeys.size
    if (tableKeySize==1){
      "SELECT "+selectColumns+" from "+keyspace+"."+table+" where "+firstPk+" in "+firstPkValues+";"
    }
    else{
      val otherPks=tableKeys.slice(1,tableKeySize).mkString("(",",",")")
      val otherPkValue=tupleList.map(tuple=>{
        var tupleRes="("
        for (i<- 1 to valueList.size){
          tupleRes=tupleRes+tuple(valueList(i))
        }
        tupleRes=tupleRes+")"
        tupleRes
      }).mkString("(",",",")")


      "SELECT "+selectColumns+" from "+keyspace+"."+table+" where "+firstPk+" in "+firstPkValues+" and "+otherPks+" in "+otherPkValue+";"
    }
  }

  private def bindWithDifferentTypes(bound: BoundStatement, columnName: String, fieldType: UmsFieldType, value: String): Unit =
    if (columnName=="ums_ts_")
      bound.setTimestamp(columnName, dt2date(value.trim.split("\\+")(0).replace("T"," ")))
      else{
    fieldType match {
    case UmsFieldType.STRING => bound.setString(columnName, value.trim)
    case UmsFieldType.INT => bound.setInt(columnName, Integer.valueOf(value.trim))
    case UmsFieldType.LONG => bound.setLong(columnName, Long.valueOf(value.trim))
    case UmsFieldType.FLOAT => bound.setFloat(columnName, Float.valueOf(value.trim))
    case UmsFieldType.DOUBLE => bound.setDouble(columnName, Double.valueOf(value.trim))
    case UmsFieldType.BOOLEAN => bound.setBool(columnName, java.lang.Boolean.valueOf(value.trim))
    case UmsFieldType.DATE => bound.setDate(columnName,  LocalDate.fromMillisSinceEpoch(dt2date(value.trim).getTime))
    case UmsFieldType.DATETIME => bound.setTimestamp(columnName, dt2date(value.trim.split("\\+")(0).replace("T"," ")))
    case UmsFieldType.DECIMAL => bound.setDecimal(columnName, new java.math.BigDecimal(value.trim).stripTrailingZeros())
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
  }}
}
