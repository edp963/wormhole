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

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.flinkx.common.ConfMemoryStorage
import edp.wormhole.flinkx.pattern.JsonFieldName.{KEYBYFILEDS, OUTPUT}
import edp.wormhole.flinkx.pattern.Output.{FIELDLIST, TYPE}
import edp.wormhole.flinkx.pattern.{OutputType, PatternGenerator, PatternOutput}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.sinks.kudu.KuduConnection
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType}
import edp.wormhole.ums.UmsDataSystem
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.kudu.client.KuduTable
import org.apache.log4j.Logger

import scala.collection.mutable


object SwiftsProcess extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  private var preSchemaMap: Map[String, (TypeInformation[_], Int)] = FlinkSchemaUtils.sourceSchemaMap.toMap

  def process(dataStream: DataStream[Row], sourceNamespace: String, tableEnv: StreamTableEnvironment, swiftsSql: Option[Array[SwiftsSql]]): (DataStream[Row], Map[String, (TypeInformation[_], Int)]) = {
    var transformedStream = dataStream
    if (swiftsSql.nonEmpty) {
      val swiftsSqlGet = swiftsSql.get
      for (index <- swiftsSqlGet.indices) {
        val element = swiftsSqlGet(index)
        SqlOptType.withName(element.optType) match {
          case SqlOptType.FLINK_SQL => transformedStream = doFlinkSql(transformedStream, sourceNamespace, tableEnv, element.sql, index)
          case SqlOptType.CEP => transformedStream = doCEP(transformedStream, element.sql, index)
          case SqlOptType.JOIN | SqlOptType.LEFT_JOIN =>
            val lookupNamespace = if (element.lookupNamespace.isDefined) element.lookupNamespace.get else null
            val lookupNameSpaceArr: Array[String] = lookupNamespace.split("\\.")
            assert(lookupNameSpaceArr.length == 3, "lookup namespace is invalid in pattern list sql")
            UmsDataSystem.dataSystem(lookupNameSpaceArr(0).toLowerCase()) match {
              case UmsDataSystem.KUDU => transformedStream = doLookupKudu(transformedStream, element, index)
              case _ => transformedStream = doLookup(transformedStream, element, index)
            }
        }
      }
    }
    (transformedStream, preSchemaMap)
  }


  private def doFlinkSql(dataStream: DataStream[Row], sourceNamespace: String, tableEnv: StreamTableEnvironment, sql: String, index: Int) = {
    var table = tableEnv.fromDataStream(dataStream)
    table.printSchema()
    val projectClause = sql.substring(0, sql.indexOf("from")).trim
    val namespaceTable = sourceNamespace.split("\\.").apply(3)
    val whereClause = sql.substring(sql.indexOf(namespaceTable) + namespaceTable.length).trim
    val newSql =s"""$projectClause FROM $table $whereClause"""
    println(newSql + " " + table)
    table = tableEnv.sqlQuery(newSql)
    table.printSchema()
    val key = s"swifts$index"
    val value = FlinkSchemaUtils.getSchemaMapFromTable(table.getSchema)
    preSchemaMap = value
    FlinkSchemaUtils.setSwiftsSchema(key, value)
    val resultDataStream = tableEnv.toAppendStream[Row](table).map(o => o)(Types.ROW(FlinkSchemaUtils.tableFieldNameArray(table.getSchema), FlinkSchemaUtils.tableFieldTypeArray(table.getSchema)))
    resultDataStream.print()
    logger.info(resultDataStream.dataType.toString + "in  doFlinkSql")
    resultDataStream
  }


  private def doCEP(dataStream: DataStream[Row], sql: String, index: Int) = {
    val patternSeq = JSON.parseObject(sql)
    val patternGenerator = new PatternGenerator(patternSeq, preSchemaMap)
    val pattern = patternGenerator.getPattern

    val keyByFields = patternSeq.getString(KEYBYFILEDS.toString).trim
    val patternStream = if (keyByFields != null && keyByFields.nonEmpty) {
      val keyArray = keyByFields.split(",").map(key => preSchemaMap(key)._2)
      CEP.pattern(dataStream.keyBy(keyArray: _*), pattern)
    } else CEP.pattern(dataStream, pattern)

    val resultDataStream = new PatternOutput(patternSeq.getJSONObject(OUTPUT.toString), preSchemaMap).getOutput(patternStream, patternGenerator, keyByFields)
    resultDataStream.print()
    println(resultDataStream.dataType)
    logger.info(resultDataStream.dataType.toString + "in  doCep")
    setSwiftsSchemaWithCEP(patternSeq, index, keyByFields)
    resultDataStream
  }


  private def setSwiftsSchemaWithCEP(patternSeq: JSONObject, index: Int, keyByFields: String): Unit = {
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key)) {
      val output = patternSeq.getJSONObject(OUTPUT.toString)
      val outputFieldList: Array[String] =
        if (output.containsKey(FIELDLIST.toString)) {
          output.getString(FIELDLIST.toString).split(",")
        } else {
          Array.empty[String]
        }
      val outputType: String = output.getString(TYPE.toString)
      val newSchema = if (OutputType.outputType(outputType) == OutputType.AGG) {
        val fieldNames = FlinkSchemaUtils.getOutputFieldNames(outputFieldList, keyByFields)
        val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, preSchemaMap)
        FlinkSchemaUtils.getSchemaMapFromArray(fieldNames, fieldTypes)
      } else preSchemaMap
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> newSchema
    }
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
  }

  private def doLookupKudu(dataStream: DataStream[Row], element: SwiftsSql, index: Int) = {
    val lookupNamespace: String = if (element.lookupNamespace.isDefined) element.lookupNamespace.get else null
    val dataStoreConnectionsMap = ConnectionMemoryStorage.getDataStoreConnectionsMap
    //get Kudu tableSchema
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)
    val database = element.lookupNamespace.get.split("\\.")(2)
    val fromIndex = element.sql.indexOf(" from ")
    val afterFromSql = element.sql.substring(fromIndex + 6).trim
    val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
    val tableName = KuduConnection.getTableName(tmpTableName, database)
    logger.info("tableName:" + tableName)
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    val table: KuduTable = client.openTable(tableName)
    val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
    val tableSchema: mutable.Map[String, String] = KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)
    KuduConnection.closeClient(client)

    //get lookupSchemaMap
    val lookupSchemaMap = LookupKuduHelper.getLookupSchemaMap(preSchemaMap, element, tableSchema)
    val fieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(lookupSchemaMap)
    val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, lookupSchemaMap)
    val resultDataStream = dataStream.map(new LookupKuduMapper(element, preSchemaMap, tableSchemaInKudu, dataStoreConnectionsMap)).flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key))
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> lookupSchemaMap
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
    resultDataStream.print()
    logger.info(resultDataStream.dataType.toString + "in  doLookup")
    resultDataStream
  }

  private def doLookup(dataStream: DataStream[Row], element: SwiftsSql, index: Int) = {
    val lookupSchemaMap = LookupHelper.getLookupSchemaMap(preSchemaMap, element)
    val fieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(lookupSchemaMap)
    val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, lookupSchemaMap)
    val resultDataStream = dataStream.map(new LookupMapper(element, preSchemaMap,LookupHelper.getDbOutPutSchemaMap(element), ConnectionMemoryStorage.getDataStoreConnectionsMap)).flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key))
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> lookupSchemaMap
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
    resultDataStream.print()
    logger.info(resultDataStream.dataType.toString + "in  doLookup")
    resultDataStream
  }


}
