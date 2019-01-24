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
import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcess, WormholeFlinkxConfig}
import edp.wormhole.flinkx.pattern.JsonFieldName.{KEYBYFILEDS, OUTPUT}
import edp.wormhole.flinkx.pattern.{OutputType, PatternGenerator, PatternOutput, PatternOutputFilter}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType}
import edp.wormhole.ums.{UmsProtocolUtils, UmsSysField}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{StreamQueryConfig, Table, Types}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.types.Row
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}


class SwiftsProcess(dataStream: DataStream[Row],
                    exceptionConfig: ExceptionConfig,
                    tableEnv: StreamTableEnvironment,
                    swiftsSql: Option[Array[SwiftsSql]],
                    specialConfigObj: JSONObject,
                    timeCharacteristic: String,
                    config: WormholeFlinkxConfig) extends Serializable {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var preSchemaMap: Map[String, (TypeInformation[_], Int)] = FlinkSchemaUtils.immutableSourceSchemaMap

  private val lookupTag = OutputTag[String]("lookupException")

  def process(): (DataStream[Row], Map[String, (TypeInformation[_], Int)]) = {
    var transformedStream = dataStream
    if (swiftsSql.nonEmpty) {
      val swiftsSqlGet = swiftsSql.get
      for (index <- swiftsSqlGet.indices) {
        val element = swiftsSqlGet(index)
        SqlOptType.withName(element.optType) match {
          case SqlOptType.FLINK_SQL => transformedStream = doFlinkSql(transformedStream, element.sql, index)
          case SqlOptType.CEP => transformedStream = doCEP(transformedStream, element.sql, index)
          case SqlOptType.JOIN | SqlOptType.LEFT_JOIN => transformedStream = doLookup(transformedStream, element, index)
        }
      }
    }
    (transformedStream, preSchemaMap)
  }

  private def doFlinkSql(transformedStream: DataStream[Row], sql: String, index: Int): DataStream[Row] = {
    var table: Table = getKeyByStream(transformedStream).toTable(tableEnv, buildExpression(): _*)
    table.printSchema()

    val projectClause = sql.substring(0, sql.toLowerCase.lastIndexOf("from")).trim
    val namespaceTable = exceptionConfig.sourceNamespace.split("\\.").apply(3)
    val fromClause = sql.substring(sql.toLowerCase.lastIndexOf("from")).trim
    val whereClause = fromClause.substring(fromClause.indexOf(namespaceTable) + namespaceTable.length).trim
    val newSql =s"""$projectClause FROM $table $whereClause"""
    println(newSql)

    try {
      table = tableEnv.sqlQuery(newSql)
      table.printSchema()
      val key = s"swifts$index"
      val value = FlinkSchemaUtils.getSchemaMapFromTable(table.getSchema, projectClause, FlinkSchemaUtils.udfSchemaMap.toMap, specialConfigObj)
      preSchemaMap = value
      FlinkSchemaUtils.setSwiftsSchema(key, value)
    } catch {
      case e: Throwable =>
        logger.error("in doFlinkSql table query", e)
        println("in doFlinkSql table query" + e)
        val feedbackInfo = UmsProtocolUtils.feedbackFlowFlinkxError(exceptionConfig.sourceNamespace, exceptionConfig.streamId, exceptionConfig.flowId, exceptionConfig.sinkNamespace, new DateTime(), "", e.getMessage)
        new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config).doExceptionProcess(feedbackInfo)
    }
    covertTable2Stream(table)
  }

  private def getKeyByStream(transformedStream: DataStream[Row]): DataStream[Row] = {
    if (null != specialConfigObj && specialConfigObj.containsKey(FlinkxSwiftsConstants.KEY_BY_FIELDS)) {
      val streamKeyByFieldsIndex = specialConfigObj.getString(FlinkxSwiftsConstants.KEY_BY_FIELDS).split(",").map(preSchemaMap(_)._2)
     println("in key by stream")
      transformedStream.keyBy(streamKeyByFieldsIndex: _*)
    }
    else transformedStream
  }

  private def buildExpression(): List[Expression] = {
    val originalSchema = preSchemaMap.toList.sortBy(_._2._2).map(_._1)
    if (timeCharacteristic == FlinkxTimeCharacteristicConstants.PROCESSING_TIME)
      ExpressionParser.parseExpressionList(originalSchema.mkString(",") + s", ${FlinkxTimeCharacteristicConstants.PROCESSING_TIME}.proctime")
    else {
      val newSchema = originalSchema.updated(preSchemaMap(UmsSysField.TS.toString)._2, UmsSysField.TS.toString + ".rowtime")
      ExpressionParser.parseExpressionList(newSchema.mkString(","))
    }
  }

  private def covertTable2Stream(table: Table): DataStream[Row] = {
    val columnNames = table.getSchema.getColumnNames
    val columnTypes = FlinkSchemaUtils.tableFieldTypeArray(table.getSchema, preSchemaMap)
    if (null != specialConfigObj && specialConfigObj.containsKey(FlinkxSwiftsConstants.PRESERVE_MESSAGE_FLAG) && specialConfigObj.getBooleanValue(FlinkxSwiftsConstants.PRESERVE_MESSAGE_FLAG)) {
      val columnNamesWithMessageFlag: Array[String] = columnNames ++ Array(FlinkxSwiftsConstants.MESSAGE_FLAG)
      val columnTypesWithMessageFlag: Array[TypeInformation[_]] = columnTypes ++ Array(Types.BOOLEAN)
      val resultDataStream = table.toRetractStream[Row](getQueryConfig).map(tuple => {
        val rowWithMessageFlag = new Row(columnNames.length + 1)
        for (i <- columnNames.indices) {
          rowWithMessageFlag.setField(i, tuple._2.getField(i))
        }
        rowWithMessageFlag.setField(columnNames.length, tuple._1)
        rowWithMessageFlag
      })(Types.ROW(columnNamesWithMessageFlag, columnTypesWithMessageFlag))
      resultDataStream.print()
      logger.info(resultDataStream.dataType.toString + "in  doFlinkSql")
      resultDataStream
    } else {
      table.toRetractStream[Row](getQueryConfig).filter(_._1).map(_._2)(Types.ROW(columnNames, columnTypes))
    }
  }

  private def getQueryConfig: StreamQueryConfig = {
    val minIdleStateRetentionTime = if (null != specialConfigObj && specialConfigObj.containsKey(FlinkxSwiftsConstants.MIN_IDLE_STATE_RETENTION_TIME)) specialConfigObj.getLongValue(FlinkxSwiftsConstants.MIN_IDLE_STATE_RETENTION_TIME) else 12L
    val maxIdleStateRetentionTime = if (null != specialConfigObj && specialConfigObj.containsKey(FlinkxSwiftsConstants.MAX_IDLE_STATE_RETENTION_TIME)) specialConfigObj.getLongValue(FlinkxSwiftsConstants.MAX_IDLE_STATE_RETENTION_TIME) else 24L
    val queryConfig = tableEnv.queryConfig
    queryConfig.withIdleStateRetentionTime(Time.hours(minIdleStateRetentionTime), Time.hours(maxIdleStateRetentionTime))
  }

  private def doCEP(transformedStream: DataStream[Row], sql: String, index: Int): DataStream[Row] = {
    var resultDataStream: DataStream[Row] = null
    try {
      val patternSeq = JSON.parseObject(sql)
      val patternGenerator = new PatternGenerator(patternSeq, preSchemaMap, exceptionConfig, config)
      val pattern = patternGenerator.getPattern

      val keyByFields = patternSeq.getString(KEYBYFILEDS.toString).trim
      val patternStream = if (keyByFields != null && keyByFields.nonEmpty) {
        val keyArray = keyByFields.split(",").map(key => preSchemaMap(key)._2)
        CEP.pattern(transformedStream.keyBy(keyArray: _*), pattern)
      } else CEP.pattern(transformedStream, pattern)

      val patternOutput = new PatternOutput(patternSeq.getJSONObject(OUTPUT.toString), preSchemaMap)
      val patternOutputStreamType: (Array[String], Array[TypeInformation[_]]) = patternOutput.getPatternOutputRowType(keyByFields)
      setSwiftsSchemaWithCEP(patternOutput, index, keyByFields)
      val patternOutputStream: DataStream[(Boolean, Row)] = patternOutput.getOutput(patternStream, patternGenerator, keyByFields)
      resultDataStream = filterException(patternOutputStream, patternOutputStreamType)

      println(resultDataStream.dataType)
      logger.info(resultDataStream.dataType.toString + "in  doCep")
    } catch {
      case e: Throwable =>
        logger.error("doCEP error in swifts process", e)
        val feedbackInfo = UmsProtocolUtils.feedbackFlowFlinkxError(exceptionConfig.sourceNamespace, exceptionConfig.streamId, exceptionConfig.flowId, exceptionConfig.sinkNamespace, new DateTime(), "", e.getMessage)
        new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config).doExceptionProcess(feedbackInfo)
    }
    resultDataStream
  }


  private def setSwiftsSchemaWithCEP(patternOutput: PatternOutput, index: Int, keyByFields: String): Unit = {
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key)) {
      val newSchema = if (OutputType.outputType(patternOutput.getOutputType) == OutputType.AGG) {
        val (fieldNames, fieldTypes) = patternOutput.getPatternOutputRowType(keyByFields)
        FlinkSchemaUtils.getSchemaMapFromArray(fieldNames, fieldTypes)
      } else preSchemaMap
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> newSchema
    }
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
  }


  def filterException(patternOutputStream: DataStream[(Boolean, Row)], patternOutputStreamType: (Array[String], Array[TypeInformation[_]])): DataStream[Row] = {
    //Todo 移动类型定义到PatternOutput class中
    val filteredDataStream = patternOutputStream.filter(new PatternOutputFilter(exceptionConfig, config, preSchemaMap))
    filteredDataStream.map(_._2)(Types.ROW(patternOutputStreamType._1, patternOutputStreamType._2))
  }


  private def doLookup(transformedStream: DataStream[Row], element: SwiftsSql, index: Int): DataStream[Row] = {
    val lookupSchemaMap = LookupHelper.getLookupSchemaMap(preSchemaMap, element)
    val fieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(lookupSchemaMap)
    val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, lookupSchemaMap)
    val resultDataStreamSeq = transformedStream.process(new LookupProcessElement(element, preSchemaMap, LookupHelper.getDbOutPutSchemaMap(element), ConnectionMemoryStorage.getDataStoreConnectionsMap, exceptionConfig, lookupTag))
    val resultDataStream = resultDataStreamSeq.flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key))
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> lookupSchemaMap
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
    //resultDataStream.print()
    //logger.info(resultDataStream.dataType.toString + "in doLookup")
    val exceptionStream: DataStream[String] = resultDataStreamSeq.getSideOutput(lookupTag)
    exceptionStream.map(new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config))

    resultDataStream
  }
}
