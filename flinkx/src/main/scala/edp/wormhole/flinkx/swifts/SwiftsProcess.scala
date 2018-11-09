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
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcess, ExceptionProcessMethod, WormholeFlinkxConfig}
import edp.wormhole.flinkx.pattern.JsonFieldName.{KEYBYFILEDS, OUTPUT}
import edp.wormhole.flinkx.pattern.Output.{FIELDLIST, TYPE}
import edp.wormhole.flinkx.pattern.{OutputType, PatternGenerator, PatternOutput}
import edp.wormhole.flinkx.sink.SinkProcess.logger
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType, SwiftsConstants}
import edp.wormhole.ums.UmsSysField
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{StreamQueryConfig, Table, Types}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}


class SwiftsProcess(dataStream: DataStream[Row],
                    exceptionConfig: ExceptionConfig,
                    tableEnv: StreamTableEnvironment,
                    swiftsSql: Option[Array[SwiftsSql]],
                    swiftsSpecialConfig: String,
                    timeCharacteristic: String,
                    config: WormholeFlinkxConfig) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val specialConfigObj = JSON.parseObject(swiftsSpecialConfig)
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
    var table: Table = transformedStream.toTable(tableEnv, buildExpression(): _*)
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
      val value = FlinkSchemaUtils.getSchemaMapFromTable(table.getSchema, projectClause, FlinkSchemaUtils.udfSchemaMap.toMap)
      preSchemaMap = value
      FlinkSchemaUtils.setSwiftsSchema(key, value)
    } catch {
      case e: Throwable =>
        logger.error("in doFlinkSql table query", e)
        e.printStackTrace()
        throw e
    }
    covertTable2Stream(table)
  }

  private def buildExpression(): List[Expression] = {
    val originalSchema = preSchemaMap.toList.sortBy(_._2._2).map(_._1)
    if (timeCharacteristic == SwiftsConstants.PROCESSING_TIME)
      ExpressionParser.parseExpressionList(originalSchema.mkString(",") + s", ${SwiftsConstants.PROCESSING_TIME}.proctime")
    else {
      val newSchema = originalSchema.updated(preSchemaMap(UmsSysField.TS.toString)._2, UmsSysField.TS.toString + ".rowtime")
      ExpressionParser.parseExpressionList(newSchema.mkString(","))
    }
  }

  private def covertTable2Stream(table: Table): DataStream[Row] = {
    val columnNames = table.getSchema.getColumnNames
    val columnTypes = replaceTimeIndicatorType(table.getSchema.getTypes)
    if (null != specialConfigObj && specialConfigObj.containsKey(SwiftsConstants.RESERVE_MESSAGE_FLAG) && specialConfigObj.getBooleanValue(SwiftsConstants.RESERVE_MESSAGE_FLAG)) {
      val columnNamesWithMessageFlag: Array[String] = columnNames ++ Array(SwiftsConstants.MESSAGE_FLAG)
      val columnTypesWithMessageFlag: Array[TypeInformation[_]] = columnTypes ++ Array(Types.BOOLEAN)
      val resultDataStream = table.toRetractStream[Row](getQueryConfig).map(tuple => {
        val rowWithMessageFlag = new Row(columnNames.length + 1)
        for (i <- 0 to columnNames.length)
          rowWithMessageFlag.setField(i, tuple._2.getField(i))
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

  private def replaceTimeIndicatorType(columnTypes: Array[TypeInformation[_]]): Array[TypeInformation[_]] = {
    columnTypes.map(fieldType =>
      if (fieldType == TimeIndicatorTypeInfo.PROCTIME_INDICATOR || fieldType == TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
        SqlTimeTypeInfo.TIMESTAMP
      else fieldType)
  }

  private def getQueryConfig: StreamQueryConfig = {
    val minIdleStateRetentionTime = if (null != specialConfigObj && specialConfigObj.containsKey(SwiftsConstants.MIN_IDLE_STATE_RETENTION_TIME)) specialConfigObj.getLongValue(SwiftsConstants.MIN_IDLE_STATE_RETENTION_TIME) else 12L
    val maxIdleStateRetentionTime = if (null != specialConfigObj && specialConfigObj.containsKey(SwiftsConstants.MAX_IDLE_STATE_RETENTION_TIME)) specialConfigObj.getLongValue(SwiftsConstants.MAX_IDLE_STATE_RETENTION_TIME) else 24L
    val queryConfig = tableEnv.queryConfig
    queryConfig.withIdleStateRetentionTime(Time.hours(minIdleStateRetentionTime), Time.hours(maxIdleStateRetentionTime))
  }

  private def doCEP(transformedStream: DataStream[Row], sql: String, index: Int): DataStream[Row] = {
    val patternSeq = JSON.parseObject(sql)
    val patternGenerator = new PatternGenerator(patternSeq, preSchemaMap)
    val pattern = patternGenerator.getPattern

    val keyByFields = patternSeq.getString(KEYBYFILEDS.toString).trim
    val patternStream = if (keyByFields != null && keyByFields.nonEmpty) {
      val keyArray = keyByFields.split(",").map(key => preSchemaMap(key)._2)
      CEP.pattern(transformedStream.keyBy(keyArray: _*), pattern)
    } else CEP.pattern(transformedStream, pattern)

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

  private def doLookup(transformedStream: DataStream[Row], element: SwiftsSql, index: Int): DataStream[Row] = {
    val lookupSchemaMap = LookupHelper.getLookupSchemaMap(preSchemaMap, element)
    val fieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(lookupSchemaMap)
    val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, lookupSchemaMap)
    val resultDataStream = transformedStream.map(new LookupMapper(element, preSchemaMap, LookupHelper.getDbOutPutSchemaMap(element), ConnectionMemoryStorage.getDataStoreConnectionsMap)).flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
    val key = s"swifts$index"
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key))
      FlinkSchemaUtils.swiftsProcessSchemaMap += key -> lookupSchemaMap
    preSchemaMap = FlinkSchemaUtils.swiftsProcessSchemaMap(key)
    resultDataStream.print()
    logger.info(resultDataStream.dataType.toString + "in doLookup")

    //handle exception
    val exceptionStream: DataStream[String] = resultDataStream.getSideOutput(lookupTag)
    exceptionStream.map(stream => {
      logger.info("--------------------lookup exception stream:" + stream)
      ExceptionProcess.doExceptionProcess(exceptionConfig.exceptionProcessMethod, stream, config)
    })
    //return
    resultDataStream
  }
}
