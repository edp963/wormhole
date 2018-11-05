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
import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcessMethod, WormholeFlinkxConfig}
import edp.wormhole.flinkx.pattern.JsonFieldName.{KEYBYFILEDS, OUTPUT}
import edp.wormhole.flinkx.pattern.Output.{FIELDLIST, TYPE}
import edp.wormhole.flinkx.pattern.{OutputType, PatternGenerator, PatternOutput}
import edp.wormhole.flinkx.sink.SinkProcess.logger
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}


object SwiftsProcess extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var preSchemaMap: Map[String, (TypeInformation[_], Int)] = FlinkSchemaUtils.immutableSourceSchemaMap
  private var udfSchemaMap: Map[String, TypeInformation[_]] = FlinkSchemaUtils.udfSchemaMap.toMap

  private val lookupTag = OutputTag[String]("lookupException")

  def process(dataStream: DataStream[Row], exceptionConfig: ExceptionConfig, tableEnv: StreamTableEnvironment, swiftsSql: Option[Array[SwiftsSql]], config: WormholeFlinkxConfig): (DataStream[Row], Map[String, (TypeInformation[_], Int)]) = {
    var transformedStream = dataStream
    if (swiftsSql.nonEmpty) {
      val swiftsSqlGet = swiftsSql.get
      for (index <- swiftsSqlGet.indices) {
        val element = swiftsSqlGet(index)
        SqlOptType.withName(element.optType) match {
          case SqlOptType.FLINK_SQL => transformedStream = doFlinkSql(transformedStream, exceptionConfig.sourceNamespace, tableEnv, element.sql, index)
          case SqlOptType.CEP => transformedStream = doCEP(transformedStream, element.sql, index)
          case SqlOptType.JOIN | SqlOptType.LEFT_JOIN => transformedStream = doLookup(transformedStream, element, index, exceptionConfig, config)
        }
      }
    }
    (transformedStream, preSchemaMap)
  }


  private def doFlinkSql(dataStream: DataStream[Row], sourceNamespace: String, tableEnv: StreamTableEnvironment, sql: String, index: Int) = {
    var table = tableEnv.fromDataStream(dataStream)
    table.printSchema()
    val projectClause = sql.substring(0, sql.lastIndexOf("from")).trim
    val namespaceTable = sourceNamespace.split("\\.").apply(3)
    val fromClause = sql.substring(sql.lastIndexOf("from")).trim
    val whereClause = fromClause.substring(fromClause.indexOf(namespaceTable) + namespaceTable.length).trim
    //println(projectClause + "-----projectClause" + namespaceTable + "-----namespaceTable" + whereClause + "-----whereClause")
    val newSql =s"""$projectClause FROM $table $whereClause"""
    println(newSql)
    try {
      table = tableEnv.sqlQuery(newSql)
      table.printSchema()
      val key = s"swifts$index"
      val value = FlinkSchemaUtils.getSchemaMapFromTable(table.getSchema, projectClause, udfSchemaMap)
      preSchemaMap = value
      FlinkSchemaUtils.setSwiftsSchema(key, value)
    } catch {
      case e: Throwable => logger.error("in doFlinkSql table query", e)
        println(e)
    }
   val resultDataStream = tableEnv.toAppendStream[Row](table).map(o => o)(Types.ROW(FlinkSchemaUtils.tableFieldNameArray(table.getSchema), FlinkSchemaUtils.tableFieldTypeArray(table.getSchema, preSchemaMap)))
    /*val resultDataStream: DataStream[Row] = tableEnv.toAppendStream[Row](table).map(o =>{
      var index = 0
      table.getSchema.getColumnNames.foreach(x=>{
        o.setField(index, FlinkSchemaUtils.object2TrueValue(preSchemaMap(x)._1, o.getField(index)))
        print(index + ":" + o.getField(index))
        index += 1
      })
      o
    })(Types.ROW(FlinkSchemaUtils.tableFieldNameArray(table.getSchema), FlinkSchemaUtils.tableFieldTypeArray(table.getSchema)))*/
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


  private def doLookup(dataStream: DataStream[Row], element: SwiftsSql, index: Int, exceptionConfig: ExceptionConfig, config: WormholeFlinkxConfig) = {
    val lookupSchemaMap = LookupHelper.getLookupSchemaMap(preSchemaMap, element)
    val fieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(lookupSchemaMap)
    val fieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(fieldNames, lookupSchemaMap)
    //val resultDataStream = dataStream.map(new LookupMapper(element, preSchemaMap, LookupHelper.getDbOutPutSchemaMap(element), ConnectionMemoryStorage.getDataStoreConnectionsMap)).flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
    val resultDataStream = dataStream.process(new LookupProcessElement(element, preSchemaMap, LookupHelper.getDbOutPutSchemaMap(element), ConnectionMemoryStorage.getDataStoreConnectionsMap, exceptionConfig, lookupTag)).flatMap(o => o)(Types.ROW(fieldNames, fieldTypes))
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
      exceptionConfig.exceptionProcess match {
        case ExceptionProcessMethod.INTERRUPT =>
          throw new Throwable("process error")
        case ExceptionProcessMethod.FEEDBACK =>
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, stream, None, config.kafka_output.brokers)
        case _ =>
          logger.info("exception process method is" + exceptionConfig.exceptionProcess)
      }})
    //exceptionStream.print()

    //return
    resultDataStream
  }


}
