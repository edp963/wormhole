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

package edp.wormhole.flinkx.pattern

import java.sql.Timestamp

import com.alibaba.fastjson.{JSONArray, JSONObject}
import edp.wormhole.flinkx.ordering.OrderingImplicit._
import edp.wormhole.flinkx.pattern.FieldType.{AGG_FIELD, KEY_BY_FIELD, SYSTEM_FIELD}
import edp.wormhole.flinkx.pattern.Functions.{HEAD, LAST, MAX, MIN}
import edp.wormhole.flinkx.pattern.Output._
import edp.wormhole.flinkx.pattern.OutputType._
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.flinkx.util.FlinkSchemaUtils.object2TrueValue
import edp.wormhole.ums.UmsSysField
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.PatternStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.existentials
import scala.math.Ordering

class PatternOutput(output: JSONObject, schemaMap: Map[String, (TypeInformation[_], Int)]) extends AbstractPatternOutput(output, schemaMap) {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val outputType = output.getString(TYPE.toString)

  def getOutputType: String = {
    outputType
  }

  def getOutput(patternStream: PatternStream[Row], patternGenerator: PatternGenerator, keyByFields: String): DataStream[(Boolean, Row)] = {
    var flag = true
    val out = patternStream.select(patternSelectFun => {
      try {
        val eventSeq = patternSelectFun.values
        OutputType.outputType(outputType) match {
          case AGG => buildRow(eventSeq, keyByFields)
          case FILTERED_ROW => filteredRow(eventSeq)
          case DETAIL => eventSeq.flatten
        }
      } catch {
        case e: Throwable =>
          flag = false
          logger.error("exception in getOutput ", e)
          null.asInstanceOf[Row]
      }
    })

    OutputType.outputType(outputType) match {
      case AGG =>
        out.asInstanceOf[DataStream[Row]].map(r => (flag, r))
      case FILTERED_ROW =>
        out.asInstanceOf[DataStream[Row]].map(r => (flag, r))
      case DETAIL =>
        out.asInstanceOf[DataStream[Seq[Row]]].flatMap(o => o).map(r => (flag, r))
    }
  }


  def getPatternOutputRowType(keyByFields: String): (Array[String], Array[TypeInformation[_]]) = {
    val originalFieldNames = FlinkSchemaUtils.getFieldNamesFromSchema(schemaMap)
    val originalFieldTypes = FlinkSchemaUtils.getFieldTypes(originalFieldNames, schemaMap)
    OutputType.outputType(outputType) match {
      case AGG =>
        val outputFieldList = getOutputFieldList(keyByFields)
        val fieldNames = outputFieldList.map(_._1).toArray
        val fieldTypes = outputFieldList.map(_._2).toArray
        (fieldNames, fieldTypes)
      case FILTERED_ROW | DETAIL => (originalFieldNames, originalFieldTypes)
    }
  }


  /**
    *
    * agg build row with key_by fields
    * and ums_ts,ums_id,ums_op (if contains)
    *
    **/

  private def buildRow(input: Iterable[Iterable[Row]], keyByFields: String) = {
    val outputFieldList = getOutputFieldList(keyByFields)
    val aggFieldMap = getAggFieldMap
    val row: Row = new Row(outputFieldList.size)
    var pos = 0
    outputFieldList.foreach(field => {
      val fieldType = field._3
      fieldType match {
        case SYSTEM_FIELD =>
          val systemFieldValue = UmsSysField.umsSysField(field._1) match {
            case UmsSysField.ID => input.flatten.map(row => row.getField(schemaMap(UmsSysField.ID.toString)._2).asInstanceOf[Long]).max
            case UmsSysField.TS => input.flatten.map(row => row.getField(schemaMap(UmsSysField.TS.toString)._2).asInstanceOf[Timestamp]).max(Ordering[Timestamp])
            case UmsSysField.OP => input.flatten.map(row => row.getField(schemaMap(UmsSysField.OP.toString)._2).asInstanceOf[String]).max
          }
          row.setField(pos, systemFieldValue)
          pos += 1
        case KEY_BY_FIELD =>
          val rowFieldType = field._2
          val rowFieldValue = input.head.head.getField(schemaMap(field._1)._2)
          val rowTrueValue = object2TrueValue(rowFieldType, rowFieldValue)
          row.setField(pos, rowTrueValue)
          pos += 1
        case AGG_FIELD =>
          val originalField = aggFieldMap(field._1)._1
          val functionType = aggFieldMap(field._1)._2
          val aggValue = new PatternAggregation(input, originalField, schemaMap).aggregationFunction(functionType)
          logger.debug(aggValue + s"$originalField agg value")
          row.setField(pos, aggValue)
          pos += 1
      }
    })
    row
  }

  /**
    * map[renameField,(originalField,functionType)]
    */

  private def getAggFieldMap: mutable.Map[String, (String, String)] = {
    val aggFieldMap = mutable.HashMap.empty[String, (String, String)]
    for (i <- 0 until outputFieldArray.size()) {
      val outputFieldJsonObj = outputFieldArray.getJSONObject(i)
      val originalFieldName = outputFieldJsonObj.getString(FIELD_NAME.toString)
      val aliasName =
        if (outputFieldJsonObj.getString(ALIAS_NAME.toString) == "") originalFieldName
        else outputFieldJsonObj.getString(ALIAS_NAME.toString)
      val functionType = outputFieldJsonObj.getString(FUNCTION_TYPE.toString)
      if (aggFieldMap.contains(aliasName)) throw new Exception(s"重复字段名称: $aliasName")
      else
        aggFieldMap += (aliasName -> (originalFieldName, functionType))
    }
    aggFieldMap
  }


  private def filteredRow(input: Iterable[Iterable[Row]]) = {
    val functionType = outputFieldArray.getJSONObject(0).getString(FUNCTION_TYPE.toString)
    Functions.functions(functionType) match {
      case HEAD => input.head.head
      case LAST => input.last.last
      case MAX => maxRow(input)
      case MIN => minRow(input)
      case _ => throw new UnsupportedOperationException(s"Unsupported output type : $functionType")
    }
  }

}
