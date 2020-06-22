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

import java.sql.{Date, Timestamp}

import com.alibaba.fastjson.{JSONArray, JSONObject}
import edp.wormhole.flinkx.ordering.OrderingImplicit._
import edp.wormhole.flinkx.pattern.FieldType.{AGG_FIELD, FieldType, KEY_BY_FIELD, SYSTEM_FIELD}
import edp.wormhole.flinkx.pattern.Functions.{HEAD, LAST, MAX, MIN}
import edp.wormhole.flinkx.pattern.Output._
import edp.wormhole.flinkx.pattern.OutputType._
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.flinkx.util.FlinkSchemaUtils.object2TrueValue
import edp.wormhole.ums.UmsSysField
import edp.wormhole.util.DateUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.scala.PatternStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}
import scala.language.existentials
import scala.math.Ordering

class PatternOutput(output: JSONObject, schemaMap: Map[String, (TypeInformation[_], Int)]) extends java.io.Serializable {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val outputType = output.getString(TYPE.toString)
  private lazy val outputFieldArray: JSONArray =
    if (output.containsKey(FIELD_LIST.toString)) {
      output.getJSONArray(FIELD_LIST.toString)
    } else {
      null.asInstanceOf[JSONArray]
    }

  def getOutputType: String = {
    outputType
  }

  def getOutput(patternStream: PatternStream[Row], patternGenerator: PatternGenerator, keyByFields: String): DataStream[Row] = {
    val timeoutOutputTag = OutputTag[Iterable[Row]]("timeout-side-output")

    OutputType.outputType(outputType) match {
      case AGG =>
        patternStream.select(pattern => {
          buildRow(pattern.values, keyByFields)
        })
      case FILTERED_ROW => patternStream.select(pattern => {
        filteredRow(pattern.values)
      })
      case DETAIL => patternStream.flatSelect((pattern, out: Collector[Iterable[Row]]) => {
        out.collect(pattern.values.flatten)
      }).flatMap(r => r)
      case TIMEOUT =>
        val result = patternStream.select(timeoutOutputTag) {
          (pattern: Map[String, Iterable[Row]], timestamp: Long) => pattern.values.flatten
        } {
          pattern: Map[String, Iterable[Row]] => pattern.values.flatten
        }
        result.getSideOutput(timeoutOutputTag).flatMap(r => r)
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
      case FILTERED_ROW | DETAIL | TIMEOUT => (originalFieldNames, originalFieldTypes)
    }
  }


  /**
   *
   * agg build row with key_by fields
   * and ums_ts,ums_id,ums_op (if contains)
   *
   **/

  private def buildRow(input: Iterable[Iterable[Row]], keyByFields: String) = {
    try{
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
    row}catch {
      case e:Exception =>
        logger.error("output build row exception " ,e)
        null;
    }
  }


  /**
   * @param keyByFields key1,key2
   *                    outputFieldList: [{"function_type":"max","field_name":"col1","alias_name":"maxCol1"}]
   *                    systemFieldArray: ums_id_ ,ums_ts_, ums_op_
   * @return keyByFields+systemFields++originalFields if keyByFields are not empty,
   *         else systemFields++originalFields
   */

  private def getOutputFieldList(keyByFields: String): ListBuffer[(String, TypeInformation[_], FieldType)] = {
    val outPutFieldListBuffer = ListBuffer.empty[(String, TypeInformation[_], FieldType)]
    if (schemaMap.contains(UmsSysField.ID.toString)) outPutFieldListBuffer.append((UmsSysField.ID.toString, schemaMap(UmsSysField.ID.toString)._1, SYSTEM_FIELD))
    if (schemaMap.contains(UmsSysField.TS.toString)) outPutFieldListBuffer.append((UmsSysField.TS.toString, schemaMap(UmsSysField.TS.toString)._1, SYSTEM_FIELD))
    if (schemaMap.contains(UmsSysField.OP.toString)) outPutFieldListBuffer.append((UmsSysField.OP.toString, schemaMap(UmsSysField.OP.toString)._1, SYSTEM_FIELD))

    for (i <- 0 until outputFieldArray.size()) {
      val outputFieldJsonObj = outputFieldArray.getJSONObject(i)
      val originalFieldName = outputFieldJsonObj.getString(FIELD_NAME.toString)
      val aliasName =
        if (outputFieldJsonObj.getString(ALIAS_NAME.toString) == "") originalFieldName
        else outputFieldJsonObj.getString(ALIAS_NAME.toString)
      val fieldType = if (schemaMap.contains(originalFieldName))
        schemaMap(originalFieldName)._1
      else Types.INT
      outPutFieldListBuffer.append((aliasName, fieldType, AGG_FIELD))
    }
    if (keyByFields != null && keyByFields.nonEmpty) {
      val keyByFiledArray = keyByFields.split(",")
      keyByFiledArray.foreach(field => outPutFieldListBuffer.append((field, schemaMap(field)._1, KEY_BY_FIELD)))
      outPutFieldListBuffer
    }
    else
      outPutFieldListBuffer
  }


  /**
   * map[renameField,(originalField,functionType)]
   */

  private def getAggFieldMap: Map[String, (String, String)] = {
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


  private def filteredRow(input: Iterable[Iterable[Row]]): Row = {
    try {
      val functionType = outputFieldArray.getJSONObject(0).getString(FUNCTION_TYPE.toString)
      Functions.functions(functionType) match {
        case HEAD => input.head.head
        case LAST => input.last.last
        case MAX => maxRow(input)
        case MIN => minRow(input)
        case _ => throw new UnsupportedOperationException(s"Unsupported output type : $functionType")
      }
    } catch {
      case e: Exception => logger.error("filteredRow exception ", e)
        null
    }
  }

  private def maxRow(input: Iterable[Iterable[Row]]) = {
    val fieldNameOfMaxRow = outputFieldArray.getJSONObject(0).getString(FIELD_NAME.toString)
    val (fieldType, fieldIndex) = schemaMap(fieldNameOfMaxRow)
    fieldType match {
      case Types.STRING => input.flatten.maxBy(row => row.getField(fieldIndex).asInstanceOf[String])
      case Types.INT => input.flatten.maxBy(row => row.getField(fieldIndex).asInstanceOf[Int])
      case Types.LONG => input.flatten.maxBy(row => row.getField(fieldIndex).asInstanceOf[Long])
      case Types.FLOAT => input.flatten.maxBy(row => row.getField(fieldIndex).asInstanceOf[Float])
      case Types.DOUBLE => input.flatten.maxBy(row => row.getField(fieldIndex).asInstanceOf[Double])
      case Types.SQL_DATE => input.flatten.maxBy(row => DateUtils.dt2sqlDate(row.getField(fieldIndex).asInstanceOf[String]))(Ordering[Date])
      case Types.SQL_TIMESTAMP => input.flatten.maxBy(row => DateUtils.dt2timestamp(row.getField(fieldIndex).asInstanceOf[String]))(Ordering[Timestamp])
      case Types.DECIMAL => input.flatten.maxBy(row => new java.math.BigDecimal(row.getField(fieldIndex).asInstanceOf[String]).stripTrailingZeros())
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
    }
  }


  private def minRow(input: Iterable[Iterable[Row]]) = {
    val fieldNameOfMinRow = outputFieldArray.getJSONObject(0).getString(FIELD_NAME.toString)
    val (fieldType, fieldIndex) = schemaMap(fieldNameOfMinRow)
    fieldType match {
      case Types.STRING => input.flatten.minBy(row => row.getField(fieldIndex).asInstanceOf[String])
      case Types.INT => input.flatten.minBy(row => row.getField(fieldIndex).asInstanceOf[Int])
      case Types.LONG => input.flatten.minBy(row => row.getField(fieldIndex).asInstanceOf[Long])
      case Types.FLOAT => input.flatten.minBy(row => row.getField(fieldIndex).asInstanceOf[Float])
      case Types.DOUBLE => input.flatten.minBy(row => row.getField(fieldIndex).asInstanceOf[Double])
      case Types.SQL_DATE => input.flatten.minBy(row => DateUtils.dt2sqlDate(row.getField(fieldIndex).asInstanceOf[String]))(Ordering[Date])
      case Types.SQL_TIMESTAMP => input.flatten.minBy(row => DateUtils.dt2timestamp(row.getField(fieldIndex).asInstanceOf[String]))(Ordering[Timestamp])
      case Types.DECIMAL => input.flatten.minBy(row => new java.math.BigDecimal(row.getField(fieldIndex).asInstanceOf[String]).stripTrailingZeros())
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
    }
  }

}
