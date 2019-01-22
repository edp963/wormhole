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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.flinkx.ordering.OrderingImplicit._
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
import org.slf4j.LoggerFactory

import scala.language.existentials
import scala.math.Ordering

class PatternOutput(output: JSONObject, schemaMap: Map[String, (TypeInformation[_], Int)]) extends java.io.Serializable {

  private var aggRowchemaMap: Map[String, (TypeInformation[_], Int)] = _
  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private lazy val outputType = output.getString(TYPE.toString)
  private lazy val outputFieldList: Array[String] =
    if (output.containsKey(FIELDLIST.toString)) {
      output.getString(FIELDLIST.toString).split(",")
    } else {
      Array.empty[String]
    }

  def getOutputType: String = {
    outputType
  }

  def getOutputFiledList: Array[String] = {
    outputFieldList
  }


  def getOutput(patternStream: PatternStream[Row], patternGenerator: PatternGenerator, keyByFields: String): DataStream[(Boolean, Row)] = {
    val patternNameList: Seq[String] = patternGenerator.outputPatternNameList
    var flag = true
    val out = patternStream.select(patternSelectFun => {
      try {
        val eventSeq = for (name <- patternNameList)
          yield patternSelectFun(name)
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
    val originalFieldTypes = FlinkSchemaUtils.getOutPutFieldTypes(originalFieldNames, schemaMap)
    OutputType.outputType(outputType) match {
      case AGG =>
        val systemFieldArray = FlinkSchemaUtils.getSystemFields(schemaMap)
        val outputFieldNames = FlinkSchemaUtils.getOutputFieldNames(outputFieldList, keyByFields, systemFieldArray)
        val outputFieldTypes = getOutPutFieldTypes(outputFieldNames)
        (outputFieldNames, outputFieldTypes)
      case FILTERED_ROW | DETAIL => (originalFieldNames, originalFieldTypes)
    }
  }

  private def getOutPutFieldTypes(fieldNames: Array[String]): Array[TypeInformation[_]] = {
    fieldNames.map(fieldWithAggValue => {
      if (schemaMap.contains(fieldWithAggValue))
        schemaMap(fieldWithAggValue)._1
      else Types.INT
    })
  }


  /**
    *
    * agg build row with key_by fields
    * and ums_ts,ums_id,ums_op (if contains)
    *
    **/

  private def buildRow(input: Seq[Iterable[Row]], keyByFields: String) = {
    val outputFieldSize: Int = outputFieldList.length
    val keyByFieldsArray = if (keyByFields != null && keyByFields != "") keyByFields.split(";")
    else null
    val systemFieldArray = FlinkSchemaUtils.getSystemFields(schemaMap)
    val systemFieldsSize = systemFieldArray.length
    val row = if (keyByFieldsArray != null) {
      new Row(outputFieldSize + keyByFieldsArray.length + systemFieldsSize)
    } else new Row(outputFieldSize + systemFieldsSize)

    for (pos <- 0 until systemFieldsSize) {
      val systemFieldValue = UmsSysField.umsSysField(systemFieldArray(pos)) match {
        case UmsSysField.ID => input.flatten.map(row => row.getField(schemaMap(UmsSysField.ID.toString)._2).asInstanceOf[Long]).min
        case UmsSysField.TS => input.flatten.map(row => row.getField(schemaMap(UmsSysField.TS.toString)._2).asInstanceOf[Timestamp]).min(Ordering[Timestamp])
        case UmsSysField.OP => input.flatten.map(row => row.getField(schemaMap(UmsSysField.OP.toString)._2).asInstanceOf[String]).min
      }
      row.setField(pos, systemFieldValue)
    }
    if (keyByFieldsArray != null)
      for (keyIndex <- keyByFieldsArray.indices) {
        val rowFieldType = schemaMap(keyByFieldsArray(keyIndex))._1
        val rowFieldValue = input.head.head.getField(schemaMap(keyByFieldsArray(keyIndex))._2)
        val rowTrueValue = object2TrueValue(rowFieldType, rowFieldValue)
        row.setField(keyIndex + systemFieldsSize, rowTrueValue)
      }
    for (i <- 0 until outputFieldSize) {
      val fieldsWithType = outputFieldList(i).split(":")
      val fieldName = fieldsWithType.head
      val functionType = fieldsWithType.last
      val aggregation = new PatternAggregation(input, fieldName, schemaMap)
      val aggValue = aggregation.aggregationMatch(functionType)
      println(aggValue + " agg value")
      if (keyByFieldsArray != null)
        row.setField(i + keyByFieldsArray.length + systemFieldsSize, aggValue)
      else row.setField(i + systemFieldsSize, aggValue)
    }
    row
  }


  private def getNewSchemaMap = {
    schemaMap.zipWithIndex
  }

  private def filteredRow(input: Seq[Iterable[Row]]) = {
    val functionType = outputFieldList.head.split(":").last
    Functions.functions(functionType) match {
      case HEAD => input.head.head
      case LAST => input.last.last
      case MAX => maxRow(input)
      case MIN => minRow(input)
      case _ => throw new UnsupportedOperationException(s"Unsupported output type : $functionType")
    }
  }

  private def maxRow(input: Seq[Iterable[Row]]) = {
    val fieldNameOfMaxRow = outputFieldList.head.split(":").head
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


  private def minRow(input: Seq[Iterable[Row]]) = {
    val fieldNameOfMinRow = outputFieldList.head.split(":").head
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
