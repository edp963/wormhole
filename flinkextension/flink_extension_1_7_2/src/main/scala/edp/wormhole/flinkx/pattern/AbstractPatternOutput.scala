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
import edp.wormhole.flinkx.pattern.Output._
import edp.wormhole.ums.UmsSysField
import edp.wormhole.util.DateUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer
import scala.language.existentials
import scala.math.Ordering

abstract class AbstractPatternOutput(output: JSONObject, schemaMap: Map[String, (TypeInformation[_], Int)]) extends java.io.Serializable {

  protected lazy val outputFieldArray: JSONArray =
    if (output.containsKey(FIELD_LIST.toString)) {
      output.getJSONArray(FIELD_LIST.toString)
    } else {
      null.asInstanceOf[JSONArray]
    }

  /**
    * @param keyByFields key1,key2
    *                    outputFieldList: [{"function_type":"max","field_name":"col1","alias_name":"maxCol1"}]
    *                    systemFieldArray: ums_id_ ,ums_ts_, ums_op_
    * @return keyByFields+systemFields++originalFields if keyByFields are not empty,
    *         else systemFields++originalFields
    */

  protected def getOutputFieldList(keyByFields: String): ListBuffer[(String, TypeInformation[_], FieldType)] = {
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

  protected def maxRow(input: Iterable[Iterable[Row]]) = {
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

  protected def minRow(input: Iterable[Iterable[Row]]) = {
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
