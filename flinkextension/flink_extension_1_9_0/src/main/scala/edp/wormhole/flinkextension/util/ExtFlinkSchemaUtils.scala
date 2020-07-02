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

package edp.wormhole.flinkextension.util

import java.sql.{Date, Timestamp}

import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.DateUtils
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

object ExtFlinkSchemaUtils extends java.io.Serializable {

  def tableFieldTypeArray(tableSchema: TableSchema, preSchemaMap: Map[String, (TypeInformation[_], Int)]): Array[TypeInformation[_]] = {
    tableSchema.getFieldNames.map(fieldName => {
      val fieldType = preSchemaMap(fieldName)._1
      if (fieldType == TimeIndicatorTypeInfo.PROCTIME_INDICATOR || fieldType == TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
        SqlTimeTypeInfo.TIMESTAMP
      else fieldType
    })
  }

  def umsType2FlinkType(umsFieldType: UmsFieldType): TypeInformation[_] = {
    umsFieldType match {
      case STRING => Types.STRING
      case INT => Types.INT
      case LONG => Types.LONG
      case FLOAT => Types.FLOAT
      case DOUBLE => Types.DOUBLE
      case BOOLEAN => Types.BOOLEAN
      case DATE => Types.SQL_DATE
      case DATETIME => Types.SQL_TIMESTAMP
      case DECIMAL => Types.BIG_DEC
    }
  }

  def FlinkType2UmsType(dataType: TypeInformation[_]): UmsFieldType = {
    dataType match {
      case Types.STRING => STRING
      case Types.INT => INT
      case Types.LONG => LONG
      case Types.FLOAT => FLOAT
      case Types.DOUBLE => DOUBLE
      case Types.BOOLEAN => BOOLEAN
      case Types.SQL_DATE => DATE
      case Types.SQL_TIMESTAMP => DATETIME
      case Types.BIG_DEC => DECIMAL
      //case _ => INT
    }
  }

  def s2FlinkType(fieldType: String): TypeInformation[_] = {
    fieldType match {
      case "datetime" => Types.SQL_TIMESTAMP
      case "date" => Types.SQL_DATE
      case "decimal" => Types.BIG_DEC
      case "int" => Types.INT
      case "long" => Types.LONG
      case "float" => Types.FLOAT
      case "double" => Types.DOUBLE
      case "string" => Types.STRING
      case "binary" => BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO

      case unknown =>
        throw new Exception("unknown type:" + unknown)
    }
  }

  def object2TrueValue(flinkType: TypeInformation[_], value: Any): Any = if (value == null) null
  else flinkType match {
    case Types.STRING =>  value.asInstanceOf[String].trim
    case Types.INT => value.asInstanceOf[Int]
    case Types.LONG => value match {
      case _: Int => value.asInstanceOf[Int].toLong
      case _ => value.asInstanceOf[Long]
    }
    case Types.FLOAT => value.asInstanceOf[Float]
    case Types.DOUBLE => value match {
      case _: Float => value.asInstanceOf[Float].toDouble
      case _ => value.asInstanceOf[Double]
    }
    case Types.BOOLEAN => value.asInstanceOf[Boolean]
    case Types.SQL_DATE => value match {
      case _:Timestamp => DateUtils.dt2sqlDate(value.asInstanceOf[Timestamp])
      case _=>DateUtils.dt2sqlDate(value.asInstanceOf[Date])
    }
    case Types.SQL_TIMESTAMP => value.asInstanceOf[Timestamp]
    case Types.BIG_DEC => new java.math.BigDecimal(value.asInstanceOf[java.math.BigDecimal].stripTrailingZeros().toPlainString)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $flinkType")
  }

  def s2TrueValue(flinkType: TypeInformation[_], value: String): Any = if (value == null) null
  else flinkType match {
    case Types.STRING => value.trim
    case Types.INT => value.trim.toInt
    case Types.LONG => value.trim.toLong
    case Types.FLOAT => value.trim.toFloat
    case Types.DOUBLE => value.trim.toDouble
    case Types.BOOLEAN => value.trim.toBoolean
    case Types.SQL_DATE => DateUtils.dt2sqlDate(value.trim)
    case Types.SQL_TIMESTAMP => DateUtils.dt2timestamp(value.trim)
    case Types.BIG_DEC => new java.math.BigDecimal(value.trim).stripTrailingZeros()
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $flinkType")
  }

}
