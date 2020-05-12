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
import java.util.Date

import edp.wormhole.flinkx.ordering.OrderingImplicit._
import edp.wormhole.flinkx.pattern.Functions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import edp.wormhole.util.DateUtils

import scala.language.existentials
import scala.math.{BigDecimal, Ordering}
import scala.collection.Map

class PatternAggregation(input: Iterable[Iterable[Row]], fieldName: String, schemaMap: Map[String, (TypeInformation[_], Int)]) {
  private lazy val (fieldType, fieldIndex) = schemaMap(fieldName)

  def aggregationFunction(functionType: String) = {
    Functions.functions(functionType) match {
      case AVG => avg()
      case SUM => sum()
      case MAX => max()
      case MIN => min()
      case COUNT=>count()
      case _ => throw new UnsupportedOperationException(s"Unsupported output type : $functionType")
    }
  }

  def sum() = {
    fieldType match {
      case Types.INT => sumInt()
      case Types.LONG => sumLong()
      case Types.FLOAT => sumFloat()
      case Types.DOUBLE => sumDouble()
      case Types.DECIMAL => sumDecimal()
      case _ => throw new UnsupportedOperationException(s"Unsupported type to sum: $fieldType")
    }
  }

  def avg() = {
    val flattenInputSize = input.flatten.size
    fieldType match {
      case Types.INT => sumInt() / flattenInputSize
      case Types.LONG => sumLong() / flattenInputSize
      case Types.FLOAT => sumFloat() / flattenInputSize
      case Types.DOUBLE => sumDouble() / flattenInputSize
      case Types.DECIMAL => sumDecimal().divide(new java.math.BigDecimal(flattenInputSize))
      case _ => throw new UnsupportedOperationException(s"Unsupported type to sum: $fieldType")
    }
  }

  private def max() = {
    fieldType match {
      case Types.STRING => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[String]).max
      case Types.INT => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Int]).max
      case Types.LONG => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Long]).max
      case Types.FLOAT => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Float]).max
      case Types.DOUBLE => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Double]).max
      case Types.SQL_DATE => input.flatten.map(row => DateUtils.dt2sqlDate(row.getField(fieldIndex).asInstanceOf[String])).max(Ordering[Date])
      case Types.SQL_TIMESTAMP => input.flatten.map(row => DateUtils.dt2timestamp(row.getField(fieldIndex).asInstanceOf[String])).max(Ordering[Timestamp])
      case Types.DECIMAL => input.flatten.map(row => BigDecimal(row.getField(fieldIndex).asInstanceOf[String])).max(Ordering[BigDecimal])
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
    }
  }

  private def min() = {
    fieldType match {
      case Types.STRING => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[String]).min
      case Types.INT => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Int]).min
      case Types.LONG => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Long]).min
      case Types.FLOAT => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Float]).min
      case Types.DOUBLE => input.flatten.map(row => row.getField(fieldIndex).asInstanceOf[Double]).min
      case Types.SQL_DATE => input.flatten.map(row => DateUtils.dt2sqlDate(row.getField(fieldIndex).asInstanceOf[String])).min(Ordering[Date])
      case Types.SQL_TIMESTAMP => input.flatten.map(row => DateUtils.dt2timestamp(row.getField(fieldIndex).asInstanceOf[String])).min(Ordering[Timestamp])
      case Types.DECIMAL => input.flatten.map(row => BigDecimal(row.getField(fieldIndex).asInstanceOf[String])).min(Ordering[BigDecimal])
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
    }
  }


  private def count(): Int ={
    input.flatten.size
  }

  private def sumInt() = {
    var sum = 0
    for (elem <- input.flatten) {
      val value = elem.getField(fieldIndex).asInstanceOf[Int]
      if (value equals null.asInstanceOf[Int]) sum
      else sum += value
    }
    sum
  }

  private def sumLong() = {
    var sum = 0L
    for (elem <- input.flatten) {
      val value = elem.getField(fieldIndex).asInstanceOf[Long]
      if (value equals null.asInstanceOf[Long]) sum
      else sum += value
    }
    sum
  }

  private def sumFloat() = {
    var sum = 0.0F
    for (elem <- input.flatten) {
      val value = elem.getField(fieldIndex).asInstanceOf[Float]
      if (value equals null.asInstanceOf[Float]) sum
      else sum += value
    }
    sum
  }

  private def sumDouble() = {
    var sum = 0.0D
    for (elem <- input.flatten) {
      val value = elem.getField(fieldIndex).asInstanceOf[Double]
      if (value equals null.asInstanceOf[Double]) sum += 0.0D
      else sum += value
    }
    sum
  }

  private def sumDecimal() = {
    var sum = new java.math.BigDecimal(0.0)
    for (elem <- input.flatten) {
      val value: java.math.BigDecimal = new java.math.BigDecimal(new java.math.BigDecimal(elem.asInstanceOf[String].trim).stripTrailingZeros().toPlainString)
      if (value == null) sum
      else sum = sum.add(value)
    }
    sum
  }
}
