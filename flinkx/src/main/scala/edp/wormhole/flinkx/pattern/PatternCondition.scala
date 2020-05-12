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

import java.util.UUID

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.feedback.ErrorPattern
import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcess, FlinkxUtils, WormholeFlinkxConfig}
import edp.wormhole.flinkx.pattern.Condition._
import edp.wormhole.flinkx.util.FlinkSchemaUtils.{object2TrueValue, s2TrueValue}
import edp.wormhole.ums.UmsProtocolType
import edp.wormhole.util.DateUtils.{dt2sqlDate, dt2timestamp}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

import scala.collection.Map
class PatternCondition(schemaMap: Map[String, (TypeInformation[_], Int)],exceptionConfig: ExceptionConfig,config: WormholeFlinkxConfig) extends java.io.Serializable {

  lazy val leftConditionIndex = 0
  lazy val rightConditionIndex = 1

  def doFilter(conditions: JSONObject, event: Row): Boolean = {
    try{
    val op = conditions.getString(OPERATOR.toString)
    LogicOperator.logicOperator(op) match {
      case LogicOperator.SINGLE =>
        val logic = conditions.getJSONObject(LOGIC.toString)
        val fieldName = logic.getString(FIELDNAME.toString).toLowerCase
        val compareType = logic.getString(COMPARETYPE.toString)
        val value = logic.getString(VALUE.toString)
        eventFilter(fieldName, value, compareType, event)
      case LogicOperator.AND =>
        val logicArray = conditions.getJSONArray(LOGIC.toString)
        println("find and operator:):):)")
        doFilter(logicArray.getJSONObject(leftConditionIndex), event) && doFilter(logicArray.getJSONObject(rightConditionIndex), event)
      case LogicOperator.OR =>
        val logicArray = conditions.getJSONArray(LOGIC.toString)
        println("find or operator{:{:{:")
        doFilter(logicArray.getJSONObject(leftConditionIndex), event) || doFilter(logicArray.getJSONObject(rightConditionIndex), event)
    }}catch{
      case e:Throwable =>
        e.printStackTrace()
        val errorMsg = FlinkxUtils.getFlowErrorMessage(null,
          exceptionConfig.sourceNamespace,
          exceptionConfig.sinkNamespace,
          1,
          e,
          UUID.randomUUID().toString,
          UmsProtocolType.DATA_INCREMENT_DATA.toString,
          exceptionConfig.flowId,
          exceptionConfig.streamId,
          ErrorPattern.FlowError)
        new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config, exceptionConfig).doExceptionProcess(errorMsg)
        false
    }
  }


  private def eventFilter(fieldName: String, value: String, compareType: String, event: Row): Boolean = {
    val rowFieldType = schemaMap(fieldName)._1
    val rowFieldValue = event.getField(schemaMap(fieldName)._2)
    val compareValue = if (null == value) null else value.trim
    val rowTrueValue = object2TrueValue(rowFieldType, rowFieldValue)
    val compareTrueValue = s2TrueValue(rowFieldType, value)

    if (rowFieldValue == null) false
    else CompareType.compareType(compareType) match {
      case CompareType.GREATERTHAN => rowFieldType match {
        case Types.STRING => rowFieldValue.asInstanceOf[String].trim > compareValue
        case Types.INT => rowFieldValue.asInstanceOf[Int] > compareValue.toInt
        case Types.LONG => rowFieldValue.asInstanceOf[Long] > compareValue.toLong
        case Types.FLOAT => rowFieldValue.asInstanceOf[Float] > compareValue.toFloat
        case Types.DOUBLE => rowFieldValue.asInstanceOf[Double] > compareValue.toDouble
        case Types.DECIMAL => new java.math.BigDecimal(value).stripTrailingZeros().compareTo(new java.math.BigDecimal(compareValue).stripTrailingZeros()) > 0
        case Types.SQL_DATE => dt2sqlDate(value).compareTo(dt2sqlDate(compareValue)) > 0
        case Types.SQL_TIMESTAMP => dt2timestamp(value).compareTo(dt2timestamp(compareValue)) > 0
      }
      case CompareType.LESSTHAN => rowFieldType match {
        case Types.STRING => rowFieldValue.asInstanceOf[String].trim < compareValue
        case Types.INT => rowFieldValue.asInstanceOf[Int] < compareValue.toInt
        case Types.LONG => rowFieldValue.asInstanceOf[Long] < compareValue.toLong
        case Types.FLOAT => rowFieldValue.asInstanceOf[Float] < compareValue.toFloat
        case Types.DOUBLE => rowFieldValue.asInstanceOf[Double] < compareValue.toDouble
        case Types.DECIMAL => new java.math.BigDecimal(value).stripTrailingZeros().compareTo(new java.math.BigDecimal(compareValue).stripTrailingZeros()) < 0
        case Types.SQL_DATE => dt2sqlDate(value).compareTo(dt2sqlDate(compareValue)) < 0
        case Types.SQL_TIMESTAMP => dt2timestamp(value).compareTo(dt2timestamp(compareValue)) < 0
      }
      case CompareType.GREATERTHANEQUALTO => rowFieldType match {
        case Types.STRING => rowFieldValue.asInstanceOf[String].trim >= compareValue
        case Types.INT => rowFieldValue.asInstanceOf[Int] >= compareValue.toInt
        case Types.LONG => rowFieldValue.asInstanceOf[Long] >= compareValue.toLong
        case Types.FLOAT => rowFieldValue.asInstanceOf[Float] >= compareValue.toFloat
        case Types.DOUBLE => rowFieldValue.asInstanceOf[Double] >= compareValue.toDouble
        case Types.DECIMAL => new java.math.BigDecimal(value).stripTrailingZeros().compareTo(new java.math.BigDecimal(compareValue).stripTrailingZeros()) >= 0
        case Types.SQL_DATE => dt2sqlDate(value).compareTo(dt2sqlDate(compareValue)) >= 0
        case Types.SQL_TIMESTAMP => dt2timestamp(value).compareTo(dt2timestamp(compareValue)) >= 0
      }
      case CompareType.LESSTHANEQUALTO => rowFieldType match {
        case Types.STRING => rowFieldValue.asInstanceOf[String].trim <= compareValue
        case Types.INT => rowFieldValue.asInstanceOf[Int] <= compareValue.toInt
        case Types.LONG => rowFieldValue.asInstanceOf[Long] <= compareValue.toLong
        case Types.FLOAT => rowFieldValue.asInstanceOf[Float] <= compareValue.toFloat
        case Types.DOUBLE => rowFieldValue.asInstanceOf[Double] <= compareValue.toDouble
        case Types.DECIMAL => new java.math.BigDecimal(value).stripTrailingZeros().compareTo(new java.math.BigDecimal(compareValue).stripTrailingZeros()) <= 0
        case Types.SQL_DATE => dt2sqlDate(value).compareTo(dt2sqlDate(compareValue)) <= 0
        case Types.SQL_TIMESTAMP => dt2timestamp(value).compareTo(dt2timestamp(compareValue)) <= 0
      }
      case CompareType.EQUALTO => rowTrueValue == compareTrueValue
      case CompareType.NOTEQUALTO => rowTrueValue != compareTrueValue
      case CompareType.LIKE => rowFieldValue.asInstanceOf[String].contains(compareValue)
      case CompareType.STARTWITH => rowFieldValue.asInstanceOf[String].startsWith(compareValue.asInstanceOf[String])
      case CompareType.ENDWITH => rowFieldValue.asInstanceOf[String].endsWith(compareValue.asInstanceOf[String])
    }
  }
}
