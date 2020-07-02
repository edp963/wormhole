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
import edp.wormhole.flinkextension.pattern.Condition._
import edp.wormhole.flinkextension.pattern.{AbstractPatternCondition, LogicOperator}
import edp.wormhole.ums.UmsProtocolType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row

class PatternCondition(schemaMap: Map[String, (TypeInformation[_], Int)], exceptionConfig: ExceptionConfig, config: WormholeFlinkxConfig) extends AbstractPatternCondition(schemaMap) {

  lazy val leftConditionIndex = 0
  lazy val rightConditionIndex = 1

  def doFilter(conditions: JSONObject, event: Row): Boolean = {
    try{
    val op = conditions.getString(OPERATOR.toString)
    LogicOperator.logicOperator(op) match {
      case LogicOperator.SINGLE =>
        val logic = conditions.getJSONObject(LOGIC.toString)
        val fieldName = logic.getString(FIELDNAME.toString)
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

}
