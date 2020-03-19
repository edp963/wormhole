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



package edp.wormhole.util.swifts

import edp.wormhole.util.swifts.Operator.Operator


case class SwiftsSql(optType: String, // MAP, UNION, FILTER, JOIN... //can add key word "code" for wh3 to reflect
                     fields: Option[String], //fields get from database (join)
                     sql: String, // with constant to replace
                     timeout:Option[Int],
                     lookupNamespace: Option[String], // string after "from" before "=" in sql
                     sourceTableFields: Option[Array[String]], //where () in (@@@) @@@ --> sourceTableFields
                     lookupTableFields: Option[Array[String]], // where (@@@) in ... @@@ --> lookupTableFields
                     lookupTableFieldsAlias: Option[Array[String]],
                     lookupTableConstantCondition: Option[Array[SqlCondition]] = None//constant condition where field1 = value1 and field2 = value2；
                    ) {// final fileds name get from database ,e.g. select a as b, c from... --> get b,c
}

case class SqlCondition(column: String,
                        operator: Operator,
                        value: String,
                        isSourceField: Boolean = false) {
}


object Operator extends Enumeration {
  //kudu support operator,not support filed in (value1,value2)
  type Operator = Value

  val EQUAL = Value("=")
  val GREATER = Value(">")
  val GREATER_EQUAL = Value(">=")
  val LESS = Value("<")
  val LESS_EQUAL = Value("<=")
  val IN = Value("in")
  val IS_NULL = Value("is null") //空格处理
  val IS_NOT_NULL = Value("is not null") //空格处理

  def getValue(operator: Operator) = Operator.toString

  def getOperator(s: String) = Operator.withName(s.toLowerCase)
}



