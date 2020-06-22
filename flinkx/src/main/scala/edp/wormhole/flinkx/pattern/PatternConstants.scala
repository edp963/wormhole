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

import edp.wormhole.flinkx.pattern.OutputType.Value

object JsonFieldName extends Enumeration {
  type JsonFieldName = Value

  val PATTERNSEQLIST = Value("pattern_seq_list")
  val PATTERNS = Value("pattern_seq")
  val KEYBYFILEDS = Value("key_by_fields")
  val CONDITIONS = Value("conditions")
  val PATTERNTYPE = Value("pattern_type")
  val QUANTIFIER = Value("quantifier")
  val STRATEGY = Value("strategy")
  val MAXINTERVALSECONDS = Value("max_interval_seconds")
  val OUTPUT = Value("output")
  val ACTION = Value("action")

  def jsonFieldName(s: String) = JsonFieldName.withName(s.toLowerCase())

}

object Condition extends Enumeration {
  type Condition = Value
  val OPERATOR = Value("operator")
  val LOGIC = Value("logic")
  val FIELDNAME = Value("field_name")
  val COMPARETYPE = Value("compare_type")
  val VALUE = Value("value")

  def condition(s: String) = Condition.withName(s.toLowerCase())
}


object LogicOperator extends Enumeration {
  type LogicOperator = Value

  val AND = Value("and")
  val OR = Value("or")
  val SINGLE = Value("single")

  def logicOperator(s: String) = LogicOperator.withName(s.toLowerCase())
}


object CompareType extends Enumeration {
  type CompareType = Value

  val GREATERTHAN = Value("gt")
  val LESSTHAN = Value("lt")
  val EQUALTO = Value("equiv")
  val GREATERTHANEQUALTO = Value("gteq")
  val LESSTHANEQUALTO = Value("lteq")
  val LIKE = Value("like")
  val ENDWITH = Value("endwith")
  val STARTWITH = Value("startwith")
  val NOTEQUALTO = Value("neq")


  def compareType(s: String) = CompareType.withName(s.toLowerCase)
}


object PatternType extends Enumeration {
  type PatternType = Value

  val BEGIN = Value("begin")
  val NEXT = Value("next")
  val FOLLOWEDBY = Value("followedby")
  val NOTNEXT = Value("notnext")
  val NOTFOLLOWEDBY = Value("notfollowedby")


  def patternType(s: String) = PatternType.withName(s.toLowerCase)
}

object Strategy extends Enumeration {
  type Strategy = Value

  val NO_SKIP = Value("no_skip")
  val SKIP_PAST_LAST_EVENT = Value("skip_past_last_event")
  val SKIP_TO_FIRST = Value("skip_to_first")
  val SKIP_TO_LAST = Value("skip_to_last")

  def strategy(s: String) = Strategy.withName(s.toLowerCase)
}


object Quantifier extends Enumeration {
  type Quantifier = Value

  val TYPE = Value("type")
  val OFTIMES = Value("value")
  val CONTIGUITY = Value("contiguity")
  val CONSECUTIVE = Value("consecutive")
  val ALLOWCOMBINATION = Value("allowcombination")

  val TIMES = Value("times")
  val TIMESORMORE = Value("timesormore")
  val ONEORMORE=Value("oneormore")
  val TIMESGREEDY=Value("timesgreedy")
  val TIMESOPTIONAL=Value("timesoptional")


  def quantifier(s: String) = Quantifier.withName(s.toLowerCase())
}


object Output extends Enumeration {
  type Output = Value

  val TYPE = Value("type")
  val FIELD_LIST = Value("field_list")
  val FUNCTION_TYPE = Value("function_type")
  val FIELD_NAME = Value("field_name")
  val ALIAS_NAME = Value("alias_name")

  def output(s: String) = Output.withName(s.toLowerCase())
}

object Functions extends Enumeration {
  type Functions = Value

  val MIN = Value("min")
  val MAX = Value("max")
  val HEAD = Value("head")
  val LAST = Value("last")
  val SUM = Value("sum")
  val AVG = Value("avg")
  val COUNT = Value("count")

  def functions(s: String) = Functions.withName(s.toLowerCase())
}

object OutputType extends Enumeration {
  type OutputType = Value

  val AGG = Value("agg")
  val FILTERED_ROW = Value("filteredrow")
  val DETAIL = Value("detail")
  val TIMEOUT=Value("timeout")

  def outputType(s: String) = OutputType.withName(s.toLowerCase())
}

object FieldType extends Enumeration{
  type FieldType = Value

  val SYSTEM_FIELD = Value("system_field")
  val KEY_BY_FIELD = Value("key_by_field")
  val AGG_FIELD = Value("agg_field")

  def fieldType(s: String) = FieldType.withName(s.toLowerCase())

}





