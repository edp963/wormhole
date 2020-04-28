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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.flinkx.common.{ExceptionConfig, WormholeFlinkxConfig}
import edp.wormhole.flinkx.pattern.JsonFieldName._
import edp.wormhole.flinkx.pattern.Quantifier.{OFTIMES, TYPE}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

import scala.collection.Map
class PatternGenerator(patternSeq: JSONObject, schemaMap: Map[String, (TypeInformation[_], Int)], exceptionConfig: ExceptionConfig, config: WormholeFlinkxConfig) extends java.io.Serializable {
  var pattern: Pattern[Row, Row] = _
  private var patternSeqSize = 0
  private lazy val patterns = patternSeq.getJSONArray(PATTERNS.toString)

  def getPattern: Pattern[Row, Row] = {
    for (i <- 0 until patterns.size()) {
      val patternJsonObject = patterns.getJSONObject(i)
      if (patternJsonObject.containsKey(PATTERNTYPE.toString)) {
        val patternType = patterns.getJSONObject(i).getString(PATTERNTYPE.toString)
        patternTypeMatch(patternType)
      }
      if (patternJsonObject.containsKey(CONDITIONS.toString)) {
        val conditions: JSONObject = patterns.getJSONObject(i).getJSONObject(CONDITIONS.toString)
        packageConditions(conditions)
      }
      if (patternJsonObject.containsKey(QUANTIFIER.toString)) {
        val quantifier = patterns.getJSONObject(i).getJSONObject(QUANTIFIER.toString)
        packageQuantifier(quantifier)
      }
    }
    doWithIn()
  }

  private def patternTypeMatch(patternType: String): Unit = {
    val patternName = s"p$patternSeqSize"
    PatternType.patternType(patternType) match {
      case PatternType.BEGIN => pattern = Pattern.begin(patternName, doStrategy(patternName)).subtype(classOf[Row])
      case PatternType.NEXT => pattern = pattern.next(patternName).subtype(classOf[Row])
      case PatternType.FOLLOWEDBY => pattern = pattern.followedBy(patternName).subtype(classOf[Row])
      case PatternType.NOTNEXT => pattern = pattern.notNext(patternName).subtype(classOf[Row])
      case PatternType.NOTFOLLOWEDBY => pattern = pattern.notFollowedBy(patternName).subtype(classOf[Row])
    }
    patternSeqSize += 1
  }

  private def doStrategy(patternName: String = "") = {
    val strategy = patternSeq.getString(STRATEGY.toString)
    Strategy.strategy(strategy) match {
      case Strategy.NO_SKIP => AfterMatchSkipStrategy.noSkip()
      case Strategy.SKIP_PAST_LAST_EVENT => AfterMatchSkipStrategy.skipPastLastEvent()
      case Strategy.SKIP_TO_FIRST => AfterMatchSkipStrategy.skipToFirst(patternName)
      case Strategy.SKIP_TO_LAST => AfterMatchSkipStrategy.skipToLast(patternName)
    }
  }

  private def doWithIn() = {
    val maxInterval = patternSeq.getLong(MAXINTERVALSECONDS.toString)
    if (maxInterval != -1) {
      println(maxInterval + MAXINTERVALSECONDS.toString)
      pattern = pattern.within(Time.seconds(maxInterval))
    }
    pattern
  }

  private def packageConditions(conditions: JSONObject): Unit = {
    val filter = new PatternCondition(schemaMap, exceptionConfig, config)
    pattern = pattern.where(event => {
      filter.doFilter(conditions, event)
    })
  }

  private def packageQuantifier(quantifier: JSONObject): Unit = {
    if (!quantifier.isEmpty) {
      val quantifierType = quantifier.getString(TYPE.toString)
      val ofTimes = quantifier.getIntValue(OFTIMES.toString)
      println(ofTimes + "ofTimes")
      Quantifier.quantifier(quantifierType) match {
        case Quantifier.TIMES => pattern = pattern.times(ofTimes)
        case Quantifier.ONEORMORE => pattern = pattern.oneOrMore
        case Quantifier.TIMESORMORE => pattern = pattern.timesOrMore(ofTimes)
        case Quantifier.TIMESGREEDY => pattern = pattern.timesOrMore(ofTimes).greedy
        case Quantifier.TIMESOPTIONAL => pattern = pattern.timesOrMore(ofTimes).optional
        case _ =>
      }
      // Todo 页面配置contiguity,在此进行解析 allowCombinations()| consecutive()
      println(quantifierType + "@@" + ofTimes + "@@")
    }
  }
}
