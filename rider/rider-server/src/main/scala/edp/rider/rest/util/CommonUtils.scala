/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
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


package edp.rider.rest.util

import com.alibaba.fastjson.JSON
import edp.rider.common.RiderLogger
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.DtFormat

import scala.concurrent.duration._

object CommonUtils extends RiderLogger {

  def currentSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_SEC)

  def currentMillSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

  def currentMicroSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MICROSEC)

  def minTimeOut = 180.seconds

  def maxTimeOut = 600.seconds

  def streamSubmitTimeout = 120.seconds

  def isJson(str: String): Boolean = {
    try {
      if (str == "" || str == null)
        true
      else {
        JSON.parseObject(str)
        true
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"$str is not json type", ex)
        false
    }
  }

  def isKeyEqualValue(str: String): Boolean = {
    if (str == "" || str == null)
      return true
    val seq = str.split(",")
    val keyEqualValuePattern = "(.)+=(.)+".r.pattern
    seq.foreach(
      str =>
        if (!keyEqualValuePattern.matcher(str).matches())
          return false
    )
    true
  }
}
