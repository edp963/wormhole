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

import java.time.ZonedDateTime

import com.alibaba.fastjson.JSON
import edp.rider.common.RiderLogger
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.DtFormat

import scala.concurrent.duration._

object CommonUtils extends RiderLogger {

  def getTimeZoneId = ZonedDateTime.now().getOffset.getId

  def currentSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_SEC)

  def currentNodSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_NOD_SEC)

  def currentMillSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

  def currentMicroSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MICROSEC)

  def currentNodMicroSec = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_NOD_MILLISEC)

  def minTimeOut = 120.seconds

  def maxTimeOut = 5.minute

  def streamSubmitTimeout = 120.seconds

  val keyEqualValuePattern = "([a-zA-Z]+[a-zA-z0-9\\_\\-\\.]*=[a-zA-Z0-9]+[a-zA-z0-9\\_\\-\\.]*(&[a-zA-Z]+[a-zA-z0-9\\_\\-\\.]*=[a-zA-Z0-9]+[a-zA-z0-9\\_\\-\\.]*)*)".r.pattern

  val keyEqualValuePatternSimple = "([a-zA-Z]+[\\S ]*=[a-zA-Z0-9]+[\\S ]*(&[a-zA-Z]+[\\S ]*=[a-zA-Z0-9]+[\\S ]*)*)".r.pattern

  val streamConfigPattern = "(.+=.+(,.+.+)*)".r.pattern

  val namePattern = "[^\\.]*".r.pattern

  def formatResponseTimestamp(time: Option[String]): Option[String] = {
    if (time.getOrElse("") == "") Some("")
    else time
  }

  def formatRequestTimestamp(time: Option[String]): Option[String] = {
    if (time.getOrElse("") == "")
      null
    else time
  }

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
    keyEqualValuePatternSimple.matcher(str.split(",").mkString("&")).matches()
  }

  def isStreamConfig(str: String): Boolean = {
    if (str == "" || str == null)
      return true
    streamConfigPattern.matcher(str).matches()
  }

}
