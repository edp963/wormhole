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


package edp.wormhole.util

import java.sql.{Timestamp, Date => SqlDate}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import edp.wormhole.util.config.WormholeDefault._
import DtFormat.DtFormat
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.matching.Regex


object DtFormat extends Enumeration {
  type DtFormat = Value

  val DATE_SLASH = Value("yyyy/MM/dd")
  val DATE_DASH = Value("yyyy-MM-dd")
  val DATE_NOD = Value("yyyyMMdd")

  val TS_SLASH_MICROSEC = Value("yyyy/MM/dd HH:mm:ss.SSS000")
  val TS_SLASH_MILLISEC = Value("yyyy/MM/dd HH:mm:ss.SSS")
  val TS_SLASH_SEC = Value("yyyy/MM/dd HH:mm:ss")

  val TS_DASH_MICROSEC = Value("yyyy-MM-dd HH:mm:ss.SSS000")
  val TS_DASH_MILLISEC = Value("yyyy-MM-dd HH:mm:ss.SSS")
  val TS_DASH_SEC = Value("yyyy-MM-dd HH:mm:ss")

  val TS_NOD_MICROSEC = Value("yyyyMMddHHmmssSSS000")
  val TS_NOD_MILLISEC = Value("yyyyMMddHHmmssSSS")
  val TS_NOD_SEC = Value("yyyyMMddHHmmss")

  val TIME_MICROSEC = Value("HH:mm:ss.SSS000")
  val TIME_MILLISEC = Value("HH:mm:ss.SSS")
  val TIME_SEC = Value("HH:mm:ss")

  val TIME_NOD_MICROSEC = Value("HHmmssSSS000")
  val TIME_NOD_MILLISEC = Value("HHmmssSSS")
  val TIME_NOD_SEC = Value("HHmmss")

  val DEFAULT_DF = TS_DASH_MILLISEC

  def formatter(dtFormat: DtFormat): SimpleDateFormat = {
    val formatter = new SimpleDateFormat(dtFormat.toString)
    formatter.setTimeZone(defaultTimeZone)
    formatter
  }
}

object DateUtils extends DateUtils

trait DateUtils {
  lazy val unixEpochDate: Date = new Date(0)
  lazy val unixEpochTimestamp: Timestamp = new Timestamp(unixEpochDate.getTime)
  lazy val unixEpochDateTime: DateTime = new DateTime(unixEpochDate)

  def currentDateTime: DateTime = new DateTime()

  def currentyyyyMMddHHmmss: String = yyyyMMddHHmmss(currentDateTime)

  def currentyyyyMMddHHmmssmls: String = yyyyMMddHHmmssmls(currentDateTime)

  def getDateFormat(format: String) = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(defaultTimeZone)
    formatter
  }

  /**
    * convert time String to Date
    *
    * @param timeString time as String type
    * @return time as Date
    */
  private def dt2dateInternal(timeString: String): Date = {
    if (timeString == null) return null

    val DATE_DASH = "yyyy-MM-dd"
    val DATE_H_DASH = "yyyy-MM-dd HH"
    val DATE_HM_DASH = "yyyy-MM-dd HH:mm"
    val DATE_HMS_DASH = "yyyy-MM-dd HH:mm:ss"
    val DATE_HMS_Z_DASH = "yyyy-MM-dd HH:mm:ss'Z'"
    val DATE_HMS_M_DASH = "yyyy-MM-dd HH:mm:ss.SSS"
    val DATE_HMS_M_Z_DASH = "yyyy-MM-dd HH:mm:ss.SSS'Z'"
    val DATE_T_HMS_M_DASH = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    val DATE_T_HMS_M_Z_DASH = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    val DATE_REGEX_DASH = """\d{4}-\d{2}-\d{2}"""
    val SEPARATOR_REGEX = """(\s*)"""
    val SEPARATOR_REGEX_T = """(T?)"""
    val DATE_FORMAT_VALIDATORS = List(
      DATE_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + """\s*$"""),
      DATE_H_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d\s*$"""),
      DATE_HM_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d:\d\d\s*$"""),
      DATE_HMS_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d:\d\d:\d\d\s*$"""),
      DATE_HMS_Z_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d:\d\d:\d\d[Z]\s*$"""),
      DATE_HMS_M_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d:\d\d:\d\d\.\d{1,3}\s*$"""),
      DATE_HMS_M_Z_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX + """\d\d:\d\d:\d\d\.\d{1,3}[Z]\s*$"""),
      DATE_T_HMS_M_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX_T + """\d\d:\d\d:\d\d\.\d{1,3}\s*$"""),
      DATE_T_HMS_M_Z_DASH -> new Regex( """^\s*""" + DATE_REGEX_DASH + SEPARATOR_REGEX_T + """\d\d:\d\d:\d\d\.\d{1,3}[Z]\s*$"""))

    def prepare(timeString: String): String = {
      val s =
        if (timeString.contains("-")) timeString
        else if (timeString.contains("/")) timeString.replaceAll("[/]", "-")
        else timeString.substring(0, 4) + "-" + timeString.substring(4, 6) + "-" + timeString.substring(6)
      val dotStart = s.indexOf('.') + 1
      val hasDot = dotStart > 0
      val msLength = dotStart + 3
      val overMsLength = s.length - msLength
      val lessMsLength = msLength - s.length
      val ts = if(s.endsWith("Z")) s
      else if (hasDot && overMsLength >= 0) s.dropRight(overMsLength)
      else if (hasDot && lessMsLength >= 0) (0 until lessMsLength).foldLeft(s)((soFar, i) => soFar + "0")
      else if (hasDot) s + ".000"
      else s

      ts
    }

    val preT =prepare(timeString)

    DATE_FORMAT_VALIDATORS.find(_._2.findFirstIn(preT).isDefined).map(_._1).map(f=>{
      val df = getDateFormat(f)
      val dt: Date = df.parse(preT)
      if(timeString.endsWith("Z")) {
        val cal = Calendar.getInstance()
        cal.setTime(dt)
        cal.add(Calendar.HOUR, 8)
        cal.getTime()
      } else dt
    }).get
  }

  def dt2date(timeString: String): Date = try {
    dt2dateInternal(timeString)
  } catch {
    case ex: Throwable => {
      if (timeString.length == 17)
        yyyyMMddHHmmssmlsToDate(timeString)
      else
        yyyyMMddHHmmssToDate(timeString)
    }
  }

  def dt2date(timeLongInMicros: JavaLong): Date = if (timeLongInMicros == null) null else new Date(timeLongInMicros / 1000L)

  def dt2date(timeDateTime: DateTime): Date = if (timeDateTime == null) null else timeDateTime.toDate

  def dt2date(timestamp: Timestamp): Date = if (timestamp == null) null else new Date(timestamp.getTime)

  def dt2date(sqlDate: SqlDate): Date = if (sqlDate == null) null else new Date(sqlDate.getTime)

  def dt2dateTime(timeDate: Date): DateTime = if (timeDate == null) null else new DateTime(timeDate)

  def dt2dateTime(timeLongInMicros: JavaLong): DateTime = if (timeLongInMicros == null) null else dt2dateTime(dt2date(timeLongInMicros))

  def dt2dateTime(timeString: String): DateTime = if (timeString == null) null else dt2dateTime(dt2date(timeString))

  def dt2timestamp(timeDate: Date): Timestamp = if (timeDate == null) null else new Timestamp(timeDate.getTime)

  def dt2timestamp(timeLongInMicros: JavaLong): Timestamp = if (timeLongInMicros == null) null else dt2timestamp(dt2date(timeLongInMicros))

  def dt2timestamp(timeString: String): Timestamp = if (timeString == null) null else dt2timestamp(dt2date(timeString))

  def dt2sqlDate(timeDate: Date): SqlDate = if (timeDate == null) null else new SqlDate(timeDate.getTime)

  def dt2sqlDate(timeLongInMicros: JavaLong): SqlDate = if (timeLongInMicros == null) null else dt2sqlDate(dt2date(timeLongInMicros))

  def dt2sqlDate(timeString: String): SqlDate = if (timeString == null) null else dt2sqlDate(dt2date(timeString))

  def dt2dateTime(timestamp: Timestamp): DateTime = if (timestamp == null) null else dt2dateTime(dt2date(timestamp))

  def dt2timestamp(timeDateTime: DateTime): Timestamp = if (timeDateTime == null) null else dt2timestamp(dt2date(timeDateTime))

  /**
    * convert Date to Long, from 1970 base
    *
    * @param timeDate time as Date type
    * @return time as Long type in millisecond
    */
  def dt2long(timeDate: Date): JavaLong = if (timeDate == null) null else timeDate.getTime

  /**
    * convert time String to Long
    *
    * @param timeString time as String type
    * @return time as Long type in microsecond
    */
  def dt2long(timeString: String): JavaLong = if (timeString == null) null else dt2long(dt2date(timeString))

  def dt2long(timeDateTime: DateTime): JavaLong = if (timeDateTime == null) null else dt2long(dt2date(timeDateTime))

  def dt2long(timestamp: Timestamp): JavaLong = if (timestamp == null) null else dt2long(dt2date(timestamp))

  def dt2string(timeDate: Date, dtFormat: DtFormat): String = if (timeDate == null) null else DtFormat.formatter(dtFormat).format(timeDate)

  def dt2string(timeDateTime: DateTime, dtFormat: DtFormat): String = if (timeDateTime == null) null else dt2string(dt2date(timeDateTime), dtFormat)

  def dt2string(timeLongInMicros: JavaLong, dtFormat: DtFormat): String = if (timeLongInMicros == null) null else dt2string(dt2date(timeLongInMicros), dtFormat)

  def dt2string(timestamp: Timestamp, dtFormat: DtFormat): String = if (timestamp == null) null else dt2string(dt2date(timestamp), dtFormat)

  def dt2string(sqlDate: SqlDate, dtFormat: DtFormat): String = if (sqlDate == null) null else dt2string(dt2date(sqlDate), dtFormat)

  def yyyyMMddHHmmssmls(timestamp: Timestamp): String = dt2string(timestamp, DtFormat.TS_NOD_MILLISEC)

  def yyyyMMddHHmmssmls(timeDateTime: DateTime): String = yyyyMMddHHmmssmls(dt2timestamp(timeDateTime))

  def yyyyMMddHHmmssmls(timeString: String): String = yyyyMMddHHmmssmls(dt2timestamp(timeString))

  def yyyyMMddHHmmss(timeDate: Date): String = dt2string(timeDate, DtFormat.TS_NOD_SEC)

  def yyyyMMddHHmmss(timeDateTime: DateTime): String = yyyyMMddHHmmss(dt2date(timeDateTime))

  def yyyyMMddHHmmss(timeString: String): String = yyyyMMddHHmmss(dt2date(timeString))

  def yyyyMMddHHmmss(timeLongInMicros: JavaLong): String = yyyyMMddHHmmss(dt2date(timeLongInMicros))

  private def yyyyMMddHHmmssToDate(yyyyMMddHHmmss: String): Date = if (yyyyMMddHHmmss == null) null else getDateFormat(DtFormat.TS_NOD_SEC.toString).parse(yyyyMMddHHmmss)

  private def yyyyMMddHHmmssmlsToDate(yyyyMMddHHmmssmls: String): Date = if (yyyyMMddHHmmssmls == null) null else getDateFormat(DtFormat.TS_NOD_MILLISEC.toString).parse(yyyyMMddHHmmssmls)

  def yyyyMMddHHmmssToDateTime(yyyyMMddHHmmss: String): DateTime = if (yyyyMMddHHmmss == null) null else new DateTime(yyyyMMddHHmmssToDate(yyyyMMddHHmmss))

  def yyyyMMddHHmmssToTimestamp(yyyyMMddHHmmss: String): Timestamp = if (yyyyMMddHHmmss == null) null else new Timestamp(yyyyMMddHHmmssToDate(yyyyMMddHHmmss).getTime)

  def yyyyMMddHHmmssToString(yyyyMMddHHmmss: String, dtFormat: DtFormat = DtFormat.TS_DASH_MICROSEC): String = if (yyyyMMddHHmmss == null) null else dt2string(yyyyMMddHHmmssToDate(yyyyMMddHHmmss), dtFormat)

  def yyyyMMddHHmmssmlsToString(yyyyMMddHHmmssmls: String, dtFormat: DtFormat = DtFormat.TS_DASH_MILLISEC): String = if (yyyyMMddHHmmssmls == null) null else dt2string(yyyyMMddHHmmssmlsToDate(yyyyMMddHHmmssmls), dtFormat)

  def isValidTimeFormat(timeString: String, dtFormat: DtFormat): Boolean = {
    try {
      DateTimeFormat.forPattern(dtFormat.toString).parseDateTime(timeString)
      true
    } catch {
      case ex: Exception => false
    }
  }
}
