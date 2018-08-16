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

import org.joda.time.DateTime
import edp.wormhole.util.config.WormholeDefault._

object CommonUtils extends CommonUtils

trait CommonUtils {

  def tryCatch[T](block: => T): Either[java.lang.Throwable, T] = {
    try {
      Right(block)
    } catch {
      case ex: Throwable => Left(ex)
    }
  }

  def tryCatchDefault[T](block: => T, default: T): T = {
    tryCatch(block) match {
      case Right(t) => t
      case Left(e) => default
    }
  }

  val base64decoder = new sun.misc.BASE64Decoder
  val base64encoder = new sun.misc.BASE64Encoder

  def arg2array(arg: String): Array[String] = arg.split("\\s+")

  def base64byte2s(bytes: Array[Byte], default: String = null): String = base64encoder.encode(bytes)

  def base64s2byte(s: String, default: JavaByteArray = null): JavaByteArray = base64decoder.decodeBuffer(s.trim)

  def s2bytes(s: String, default: JavaByteArray = null): JavaByteArray = tryCatchDefault(s.trim.getBytes, default)

  def s2short(s: String, default: JavaShort = null): JavaShort = tryCatchDefault(s.trim.toShort, default)

  def s2int(s: String, default: JavaInteger = null): JavaInteger = tryCatchDefault(s.trim.toInt, default)

  def s2long(s: String, default: JavaLong = null): JavaLong = tryCatchDefault(s.trim.toLong, default)

  def s2float(s: String, default: JavaFloat = null): JavaFloat = tryCatchDefault(s.trim.toFloat, default)

  def s2decimal(s: String, default: JavaBigDecimal = null): JavaBigDecimal = tryCatchDefault(new java.math.BigDecimal(s.trim), default)

  def s2double(s: String, default: JavaDouble = null): JavaDouble = tryCatchDefault(s.trim.toDouble, default)

  def s2boolean(s: String, default: JavaBoolean = null): JavaBoolean = tryCatchDefault(s.trim.toBoolean, default)

  def any2string(s: Any, default: String = null): String = tryCatchDefault(s.toString, default)

  def trimLeading(s: String, c: Char): String = s.dropWhile(_ == c)

  def trimTrailing(s: String, c: Char): String = s.reverse.dropWhile(_ == c).reverse

  def trimBoth(s: String, c: Char): String = trimTrailing(trimLeading(s, c), c)

  def trimLeadingBlank(s: String): String = trimLeading(s, ' ')

  def trimTrailingBlank(s: String): String = trimTrailing(s, ' ')

  def trimBothBlank(s: String): String = s.trim

  def trimLeadingZero(s: String): String = trimLeading(s, '0')

  def trimTrailingZero(s: String): String = trimTrailing(s, '0')

  def trimBothZero(s: String): String = trimBoth(s, '0')

  def sleep(millis: Long) = Thread.sleep(millis)

  def tuple2list(tuple: Product): List[String] = 0.until(tuple.productArity).map(i => tuple.productElement(i) match {
    case timeDateTime: DateTime => DateUtils.dt2string(timeDateTime, DtFormat.TS_DASH_MILLISEC)
    case _ => any2string(tuple.productElement(i))
  }).toList
}
