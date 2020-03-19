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


package edp.wormhole.sinks.utils

import edp.wormhole.ums.UmsFieldType
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.{CommonUtils, DateUtils}


object SinkDbSchemaUtils extends SinkDbSchemaUtils

trait SinkDbSchemaUtils {
  def s2dbValue(umsFieldType: UmsFieldType, value: String): Any = if (value == null) null
  else umsFieldType match {
    case UmsFieldType.STRING => value.trim
    case UmsFieldType.INT => value.trim.toInt
    case UmsFieldType.LONG => value.trim.toLong
    case UmsFieldType.FLOAT => value.trim.toFloat
    case UmsFieldType.DOUBLE => value.trim.toDouble
    case UmsFieldType.BINARY => CommonUtils.base64s2byte(value.trim)
    case UmsFieldType.DECIMAL => new java.math.BigDecimal(new java.math.BigDecimal(value.trim).stripTrailingZeros().toPlainString)
    case UmsFieldType.BOOLEAN => value.trim.toBoolean
    case UmsFieldType.DATE => DateUtils.dt2sqlDate(value.trim)
    case UmsFieldType.DATETIME => DateUtils.dt2timestamp(value.trim)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }

  def ums2dbType(umsFieldType: UmsFieldType): Int = umsFieldType match {
    case UmsFieldType.STRING => java.sql.Types.VARCHAR
    case UmsFieldType.INT => java.sql.Types.INTEGER
    case UmsFieldType.LONG => java.sql.Types.BIGINT
    case UmsFieldType.FLOAT => java.sql.Types.FLOAT
    case UmsFieldType.DOUBLE => java.sql.Types.DOUBLE
    case UmsFieldType.DECIMAL => java.sql.Types.DECIMAL
    case UmsFieldType.BOOLEAN => java.sql.Types.BIT
    case UmsFieldType.BINARY => java.sql.Types.BINARY
    case UmsFieldType.DATE => java.sql.Types.DATE
    case UmsFieldType.DATETIME => java.sql.Types.TIMESTAMP
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }
}
