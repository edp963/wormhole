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
import java.lang.{Boolean, Double, Float, Long}
import java.util.Date

import edp.wormhole.util.DateUtils

object SinkCassandraSchemaUtils extends SinkCassandraSchemaUtils
//NOT USED YET
trait SinkCassandraSchemaUtils {
  def s2CassandraValue(umsFieldType: UmsFieldType, value: String):Object = umsFieldType match{
    case UmsFieldType.STRING => if(value == null) null else value.trim
    case UmsFieldType.INT => if (value == null) null else  Integer.valueOf(value.trim)
    case UmsFieldType.LONG => if (value == null) null else Long.valueOf(value.trim)
    case UmsFieldType.FLOAT => if (value == null) null else Float.valueOf(value.trim)
    case UmsFieldType.DOUBLE => if (value == null) null else Double.valueOf(value.trim)
    case UmsFieldType.BOOLEAN => if (value == null) null else Boolean.valueOf(value.trim)
    case UmsFieldType.DATE => if (value == null) null else new Date(DateUtils.dt2date(value.trim).getTime)
    case UmsFieldType.DATETIME => if (value == null) null else new Date(DateUtils.dt2date(value.trim).getTime)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }
}
