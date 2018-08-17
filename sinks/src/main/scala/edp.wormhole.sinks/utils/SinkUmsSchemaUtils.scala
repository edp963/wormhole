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

import SinkDefault._
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.ums.UmsFieldType._
import org.joda.time.DateTime
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.CommonUtils._
import edp.wormhole.util.DtFormat

object SinkUmsSchemaUtils extends SinkUmsSchemaUtils


trait SinkUmsSchemaUtils {
  def s2umsValue(umsFieldType: UmsFieldType, value: String): Any = if (value == null) null else umsFieldType match {
    case UmsFieldType.STRING => any2string(nullify(value))
    case UmsFieldType.INT => s2int(nullify(value))
    case UmsFieldType.LONG => s2long(nullify(value))
    case UmsFieldType.FLOAT => s2float(nullify(value))
    case UmsFieldType.DOUBLE => s2double(nullify(value))
    case UmsFieldType.BOOLEAN => s2boolean(nullify(value))
    case UmsFieldType.DATE => dt2dateTime(nullify(value))
    case UmsFieldType.DATETIME => dt2dateTime(nullify(value))
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }


  def ums2sValue(umsFieldType: UmsFieldType, value: Any): String = if (value == null) null else umsFieldType match {
    case UmsFieldType.STRING => any2string(value.asInstanceOf[String])
    case UmsFieldType.INT => any2string(value.asInstanceOf[Int])
    case UmsFieldType.LONG => any2string(value.asInstanceOf[Long])
    case UmsFieldType.FLOAT => any2string(value.asInstanceOf[Float])
    case UmsFieldType.DOUBLE => any2string(value.asInstanceOf[Double])
    case UmsFieldType.BOOLEAN => any2string(value.asInstanceOf[Boolean])
    case UmsFieldType.DATE => dt2string(value.asInstanceOf[DateTime], DtFormat.DEFAULT_DF)
    case UmsFieldType.DATETIME => dt2string(value.asInstanceOf[DateTime], DtFormat.DEFAULT_DF)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }
}
