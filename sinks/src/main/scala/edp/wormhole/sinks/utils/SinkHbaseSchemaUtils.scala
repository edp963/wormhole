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
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.DateUtils._
import org.apache.hadoop.hbase.util.Bytes

object SinkHbaseSchemaUtils extends SinkHbaseSchemaUtils

trait SinkHbaseSchemaUtils {
  def s2hbaseValue(umsFieldType: UmsFieldType, value: String): Array[Byte] = umsFieldType match {
    case UmsFieldType.STRING => if (value == null) null else if(value.trim=="") Bytes.toBytes(" ") else Bytes.toBytes(value.trim)
    case UmsFieldType.INT => if (value == null) null else Bytes.toBytes(value.trim.toInt)
    case UmsFieldType.LONG => if (value == null) null else Bytes.toBytes(value.trim.toLong)
    case UmsFieldType.FLOAT => if (value == null) null else Bytes.toBytes(value.trim.toFloat)
    case UmsFieldType.DOUBLE => if (value == null) null else Bytes.toBytes(value.trim.toDouble)
    case UmsFieldType.BOOLEAN => if (value == null) null else Bytes.toBytes(value.trim.toBoolean)
    case UmsFieldType.DECIMAL => if (value == null) null else Bytes.toBytes(new java.math.BigDecimal(new java.math.BigDecimal(value.trim).stripTrailingZeros().toPlainString))
    case UmsFieldType.DATE => if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
    case UmsFieldType.DATETIME => if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
    case UmsFieldType.BINARY => if (value == null) null else CommonUtils.base64s2byte(value.trim)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }
//def s2hbaseValue(umsFieldType: DataType, value: String): Array[Byte] =
//  if (umsFieldType.typeName.startsWith("decimal")) {if (value == null) null else Bytes.toBytes(new java.math.BigDecimal(value.trim).stripTrailingZeros())}
//  else umsFieldType match {
//  case StringType => if (value == null) null else Bytes.toBytes(value.trim)
//  case IntegerType => if (value == null) null else Bytes.toBytes(value.trim.toInt)
//  case LongType => if (value == null) null else Bytes.toBytes(value.trim.toLong)
//  case FloatType => if (value == null) null else Bytes.toBytes(value.trim.toFloat)
//  case DoubleType => if (value == null) null else Bytes.toBytes(value.trim.toDouble)
//  case BooleanType => if (value == null) null else Bytes.toBytes(value.trim.toBoolean)
////  case DecimalType => if (value == null) null else Bytes.toBytes(new java.math.BigDecimal(value.trim).stripTrailingZeros())
//  case DateType => if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
//  case TimestampType => if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
//  case BinaryType => if (value == null) null else base64s2byte(value.trim)
//  case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
//}

//  def s2hbaseStringValue(umsFieldType: DataType, value: String, column: String): Array[Byte] =
//    if (column.toLowerCase == "ums_ts_") {
//      umsFieldType match {
//        case DateType=> if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
//        case TimestampType => if (value == null) null else Bytes.toBytes(dt2date(value).getTime)
//      }
//    }
//    else {
//      if (umsFieldType.typeName.startsWith("decimal")) {
//        Bytes.toBytes(new java.math.BigDecimal(value.trim).toString)
//      }
//      else Bytes.toBytes(value.trim)
//    }

  def s2hbaseStringValue(umsFieldType: UmsFieldType, value: String, column: String,umsTsSaveAsString:Boolean): Array[Byte] =
    if (column.toLowerCase == "ums_ts_") {
      umsFieldType match {
        case UmsFieldType.DATE => if (value == null) null else if(umsTsSaveAsString) Bytes.toBytes(dt2date(value).getTime.toString) else Bytes.toBytes(dt2date(value).getTime)
        case UmsFieldType.DATETIME => if (value == null) null else if(umsTsSaveAsString) Bytes.toBytes(dt2date(value).getTime.toString) else Bytes.toBytes(dt2date(value).getTime)
      }
    }
    //    else {
    //      if (umsFieldType.equals(UmsFieldType.DECIMAL)) {
    //        if (value == null) null else Bytes.toBytes(new java.math.BigDecimal(value.trim).toString)
    //      }
    else {
      if (value == null) null
      else if(value.trim=="") Bytes.toBytes(" ")
      else Bytes.toBytes(value.trim)
    }

  //  def s2hbaseValue(umsFieldType: UmsFieldType, value: String): Array[Byte] = umsFieldType match {
  //    case UmsFieldType.STRING => Bytes.toBytes(any2string(nullify(value)))
  //    case UmsFieldType.INT => Bytes.toBytes(s2int(nullify(value)))
  //    case UmsFieldType.LONG => Bytes.toBytes(s2long(nullify(value)))
  //    case UmsFieldType.FLOAT => Bytes.toBytes(s2float(nullify(value)))
  //    case UmsFieldType.DOUBLE => Bytes.toBytes(s2double(nullify(value)))
  //    case UmsFieldType.BOOLEAN => Bytes.toBytes(s2boolean(nullify(value)))
  //    case UmsFieldType.DATE => Bytes.toBytes(dt2date(nullify(value)).getTime)
  //    case UmsFieldType.DATETIME => Bytes.toBytes(dt2date(nullify(value)).getTime)
  //    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  //  }

  def hbase2umsValue(umsFieldType: UmsFieldType, value: Array[Byte]): Any =
    if (value == null || value.length == 0) null
    else umsFieldType match {
      case UmsFieldType.STRING => Bytes.toString(value)
      case UmsFieldType.INT => Bytes.toInt(value)
      case UmsFieldType.LONG => Bytes.toLong(value)
      case UmsFieldType.FLOAT => Bytes.toFloat(value)
      case UmsFieldType.DOUBLE => Bytes.toDouble(value)
      case UmsFieldType.BOOLEAN => Bytes.toBoolean(value)
      case UmsFieldType.DATE => dt2dateTime(Bytes.toLong(value) * 1000)
      case UmsFieldType.DATETIME => dt2dateTime(Bytes.toLong(value) * 1000)
      case UmsFieldType.DECIMAL => Bytes.toBigDecimal(value)
      case UmsFieldType.BINARY => Bytes.toStringBinary(value)
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
    }
}
