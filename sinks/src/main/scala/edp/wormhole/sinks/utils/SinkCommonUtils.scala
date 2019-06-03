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

//import java.sql.Timestamp

import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.{UmsFieldType, UmsSysField}
/*import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import edp.wormhole.common.util.CommonUtils._
import edp.wormhole.common.util.DateUtils._*/
import edp.wormhole.sinks.utils.SinkDbSchemaUtils._
//import org.apache.spark.sql.types._

object SinkCommonUtils extends SinkCommonUtils


trait SinkCommonUtils {
  def keys2keyList(keys: String): List[String] = if (keys == null) Nil else keys.split(",").map(_.trim.toLowerCase).toList

/*  def getIncrementByTs(df: DataFrame, keys: List[String], from_yyyyMMddHHmmss: String, to_yyyyMMddHHmmss: String): DataFrame = {
    val fromTs = dt2timestamp(from_yyyyMMddHHmmss)
    val toTs = dt2timestamp(dt2dateTime(to_yyyyMMddHHmmss).plusSeconds(1).minusMillis(1))
    getIncrementByTs(df, keys, fromTs, toTs)
  }

  private def getIncrementByTs(df: DataFrame, keys: List[String], fromTs: Timestamp, toTs: Timestamp): DataFrame = {
    val w = Window.partitionBy(keys.head, keys.tail: _*).orderBy(df(UmsSysField.TS.toString).desc, df(UmsSysField.ID.toString).desc)
    //    val w = Window.partitionBy(keys.head, keys.tail: _*).orderBy(df(UmsSysField.TS.toString).desc)

    df.where(df(UmsSysField.TS.toString) >= fromTs)
      .where(df(UmsSysField.TS.toString) <= toTs)
      .withColumn("rn", row_number.over(w)).where("rn = 1").drop("rn")
  }*/

  def firstTimeAfterSecond(firstTime: String, secondTime: String): Boolean = {
    if (secondTime.isEmpty) true
    else {
      if (firstTime.length == secondTime.length) {
        if (firstTime >= secondTime) true else false
      } else if (firstTime.length < secondTime.length) {
        if (firstTime > secondTime.substring(0, firstTime.length)) true else false
      } else {
        if (firstTime.substring(0, secondTime.length) >= secondTime) true else false
      }
    }
  }

  //  def compareTsAndId(firstTime: String, firstId: Long, secondTime: String, secondId: Long): Boolean = {
  //    def compareTime(firstTime: String, firstId: Long, secondTime: String, secondId: Long): Boolean = {
  //      if (firstTime > secondTime) true
  //      else if (firstTime == secondTime && firstId > secondId) true
  //      else false
  //    }
  //
  //    if (secondTime.isEmpty) true
  //    else {
  //      if (firstTime.length == secondTime.length) {
  //        compareTime(firstTime, firstId, secondTime, secondId)
  //      } else if (firstTime.length < secondTime.length) {
  //        compareTime(firstTime, firstId, secondTime.substring(0, firstTime.length), secondId)
  //      } else {
  //        compareTime(firstTime.substring(0, secondTime.length), firstId, secondTime, secondId)
  //      }
  //    }
  //  }

  def keyList2values(keyList: List[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tuple: Seq[String]): String = {
    keyList.map(keyName => {
      val fieldSchema: (Int, UmsFieldType, Boolean) = schemaMap(keyName)
      val keyValue = fieldValue(keyName, schemaMap, tuple)
      if (fieldSchema._2 == UmsFieldType.DECIMAL) keyValue.asInstanceOf[java.math.BigDecimal].toPlainString
      else keyValue.toString
    }).mkString("_")
  }

  //def keyList2values(keyList: List[String], schemaMap: Map[String, (StructField, Int)], row: Row): String = {
  //  keyList.map(keyName=>{
  ////    val fieldSchema: (Int, UmsFieldType, Boolean) = schemaMap(keyName)._1.dataType
  //    val keyValue = fieldValue(keyName, schemaMap, row)
  //    if (schemaMap(keyName)._1.dataType.typeName.startsWith("decimal")) keyValue.asInstanceOf[java.math.BigDecimal].toPlainString
  //    else keyValue.toString
  //  }).mkString("_")
  //}

  def fieldValue(fieldName: String, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tuple: Seq[String]): Any = {
    val fieldSchema: (Int, UmsFieldType, Boolean) = schemaMap(fieldName)
    s2dbValue(fieldSchema._2, tuple(fieldSchema._1))
  }

  //  def fieldValue(fieldName: String, schemaMap: Map[String, (StructField, Int)], row: Row): Any = {
  //    string2DbValue(schemaMap(fieldName)._1.dataType,row.getString(schemaMap(fieldName)._2))
  //  }
/*  def string2DbValue(fieldType: DataType, value: String): Any = if (value == null) null
  else fieldType match {
    case StringType => value.trim
    case IntegerType => value.trim.toInt
    case LongType => value.trim.toLong
    case FloatType => value.trim.toFloat
    case DoubleType => value.trim.toDouble
    case BinaryType => base64s2byte(value.trim)
    //    case DecimalType => new java.math.BigDecimal(value.trim).stripTrailingZeros()
    case BooleanType => value.trim.toBoolean
    case DateType => dt2sqlDate(value.trim)
    case TimestampType => dt2timestamp(value.trim)
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
  }*/

  def getPartitionNum(count: Long, partition_threshold: Int): Int = {
    val partition_num = if (Math.floor(count / partition_threshold) != 0)
      Math.floor(count / partition_threshold)
    else 1
    partition_num.toInt
  }

  def isLocalMode(sparkMaster: String): Boolean = sparkMaster != null && sparkMaster.contains("local")
}
