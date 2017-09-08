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


package edp.wormhole.common

import edp.wormhole.common.SparkSchemaUtils.ss2sparkTuple
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsField, UmsFieldType}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object SparkUtils extends EdpLogging{

  def getAppId:String={
    var appId = System.getProperty("spark.yarn.app.id")
    if (appId == null) {
      val tmpPath = System.getProperty("user.dir")
      if (tmpPath.indexOf("/") > -1)
        appId = if (tmpPath == null) "appId" else tmpPath.substring(tmpPath.lastIndexOf("/") + 1)
      else
        appId = if (tmpPath == null) "appId" else tmpPath.substring(tmpPath.lastIndexOf("\\") + 1)
    }
    appId
  }

  def isLocalMode(sparkMaster: String): Boolean = sparkMaster != null && sparkMaster.contains("local")

  def umsToSparkRowWrapper(ns: String, umsFields: Seq[UmsField], tuple: Seq[String]): Option[Row] = {
    try {
      Some(ss2sparkTuple(umsFields, tuple))
    } catch {
      case NonFatal(e) => logError(s"namespace = $ns, schema = $umsFields, payload.size = $tuple", e)
        None
    }
  }

  def getSchemaMap(schema: StructType): Map[String, (Int, UmsFieldType, Boolean)] = {
    var index = -1
    val schemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
    schema.fields.foreach(field => {
      if (!schemaMap.contains(field.name.toLowerCase)) {
        index += 1
        schemaMap(field.name.toLowerCase) = (index, SparkSchemaUtils.spark2umsType(field.dataType), field.nullable)
      }
    })
    schemaMap.toMap
  }

  def getRowData(row: Row, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): ArrayBuffer[String] = {
    val dataArray = ArrayBuffer.fill(schemaMap.size) {
      ""
    }
    schemaMap.foreach(column => {
      val nameToIndex = row.asInstanceOf[GenericRowWithSchema].schema.fields.map(_.name.toLowerCase).zipWithIndex.toMap
      val data = row.get(nameToIndex(column._1))
      if (column._2._2 == UmsFieldType.BINARY) {
        dataArray(column._2._1) = if (null != data) {
          if (data != null) new String(data.asInstanceOf[Array[Byte]]) else null.asInstanceOf[String]
        } else null.asInstanceOf[String]
      } else dataArray(column._2._1) = if (data != null) data.toString else null.asInstanceOf[String]
    })
    dataArray
  }
}
