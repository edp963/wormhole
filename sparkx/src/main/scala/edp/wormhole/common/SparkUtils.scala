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
import edp.wormhole.ums
import edp.wormhole.ums.{UmsField, UmsFieldType, UmsSysField}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object SparkUtils extends EdpLogging {

  def getAppId: String = {
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
    if (schemaMap.contains(UmsSysField.UID.toString)) {
      val swapFieldName = schemaMap.filter(_._2._1 == schemaMap.size - 1).head._1 // to delete ums_uid_, move ums_uid to the last one
      schemaMap(swapFieldName) = (schemaMap(UmsSysField.UID.toString)._1, schemaMap(swapFieldName)._2, schemaMap(swapFieldName)._3)
      schemaMap(UmsSysField.UID.toString) = (schemaMap.size - 1, schemaMap(UmsSysField.UID.toString)._2, schemaMap(UmsSysField.UID.toString)._3)
    }
    schemaMap.toMap
  }

  def getSchemaMap(sinkFields: Seq[UmsField], outputField: String): (Map[String, (Int, UmsFieldType, Boolean)], Map[String, (Int, UmsFieldType, Boolean)], Option[Map[String, String]]) = {
    val schemaArr = sinkFields.zipWithIndex.map(t => (t._1.name, (t._2, t._1.`type`, t._1.nullable.get)))
    val tmpSchema = schemaArr.toMap
    if (outputField.nonEmpty) {
      val resultMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
      val renameMap  = mutable.HashMap.empty[String,String]
      outputField.split(",").foreach { case nameField =>
        if (!nameField.contains(":")) {
          if (!tmpSchema.contains(nameField)) {
            throw new Exception("output Fields DO NOT contains field " + nameField + " you configure at sink step.")
          } else {
            resultMap(nameField) = tmpSchema(nameField)
          }
        } else {
          val colonIndex = nameField.indexOf(":")
          val (beforeName, afterName) = (nameField.substring(0, colonIndex), nameField.substring(colonIndex + 1))
          if (!tmpSchema.contains(beforeName)) {
            throw new Exception("output Fields DO NOT contains field " + beforeName + " you configure at sink step.")
          } else {
            renameMap(afterName) = beforeName
            resultMap(afterName) = tmpSchema(beforeName)
          }
        }
      }
      var index = -1
      (resultMap.map(t => {
        index += 1
        (t._1, (index, t._2._2, t._2._3))
      }).toMap, tmpSchema, if(renameMap.isEmpty) None else Some(renameMap.toMap))
    } else {
      var index = -1
      (schemaArr.filter(_._1 != UmsSysField.UID.toString).map(t => {
        index += 1
        (t._1, (index, t._2._2, t._2._3))
      }).toMap, tmpSchema,None)
    }
  }


  //

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

  def getRowData(row: Seq[String], resultSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],renameMap:Option[Map[String, String]]): ArrayBuffer[String] = {
    val dataArray = ArrayBuffer.fill(resultSchemaMap.size) {""}
    resultSchemaMap.foreach { case (columnName, (index, fieldType, _)) => {
      val data = if (renameMap.isDefined && renameMap.get.contains(columnName)) row(originalSchemaMap(renameMap.get(columnName))._1) else row(originalSchemaMap(columnName)._1)
      if (fieldType == UmsFieldType.BINARY) {
        dataArray(index) = if (null != data) {
          if (data != null) new String(data.asInstanceOf[Array[Byte]]) else null.asInstanceOf[String]
        } else null.asInstanceOf[String]
      } else dataArray(index) = if (data != null) data.toString else null.asInstanceOf[String]
    }
    }
    dataArray
  }

  def sparkSqlType2UmsFieldType(dataType: String): ums.UmsFieldType.Value = {
    dataType match {
      case "DoubleType" => UmsFieldType.DOUBLE
      case "FloatType" => UmsFieldType.FLOAT
      case "LongType" => UmsFieldType.LONG
      case "IntegerType" => UmsFieldType.INT
      case "DecimalType" => UmsFieldType.DECIMAL
      case "StringType" => UmsFieldType.STRING
      case "DateType" => UmsFieldType.DATETIME
      case "TimestampType" => UmsFieldType.DATETIME
    }
  }
}
