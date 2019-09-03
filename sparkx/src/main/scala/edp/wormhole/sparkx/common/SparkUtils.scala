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


package edp.wormhole.sparkx.common

import com.alibaba.fastjson.{JSONArray, JSONObject}
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums
import edp.wormhole.ums.{UmsField, UmsFieldType, UmsOpType, UmsSysField}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.CommonUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.OffsetRange

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
      Some(SparkSchemaUtils.ss2sparkTuple(umsFields, tuple))
    } catch {
      case NonFatal(e) => logError(s"namespace = $ns, schema = $umsFields, payload.size = $tuple", e)
        None
    }
  }

  def getSchemaMap(schema: StructType, sinkUid: Boolean): Map[String, (Int, UmsFieldType, Boolean)] = {
    var index = -1
    val schemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
    schema.fields.foreach(field => {
      if (!schemaMap.contains(field.name.toLowerCase)) {
        index += 1
        schemaMap(field.name.toLowerCase) = (index, SparkSchemaUtils.spark2umsType(field.dataType), field.nullable)
      }
    })
    //    logInfo("schemaMap:"+schemaMap)

    //    val r = schemaMap.toMap.filter(_._1 != UmsSysField.UID.toString)
    //    schemaMap.remove(UmsSysField.UID.toString)
    logInfo("schemaMap:" + schemaMap)
    if(sinkUid) {
      logInfo("sink uid, schemaMap:" + schemaMap)
      schemaMap.toMap
    } else {
      if (schemaMap.contains(UmsSysField.UID.toString)) {
        val swapFieldName = schemaMap.filter(_._2._1 == schemaMap.size - 1).head._1 // to delete ums_uid_, move ums_uid to the last one
        schemaMap(swapFieldName) = (schemaMap(UmsSysField.UID.toString)._1, schemaMap(swapFieldName)._2, schemaMap(swapFieldName)._3)
        schemaMap(UmsSysField.UID.toString) = (schemaMap.size - 1, schemaMap(UmsSysField.UID.toString)._2, schemaMap(UmsSysField.UID.toString)._3)
      }
      logInfo("swap schemaMap:" + schemaMap)
      schemaMap.remove(UmsSysField.UID.toString)
      logInfo("not sink uid, remove schemaMap:" + schemaMap)
      schemaMap.toMap
    }
  }

  def getSchemaMap(sinkFields: Seq[UmsField], sinkProcessConfig: SinkProcessConfig):
  (Map[String, (Int, UmsFieldType, Boolean)], Map[String, (Int, UmsFieldType, Boolean)], Option[Map[String, String]]) = {
    val outputField = sinkProcessConfig.sinkOutput
    val schemaArr: Seq[(String, (Int, UmsFieldType, Boolean))] = sinkFields.zipWithIndex.map(t => (t._1.name.toLowerCase, (t._2, t._1.`type`, t._1.nullable.get)))
    val tmpSchema: Map[String, (Int, UmsFieldType, Boolean)] = schemaArr.toMap
    if (outputField.nonEmpty) {
      val resultMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
      val renameMap = mutable.HashMap.empty[String, String]
      outputField.split(",").foreach { case nameField =>
        if (!nameField.contains(":")) {
          if (!tmpSchema.contains(nameField.toLowerCase)) {
            throw new Exception("output Fields DO NOT contains field " + nameField + " you configure at sink step.")
          } else {
            resultMap(nameField.toLowerCase) = tmpSchema(nameField.toLowerCase)
          }
        } else {
          val colonIndex = nameField.indexOf(":")
          val (beforeName, afterName) = (nameField.substring(0, colonIndex).toLowerCase, nameField.substring(colonIndex + 1).toLowerCase)
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
      }).toMap, tmpSchema, if (renameMap.isEmpty) None else Some(renameMap.toMap))
    } else {
      val sinkUid:Boolean = sinkProcessConfig.sinkUid
      val tmpSchemaArr = if(!sinkUid) schemaArr.filter(_._1 != UmsSysField.UID.toString)
      else schemaArr
      logInfo("tmpSchemaArr:"+tmpSchemaArr)
      var index = -1
      (tmpSchemaArr.map(t => {
        index += 1
        (t._1, (index, t._2._2, t._2._3))
      }).toMap, tmpSchema, None)
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

  def getRowData(row: Seq[String], resultSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], renameMap: Option[Map[String, String]]): ArrayBuffer[String] = {
    val dataArray = ArrayBuffer.fill(resultSchemaMap.size) {
      ""
    }
    resultSchemaMap.foreach { case (columnName, (index, fieldType, _)) => {
      val data = if (renameMap.isDefined && renameMap.get.contains(columnName)) row(originalSchemaMap(renameMap.get(columnName).toLowerCase)._1) else row(originalSchemaMap(columnName.toLowerCase)._1)
      //      if (fieldType == UmsFieldType.BINARY) {
      //        dataArray(index) = if (null != data) {
      //          if (data != null) new String(data.asInstanceOf[Array[Byte]]) else null.asInstanceOf[String]
      //        } else null.asInstanceOf[String]
      //      } else
      dataArray(index) = if (data != null) data.toString else null.asInstanceOf[String]
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
      case t if t.startsWith("DecimalType") => UmsFieldType.DECIMAL
      case "StringType" => UmsFieldType.STRING
      case "DateType" => UmsFieldType.DATE
      case "DateTimeType" => UmsFieldType.DATETIME
      case "TimestampType" => UmsFieldType.DATETIME
      case "BooleanType" => UmsFieldType.BOOLEAN
      case "BinaryType" => UmsFieldType.BINARY
      case t if t.startsWith("StructType") => UmsFieldType.JSONOBJECT
      case t if t.startsWith("ArrayType") => UmsFieldType.JSONARRAY
    }
  }

  def sparkValue2Object(value: Any, dataType: DataType): Any = {
    if (value == null) null else dataType match {
      case BinaryType => CommonUtils.base64byte2s(value.asInstanceOf[Array[Byte]])
      case FloatType => java.lang.Float.parseFloat(value.toString)
      case DoubleType => java.lang.Double.parseDouble(value.toString)
      case t if t.isInstanceOf[DecimalType] => new java.math.BigDecimal(value.toString).toPlainString
      case t if t.isInstanceOf[StructType] =>
        val rowValue = value.asInstanceOf[Row]
        val schema = t.asInstanceOf[StructType]
        schema.fields.map(f => (f.name, schema.fieldIndex(f.name), f.dataType))
          .sortBy(_._2)
          .map(tuple => (tuple._1, sparkValue2Object(rowValue.get(tuple._2), tuple._3)))
          .foldLeft(new JSONObject)((jsonObject, t) => jsonObject.fluentPut(t._1, t._2))
      case t if t.isInstanceOf[ArrayType] =>
        val seqValue = value.asInstanceOf[Seq[Any]]
        val elementType = t.asInstanceOf[ArrayType].elementType
        seqValue.map(value => sparkValue2Object(value, elementType))
          .foldLeft(new JSONArray)((jsonArray, value) => jsonArray.fluentAdd(value))
      case _ => value
    }
  }

  def getTopicPartitionOffset(offsetInfo: ArrayBuffer[OffsetRange]): JSONObject = {
    val startTopicPartitionOffsetJson = new JSONObject() //{topic,partition_offset(array)}
    offsetInfo.foreach(offsetRange => {
      val tmpJson = if (startTopicPartitionOffsetJson.containsKey(offsetRange.topic)) {
        startTopicPartitionOffsetJson.getJSONObject(offsetRange.topic)
      } else {
        val tmpTopicsJson = new JSONObject()
        tmpTopicsJson.put("topic_name", offsetRange.topic)
        tmpTopicsJson.put("partition_offset", new JSONArray())
        tmpTopicsJson
      }

      val tmpArray = tmpJson.getJSONArray("partition_offset")
      val tmpPartitionJson = new JSONObject()
      tmpPartitionJson.put("partition_num", offsetRange.partition)
      tmpPartitionJson.put("from_offset", offsetRange.fromOffset)
      tmpPartitionJson.put("until_offset", offsetRange.untilOffset)
      tmpArray.add(tmpPartitionJson)
      tmpJson.put("partition_offset", tmpArray)

      startTopicPartitionOffsetJson.put(offsetRange.topic,tmpJson)
    })

    startTopicPartitionOffsetJson
  }

  def mergeTuple(dataSeq: Seq[Seq[String]], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tableKeyList: List[String]): Seq[Seq[String]] = {
    val keys2TupleMap = new mutable.HashMap[String, Seq[String]] //[keys,tuple]
    dataSeq.foreach(dataArray => {
      val opValue = SinkCommonUtils.fieldValue(UmsSysField.OP.toString, schemaMap, dataArray)
      if (UmsOpType.BEFORE_UPDATE.toString != opValue) {
        val keyValues = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, dataArray)
        val idInTuple = dataArray(schemaMap(UmsSysField.ID.toString)._1).toLong
        if (!keys2TupleMap.contains(keyValues) || (idInTuple > keys2TupleMap(keyValues)(schemaMap(UmsSysField.ID.toString)._1).toLong)) {
          keys2TupleMap(keyValues) = dataArray
        }
      }
    })
    keys2TupleMap.values.toList
  }
}
