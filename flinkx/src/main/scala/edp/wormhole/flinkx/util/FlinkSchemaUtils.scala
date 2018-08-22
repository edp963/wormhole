/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole.flinkx.util

import java.sql.{Date, Timestamp}

import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.flinkx.eventflow.WormholeFlinkxConfig
import edp.wormhole.kafka.WormholeKafkaConsumer
import edp.wormhole.swifts.SwiftsConstants
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType.{DATA_BATCH_DATA, DATA_INCREMENT_DATA, DATA_INITIAL_DATA}
import edp.wormhole.ums.{UmsCommonUtils, UmsSchema, UmsSysField}
import edp.wormhole.util.DateUtils
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, TypeInformation}
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.log4j.Logger

import scala.collection.mutable

object FlinkSchemaUtils extends java.io.Serializable {

  private val logger = Logger.getLogger(this.getClass)
  val sourceSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]

  val swiftsProcessSchemaMap = mutable.HashMap.empty[String, Map[String, (TypeInformation[_], Int)]]

  def sourceFieldNameArray: Array[String] = getFieldNamesFromSchema(sourceSchemaMap.toMap)

  def sourceFlinkTypeArray: Array[TypeInformation[_]] = sourceFieldNameArray.map(field => sourceSchemaMap(field)._1)

  def sourceFieldIndexArray: Array[Int] = sourceFieldNameArray.map(field => sourceSchemaMap(field)._2)

  def setSourceSchemaMap(umsSchema: UmsSchema): Unit = {
    val fields = umsSchema.fields_get
    sourceSchemaMap += SwiftsConstants.PROTOCOL_TYPE -> (Types.STRING, 0)
    var index = 1
    fields.foreach {
      field =>
        sourceSchemaMap += field.name -> (umsType2FlinkType(field.`type`), index)
        index += 1
    }
  }

  def setSwiftsSchema(key: String, value: Map[String, (TypeInformation[_], Int)]): Unit = {
    if (!FlinkSchemaUtils.swiftsProcessSchemaMap.contains(key))
      FlinkSchemaUtils.swiftsProcessSchemaMap(key) = value
  }

  def getFieldNamesFromSchema(schemaMap: Map[String, (TypeInformation[_], Int)]): Array[String] = {
    val fieldInMap = schemaMap.keySet.toArray
    val newArray = new Array[String](fieldInMap.length)
    fieldInMap.foreach(field => {
      val index = schemaMap(field)._2
      newArray(index) = field
    })
    newArray
  }

  def tableFieldNameArray(tableSchema: TableSchema): Array[String] = {
    tableSchema.getColumnNames
  }

  def tableFieldTypeArray(tableSchema: TableSchema): Array[TypeInformation[_]] = {
    tableFieldNameArray(tableSchema).map(fieldName => tableSchema.getType(fieldName).get)
  }

  def getSchemaMapFromTable(tableSchema: TableSchema): Map[String, (TypeInformation[_], Int)] = {
    println("in getSchemaMapFromTable *******************")
    val resultSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    var index = 0
    tableSchema.getColumnNames.foreach(s => {
      logger.info(s"field $index in table $s")
      resultSchemaMap += s -> (tableSchema.getType(s).get, index)
      index += 1
    }
    )
    resultSchemaMap.toMap
  }

  def getSchemaMapFromArray(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): Map[String, (TypeInformation[_], Int)] = {
    println("in getSchemaMapFromArray &&&&&&&&&&&&&&&&&&")
    val resultSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    for (i <- fieldNames.indices) {
      resultSchemaMap += fieldNames(i) -> (fieldTypes(i), i)
    }
    resultSchemaMap.toMap
  }

  def getOutputFieldNames(outputFieldList: Array[String], keyByFields: String): Array[String] = {
    val outputFieldSize: Int = outputFieldList.length
    val outputFieldNames = for (i <- 0 until outputFieldSize)
      yield outputFieldList(i).split(":").head
    if (keyByFields != null && keyByFields.nonEmpty)
      Array(SwiftsConstants.PROTOCOL_TYPE.toString, UmsSysField.ID.toString, UmsSysField.TS.toString, UmsSysField.OP.toString) ++
        keyByFields.split(";") ++ outputFieldNames
    else
      Array(SwiftsConstants.PROTOCOL_TYPE.toString, UmsSysField.ID.toString, UmsSysField.TS.toString, UmsSysField.OP.toString) ++
        outputFieldNames
  }

  def getOutPutFieldTypes(fieldNames: Array[String], schemaMap: Map[String, (TypeInformation[_], Int)]): Array[TypeInformation[_]] = {
    fieldNames.map(field => {
      schemaMap(field)._1
    })
  }

  def getSchemaFromZk(zkAddress: String, realZkPath: String): String = {
    val schemaGetFromZk: String = new String(WormholeZkClient.getData(zkAddress, realZkPath))
    WormholeZkClient.closeZkClient()
    schemaGetFromZk
  }

  def findJsonSchema(config: WormholeFlinkxConfig, zkAddress: String, zkPath: String, sourceNamespace: String): UmsSchema = {
    val consumer = WormholeKafkaConsumer.initConsumer(config.kafka_input.kafka_base_config.brokers, config.kafka_input.kafka_base_config.group_id, None)
    WormholeKafkaConsumer.subscribeTopicFromOffset(consumer, new WormholeFlinkxConfigUtils(config).getTopicOffsetMap)
    var correctData = false
    var record: UmsSchema = null
    try {
      while (!correctData) {
        val records: ConsumerRecords[String, String] = WormholeKafkaConsumer.consumerRecords(consumer, 10000)
        if (records != null && !records.isEmpty) {
          val it = records.iterator()
          while (it.hasNext && !correctData) {
            val r: ConsumerRecord[String, String] = it.next()
            println(r.offset() + " offset")
            val (key, value) = (r.key(), r.value())
            println("key is " + key)
            val key2Verify = UmsCommonUtils.checkAndGetKey(key, value)
            println("key2Verify " + key2Verify)
            correctData = isCorrectRecord(key2Verify, value, sourceNamespace)
            if (correctData) {
              println(s"the true value $value")
              val ums = UmsCommonUtils.json2Ums(value)
              if (ums.payload.isEmpty || ums.schema.fields.isEmpty || !matchNamespace(ums.schema.namespace, sourceNamespace)) {
                println("ums is not correct")
                correctData = false
              }
              else record = ums.schema
            }
          }
        } else logger.info("continue")
      }
    } catch {
      case e: Throwable => logger.error("findJsonSchema", e)
    }

    WormholeKafkaConsumer.close(consumer)
    val umsSchema: UmsSchema = record
    //    WormholeZkClient.createAndSetData(zkAddress, zkPath, jsonSchema.getBytes("UTF-8"))
    //    WormholeZkClient.closeZkClient()
    umsSchema
  }

  private def isCorrectRecord(key: String, value: String, sourceNamespace: String): Boolean = {
    val (umsProtocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(key)
    if ((umsProtocolType == DATA_INITIAL_DATA || umsProtocolType == DATA_INCREMENT_DATA || umsProtocolType == DATA_BATCH_DATA) && matchNamespace(namespace, sourceNamespace)) {
      true
    } else {
      logger.info("continue")
      false
    }
  }


  def matchNamespace(dataNamespace: String, sourceNamespace: String): Boolean = {
    val dataNamespacePart = dataNamespace.split("\\.")
    println("the dataNamespace is " + dataNamespace)
    val sourceNamespacePart = sourceNamespace.split("\\.")
    println("the sourceNamespace is " + sourceNamespace)
    var compareNum = 4
    if (sourceNamespacePart(4) == "*") compareNum = 4
    else if (sourceNamespacePart(5) == "*") compareNum = 5
    else if (sourceNamespacePart(6) == "*") compareNum = 6
    else compareNum = 7
    println("the final compareNum is " + compareNum)
    dataNamespacePart.slice(0, compareNum).mkString("") == sourceNamespacePart.slice(0, compareNum).mkString("")
  }


  def umsType2FlinkType(umsFieldType: UmsFieldType): TypeInformation[_] = {
    umsFieldType match {
      case STRING => Types.STRING
      case INT => Types.INT
      case LONG => Types.LONG
      case FLOAT => Types.FLOAT
      case DOUBLE => Types.DOUBLE
      case BOOLEAN => Types.BOOLEAN
      case DATE => Types.SQL_DATE
      case DATETIME => Types.SQL_TIMESTAMP
      case DECIMAL => Types.DECIMAL
    }
  }

  def FlinkType2UmsType(dataType: TypeInformation[_]): UmsFieldType = {
    dataType match {
      case Types.STRING => STRING
      case Types.INT => INT
      case Types.LONG => LONG
      case Types.FLOAT => FLOAT
      case Types.DOUBLE => DOUBLE
      case Types.BOOLEAN => BOOLEAN
      case Types.SQL_DATE => DATE
      case Types.SQL_TIMESTAMP => DATETIME
      case Types.DECIMAL => DECIMAL
    }

  }

  def s2FlinkType(fieldType: String): TypeInformation[_] = {
    fieldType match {
      case "datetime" => Types.SQL_TIMESTAMP
      case "date" => Types.SQL_DATE
      case "decimal" => Types.DECIMAL
      case "int" => Types.INT
      case "long" => Types.LONG
      case "float" => Types.FLOAT
      case "double" => Types.DOUBLE
      case "string" => Types.STRING
      case "binary" => BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO

      case unknown =>
        throw new Exception("unknown type:" + unknown)
    }
  }

  def object2TrueValue(flinkType: TypeInformation[_], value: Any): Any = if (value == null) null
  else flinkType match {
    case Types.STRING => value.asInstanceOf[String].trim
    case Types.INT => value.asInstanceOf[Int]
    case Types.LONG => value.asInstanceOf[Long]
    case Types.FLOAT => value.asInstanceOf[Float]
    case Types.DOUBLE => value.asInstanceOf[Double]
    case Types.BOOLEAN => value.asInstanceOf[Boolean]
    case Types.SQL_DATE => DateUtils.dt2sqlDate(value.asInstanceOf[Date])
    case Types.SQL_TIMESTAMP => value.asInstanceOf[Timestamp]
    case Types.DECIMAL => new java.math.BigDecimal(value.asInstanceOf[String].trim).stripTrailingZeros()
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $flinkType")
  }

  def s2TrueValue(flinkType: TypeInformation[_], value: String): Any = if (value == null) null
  else flinkType match {
    case Types.STRING => value.trim
    case Types.INT => value.trim.toInt
    case Types.LONG => value.trim.toLong
    case Types.FLOAT => value.trim.toFloat
    case Types.DOUBLE => value.trim.toDouble
    case Types.BOOLEAN => value.trim.toBoolean
    case Types.SQL_DATE => DateUtils.dt2sqlDate(value.trim)
    case Types.SQL_TIMESTAMP => DateUtils.dt2timestamp(value.trim)
    case Types.DECIMAL => new java.math.BigDecimal(value.trim).stripTrailingZeros()
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $flinkType")
  }

  def getRelValue(fieldIndex: Int, value: String, schemaMap: Map[String, (TypeInformation[_], Int)]): Any = if (value == null) null
  else {
    val fieldNames = getFieldNamesFromSchema(schemaMap)
    val flinkTypes = fieldNames.map(field => schemaMap(field)._1)
    flinkTypes(fieldIndex) match {
      case Types.STRING => value.trim
      case Types.INT => value.trim.toInt
      case Types.LONG => value.trim.toLong
      case Types.FLOAT => value.trim.toFloat
      case Types.DOUBLE => value.trim.toDouble
      case Types.BOOLEAN => value.trim.toBoolean
      case Types.SQL_DATE => DateUtils.dt2sqlDate(value.trim)
      case Types.SQL_TIMESTAMP => DateUtils.dt2timestamp(value.trim)
      case Types.DECIMAL => new java.math.BigDecimal(value.trim).stripTrailingZeros()
      case _ => throw new UnsupportedOperationException(s"Unknown Type: ${
        flinkTypes(fieldIndex)
      }")
    }
  }

}
