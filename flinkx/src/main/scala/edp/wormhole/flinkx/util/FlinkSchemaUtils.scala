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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.flinkx.common.WormholeFlinkxConfig
import edp.wormhole.flinkx.swifts.FlinkxSwiftsConstants
import edp.wormhole.kafka.WormholeKafkaConsumer
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType.{DATA_BATCH_DATA, DATA_INCREMENT_DATA, DATA_INITIAL_DATA}
import edp.wormhole.ums.{UmsCommonUtils, UmsSchema, UmsSysField}
import edp.wormhole.util.DateUtils
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.Map

object FlinkSchemaUtils extends java.io.Serializable {

  private val logger = Logger.getLogger(this.getClass)
  val sourceSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]

  lazy val immutableSourceSchemaMap: Map[String, (TypeInformation[_], Int)] = sourceSchemaMap.toMap

  val swiftsProcessSchemaMap = mutable.HashMap.empty[String, Map[String, (TypeInformation[_], Int)]]

  def sourceFieldNameArray: Array[String] = getFieldNamesFromSchema(immutableSourceSchemaMap)

  def sourceFlinkTypeArray: Array[TypeInformation[_]] = sourceFieldNameArray.map(field => immutableSourceSchemaMap(field)._1)

  def sourceFieldIndexArray: Array[Int] = sourceFieldNameArray.map(field => immutableSourceSchemaMap(field)._2)

  def setSourceSchemaMap(umsSchema: UmsSchema): Unit = {
    val fields = umsSchema.fields_get
    var index = 0
    fields.foreach {
      field =>
        sourceSchemaMap += field.name -> (umsType2FlinkType(field.`type`), index)
        index += 1
    }
  }

  val udfSchemaMap = mutable.HashMap.empty[String, TypeInformation[_]]

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

  def tableFieldTypeArray(tableSchema: TableSchema, preSchemaMap: Map[String, (TypeInformation[_], Int)]): Array[TypeInformation[_]] = {
    tableSchema.getFieldNames.map(fieldName => {
      val fieldType = preSchemaMap(fieldName)._1
      if (fieldType == TimeIndicatorTypeInfo.PROCTIME_INDICATOR || fieldType == TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
        SqlTimeTypeInfo.TIMESTAMP
      else fieldType
    })
  }

  def getSchemaMapFromTable(tableSchema: TableSchema, projectClause: String, udfSchemaMap: Map[String, TypeInformation[_]], specialConfigObj: JSONObject): Map[String, (TypeInformation[_], Int)] = {
    logger.debug("in getSchemaMapFromTable *******************")
    logger.debug("projectClause: " + projectClause)
    val fieldString = projectClause.substring(6)
    logger.debug("fieldString: " + fieldString)

    val nameMap = mutable.HashMap.empty[String, String]
    var s = ""
    var num = 0
    for (sIndex <- 0 until fieldString.length) {
      if (fieldString(sIndex) == ',' && num == 0) {
        if (s.contains('(') && s.contains("as")) {
          //val udfName = s.trim.substring(0, s.trim.indexOf('('))
          //val newName = s.trim.substring(s.indexOf("as") + 2).trim
          val udfName = s.substring(0, s.indexOf('(')).trim
          val newName = s.substring(s.indexOf("as") + 2).trim
          nameMap += newName -> udfName
        }
        s = ""
      } else {
        if (fieldString(sIndex) == '(') num += 1
        else if (fieldString(sIndex) == ')') num -= 1
        s = s + fieldString(sIndex)
      }
    }
    if (s.contains('(') && s.contains("as")) {
      //val udfName = s.trim.substring(0, s.trim.indexOf('('))
      //val newName = s.trim.substring(s.indexOf("as") + 2).trim
      val udfName = s.substring(0, s.indexOf('(')).trim
      val newName = s.substring(s.indexOf("as") + 2).trim
      nameMap += newName -> udfName
    }
    logger.debug("nameMap:" + nameMap.toString())

    val resultSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    var index = 0
    var udfIndexCur = 0
    tableSchema.getFieldNames.foreach(s => {
      logger.debug(s"field $index in table $s")
      if (tableSchema.getFieldType(s).get.toString.contains("java.lang.Object") && udfSchemaMap.contains(nameMap(s))) {
        resultSchemaMap += s -> (udfSchemaMap(nameMap(s)), index)
        udfIndexCur += 1
      } else {
        resultSchemaMap += s -> (tableSchema.getFieldType(s).get, index)
      }
      index += 1
    }
    )
    if (null != specialConfigObj && specialConfigObj.containsKey(FlinkxSwiftsConstants.PRESERVE_MESSAGE_FLAG) && specialConfigObj.getBooleanValue(FlinkxSwiftsConstants.PRESERVE_MESSAGE_FLAG)) {
      resultSchemaMap += FlinkxSwiftsConstants.MESSAGE_FLAG -> (Types.BOOLEAN, index)
    }
    resultSchemaMap.toMap
  }

  def getSchemaMapFromArray(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): Map[String, (TypeInformation[_], Int)] = {
    val resultSchemaMap = mutable.HashMap.empty[String, (TypeInformation[_], Int)]
    for (i <- fieldNames.indices) {
      resultSchemaMap += fieldNames(i) -> (fieldTypes(i), i)
    }
    resultSchemaMap.toMap
  }


  def getFieldTypes(fieldNames: Array[String], schemaMap: Map[String, (TypeInformation[_], Int)]): Array[TypeInformation[_]] = {
    fieldNames.map(field => {
      schemaMap(field)._1
    })
  }

  /**
   * In order to find the correct umsSchema
   * The function need to read from kafka to find the corresponding sourceNamespace data
   * @param config
   * @param sourceNamespace
   * @return
   */
  def findUmsSchemaFromKafka(config: WormholeFlinkxConfig, sourceNamespace: String): UmsSchema = {
    val consumer = WormholeKafkaConsumer.initConsumer(config.kafka_input.kafka_base_config.brokers, config.kafka_input.kafka_base_config.group_id, None, config.kafka_input.kafka_base_config.kerberos)
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
            logger.debug(r.offset() + " offset")
            val (key, value) = (r.key(), r.value())
            logger.debug("key is " + key)
            val key2Verify = UmsCommonUtils.checkAndGetKey(key, value)
            logger.debug("key2Verify " + key2Verify)
            correctData = isCorrectRecord(key2Verify, value, sourceNamespace)
            if (correctData) {
              logger.debug(s"the true value $value")
              val ums = UmsCommonUtils.json2Ums(value)
              if (ums.payload.isEmpty || ums.schema.fields.isEmpty || !matchNamespace(ums.schema.namespace, sourceNamespace)) {
                logger.debug("ums is not correct")
                correctData = false
              }
              else record = ums.schema
            }
          }
        } else logger.debug("continue")
      }
    } catch {
      case e: Throwable => logger.error("findUmsSchemaFromKafka", e)
    }

    WormholeKafkaConsumer.close(consumer)
    val umsSchema: UmsSchema = record

    umsSchema
  }

  private def isCorrectRecord(key: String, value: String, sourceNamespace: String): Boolean = {
    val (umsProtocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(key)
    if ((umsProtocolType == DATA_INITIAL_DATA || umsProtocolType == DATA_INCREMENT_DATA || umsProtocolType == DATA_BATCH_DATA) && matchNamespace(namespace, sourceNamespace)) {
      true
    } else {
      logger.debug("continue")
      false
    }
  }


  def matchNamespace(dataNamespace: String, sourceNamespace: String): Boolean = {
    val dataNamespacePart = dataNamespace.split("\\.")
    logger.debug("the dataNamespace is " + dataNamespace)
    val sourceNamespacePart = sourceNamespace.split("\\.")
    logger.debug("the sourceNamespace is " + sourceNamespace)
    var compareNum = 4
    if (sourceNamespacePart(4) == "*") compareNum = 4
    else if (sourceNamespacePart(5) == "*") compareNum = 5
    else if (sourceNamespacePart(6) == "*") compareNum = 6
    else compareNum = 7
    logger.debug("the final compareNum is " + compareNum)
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
      //case _ => INT
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
    case Types.STRING =>  value.asInstanceOf[String].trim
    case Types.INT => value.asInstanceOf[Int]
    case Types.LONG => value match {
      case _: Int => value.asInstanceOf[Int].toLong
      case _ => value.asInstanceOf[Long]
    }
    case Types.FLOAT => value.asInstanceOf[Float]
    case Types.DOUBLE => value match {
      case _: Float => value.asInstanceOf[Float].toDouble
      case _ => value.asInstanceOf[Double]
    }
    case Types.BOOLEAN => value.asInstanceOf[Boolean]
    case Types.SQL_DATE => value match {
      case _:Timestamp => DateUtils.dt2sqlDate(value.asInstanceOf[Timestamp])
      case _=>DateUtils.dt2sqlDate(value.asInstanceOf[Date])
    }
    case Types.SQL_TIMESTAMP => value.asInstanceOf[Timestamp]
    case Types.DECIMAL => new java.math.BigDecimal(value.asInstanceOf[java.math.BigDecimal].stripTrailingZeros().toPlainString)
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

  def getRelValue(fieldIndex: Int, value: String, schemaMap: Map[String, (TypeInformation[_], Int)]): Any =
    if (value == null) null
    else {
      val fieldNames = getFieldNamesFromSchema(schemaMap)
      val flinkTypes = fieldNames.map(field => schemaMap(field)._1)
      s2TrueValue(flinkTypes(fieldIndex), value)
    }

  def checkOtherData(protocolType: String): Boolean = {
    protocolType.startsWith("directive_") || protocolType.endsWith("_heartbeat") || protocolType.endsWith("_termination")
  }
}
