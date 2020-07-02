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
import scala.collection.mutable.ListBuffer

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
    ExtFlinkSchemaUtils.tableFieldTypeArray(tableSchema, preSchemaMap)
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
          val udfName = s.trim.substring(0, s.trim.indexOf('('))
          val newName = s.trim.substring(s.indexOf("as") + 2).trim
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
      val udfName = s.trim.substring(0, s.trim.indexOf('('))
      val newName = s.trim.substring(s.indexOf("as") + 2).trim
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

  def getSchemaFromZk(zkAddress: String, realZkPath: String): String = {
    val schemaGetFromZk: String = new String(WormholeZkClient.getData(zkAddress, realZkPath))
    WormholeZkClient.closeZkClient()
    schemaGetFromZk
  }

  def findJsonSchema(config: WormholeFlinkxConfig, zkAddress: String, zkPath: String, sourceNamespace: String): UmsSchema = {
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
    ExtFlinkSchemaUtils.umsType2FlinkType(umsFieldType)
  }

  def FlinkType2UmsType(dataType: TypeInformation[_]): UmsFieldType = {
    ExtFlinkSchemaUtils.FlinkType2UmsType(dataType)
  }

  def s2FlinkType(fieldType: String): TypeInformation[_] = {
    ExtFlinkSchemaUtils.s2FlinkType(fieldType)
  }

  def object2TrueValue(flinkType: TypeInformation[_], value: Any): Any = {
    ExtFlinkSchemaUtils.object2TrueValue(flinkType, value)
  }

  def s2TrueValue(flinkType: TypeInformation[_], value: String): Any = {
    ExtFlinkSchemaUtils.s2TrueValue(flinkType, value)
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
