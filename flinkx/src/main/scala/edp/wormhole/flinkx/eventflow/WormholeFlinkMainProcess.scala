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

package edp.wormhole.flinkx.eventflow

import java.util.Properties

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import edp.wormhole.common.json.FieldInfo
import edp.wormhole.flinkx.common.ConfMemoryStorage
import edp.wormhole.flinkx.deserialization.WormholeDeserializationStringSchema
import edp.wormhole.flinkx.sink.SinkProcess
import edp.wormhole.flinkx.swifts.{ParseSwiftsSql, SwiftsProcess}
import edp.wormhole.flinkx.util.FlinkSchemaUtils._
import edp.wormhole.flinkx.util.{FlinkxTimestampExtractor, UmsFlowStartUtils, WormholeFlinkxConfigUtils}
import edp.wormhole.swifts.SwiftsConstants
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.types.Row
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer


class WormholeFlinkMainProcess(config: WormholeFlinkxConfig, umsFlowStart: Ums) extends Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)
  private val flowStartFields = umsFlowStart.schema.fields_get
  private val flowStartPayload = umsFlowStart.payload_get.head
  private val swiftsString: String = UmsFlowStartUtils.extractSwifts(flowStartFields, flowStartPayload)
  logger.info(swiftsString + "------swifts string")
  private val swifts: fastjson.JSONObject = JSON.parseObject(swiftsString)
  private val timeCharacteristic = UmsFlowStartUtils.extractTimeCharacteristic(swifts)
  private val sinkNamespace = UmsFlowStartUtils.extractSinkNamespace(flowStartFields, flowStartPayload)
  private val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)

  def process(): JobExecutionResult = {
    val swiftsSql = getSwiftsSql(swiftsString, UmsFlowStartUtils.extractDataType(flowStartFields, flowStartPayload))
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(config.parallelism)

    assignTimeCharacteristic(env)
    val inputStream: DataStream[Row] = createKafkaStream(env, umsFlowStart.schema.namespace.toLowerCase)
    val watermarkStream = assignTimestamp(inputStream, sourceSchemaMap.toMap)
    watermarkStream.print()
    try {
      val (stream, schemaMap) = SwiftsProcess.process(watermarkStream, sourceNamespace, TableEnvironment.getTableEnvironment(env), swiftsSql)
      SinkProcess.doProcess(stream, umsFlowStart, schemaMap)
    } catch {
      case e: Throwable => logger.error("swifts and sink", e)
    }

    env.execute(s"$sourceNamespace-$sinkNamespace")
  }

  private def createKafkaStream(env: StreamExecutionEnvironment, flowNamespace: String) = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.kafka_input.kafka_base_config.brokers)
    properties.setProperty("zookeeper.connect", config.zookeeper_address)
    properties.setProperty("group.id", config.kafka_input.groupId)
    properties.setProperty("session.timeout.ms", config.kafka_input.sessionTimeout)
    properties.setProperty("enable.auto.commit", config.kafka_input.autoCommit.toString)

    val flinkxConfigUtils = new WormholeFlinkxConfigUtils(config)
    val topics = flinkxConfigUtils.getKafkaTopicList
    val myConsumer = new FlinkKafkaConsumer010[(String, String, String, Int, Long)](topics, new WormholeDeserializationStringSchema, properties)

    val specificStartOffsets = flinkxConfigUtils.getTopicPartitionOffsetMap
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
    val consumeProtocolMap = UmsFlowStartUtils.extractConsumeProtocol(flowStartFields, flowStartPayload)
    val initialStream: DataStream[(String, String, String, Int, Long)] = env.addSource(myConsumer)
      .map(event => (UmsCommonUtils.checkAndGetKey(event._1, event._2), event._2, event._3, event._4, event._5))
      .filter(event => {
        val (umsProtocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(event._1)
        consumeProtocolMap.contains(umsProtocolType) && consumeProtocolMap(umsProtocolType) && matchNamespace(namespace, flowNamespace)
      })

    val jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])] = ConfMemoryStorage.getAllSourceParseMap
    initialStream.flatMap(new UmsFlatMapper(sourceSchemaMap.toMap, sourceNamespace, jsonSourceParseMap))(Types.ROW(sourceFieldNameArray, sourceFlinkTypeArray))
  }

  private def assignTimeCharacteristic(env: StreamExecutionEnvironment): Unit = {
    if (timeCharacteristic == SwiftsConstants.PROCESSING_TIME)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    else env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }


  private def assignTimestamp(inputStream: DataStream[Row], sourceSchemaMap: Map[String, (TypeInformation[_], Int)]) = {
    if (timeCharacteristic != SwiftsConstants.PROCESSING_TIME)
      inputStream.assignTimestampsAndWatermarks(new FlinkxTimestampExtractor(sourceSchemaMap))
    else inputStream
  }

  private def getSwiftsSql(swiftsString: String, dataType: String): Option[Array[SwiftsSql]] = {
    val action: String = if (swifts.containsKey("action") && swifts.getString("action").trim.nonEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(swifts.getString("action").trim)) else null
    if (null != action) {
      logger.info(s"action in getSwiftsSql $action")
      val parser = new ParseSwiftsSql(action, sourceNamespace, sinkNamespace)
      parser.registerConnections(swifts)
      parser.parse(dataType, sourceSchemaMap.keySet)
    } else None
  }


}
