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


package edp.wormhole.sinks.kafkasink

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.WormholeUms._
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums._
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

class Data2KafkaSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String,
                       sinkNamespaceOrg: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val sinkNamespaceSeq = sinkNamespaceOrg.split("\\.")
    val sourceNamespaceSeq = sourceNamespace.split("\\.")
    val sinkNamespace = s"${sinkNamespaceSeq(0)}.${sinkNamespaceSeq(1)}.${sinkNamespaceSeq(2)}.${sinkNamespaceSeq(3)}.${sourceNamespaceSeq(4)}.${sourceNamespaceSeq(5)}.${sourceNamespaceSeq(6)}"

    if(tupleList.nonEmpty) {
      logger.info(s"In Data2KafkaSink ${tupleList.head}, size is ${tupleList.size}")
    }
    logger.info("Data2KafkaSink sink config: " + sinkProcessConfig)
    val sinkSpecificConfig = if (sinkProcessConfig.specialConfig.isDefined) JsonUtils.json2caseClass[KafkaConfig](sinkProcessConfig.specialConfig.get) else KafkaConfig(None, None, None, None, None)
    WormholeKafkaProducer.init(connectionConfig.connectionUrl, connectionConfig.parameters, sinkSpecificConfig.kerberos.getOrElse(false))

    val schemaList: Seq[(String, (Int, UmsFieldType, Boolean))] = schemaMap.toSeq.sortBy(_._2._1)
    val protocol: UmsProtocol =
      if (sinkSpecificConfig.topic.nonEmpty && sinkSpecificConfig.topic.get.nonEmpty)
        UmsProtocol(UmsProtocolType.DATA_BATCH_DATA)
      else
        UmsProtocol(UmsProtocolType.DATA_INCREMENT_DATA)

    //for job of feedback
    val kafkaTopic = if (sinkSpecificConfig.topic.nonEmpty && sinkSpecificConfig.topic.get.nonEmpty) sinkSpecificConfig.topic.get else sinkNamespace.split("\\.")(2)
    logger.info("sink topic: " + kafkaTopic + " sink namespace: " + sinkNamespace)
    logger.info("sink protocol: " + protocol.`type`.toString)

    val format = sinkSpecificConfig.messageFormat.trim
    format match {
      case "ums" =>
        val seqUmsField: Seq[UmsField] = schemaList.map(kv => UmsField(kv._1, kv._2._2, Some(kv._2._3)))
        val schema = UmsSchema(sinkNamespace, Some(seqUmsField))
        val kafkaLimitNum = sinkSpecificConfig.limitNum
        ums2Kafka(tupleList, kafkaLimitNum, protocol, schema, protocol.`type`, sinkNamespace, kafkaTopic, connectionConfig)
      case "flattenJson" =>
        val hasSystemField = sinkSpecificConfig.hasSystemField
        if (hasSystemField) flattenJson2KafkaWithSystemValue(tupleList, schemaList, sinkNamespace, kafkaTopic, connectionConfig, protocol.`type`.toString)
        else flattenJson2KafkaWithoutSystemValue(tupleList, schemaList, sinkNamespace, kafkaTopic, connectionConfig, protocol.`type`.toString)
      case "userDefinedJson" =>
        logger.error("not support yet")
      case _ =>
        logger.error("cannot recognize " + format)
    }
  }

  private def flattenJson2KafkaWithSystemValue(tupleList: Seq[Seq[String]], schemaList: Seq[(String, (Int, UmsFieldType, Boolean))], sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig, protocol: String): Unit = {
    tupleList.foreach(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        val umsFieldType = schemaList(index)._2._2
        if (umsFieldType == DATETIME || umsFieldType == DATE)
          flattenJson.put(schemaList(index)._1, t)
        else flattenJson.put(schemaList(index)._1, UmsFieldType.umsFieldValue(t, schemaList(index)._2._2))
        index += 1
      })
      flattenJson.put("namespace", sinkNamespace)
      flattenJson.put("protocol", protocol)
      WormholeKafkaProducer.sendMessage(kafkaTopic, JSON.toJSONString(flattenJson, SerializerFeature.WriteMapNullValue), Some(protocol + "." + sinkNamespace + "..." + UUID.randomUUID().toString), connectionConfig.connectionUrl)
    }
    )
  }


  private def flattenJson2KafkaWithoutSystemValue(tupleList: Seq[Seq[String]], schemaList: Seq[(String, (Int, UmsFieldType, Boolean))], sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig, protocol: String): Unit = {
    tupleList.foreach(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        if (!schemaList(index)._1.startsWith("ums_")) {
          val umsFieldType = schemaList(index)._2._2
          if (umsFieldType == DATETIME || umsFieldType == DATE)
            flattenJson.put(schemaList(index)._1, t)
          else flattenJson.put(schemaList(index)._1, UmsFieldType.umsFieldValue(t, schemaList(index)._2._2))
        }
        index += 1
      })
      WormholeKafkaProducer.sendMessage(kafkaTopic, JSON.toJSONString(flattenJson, SerializerFeature.WriteMapNullValue), Some(protocol + "." + sinkNamespace + "..." + UUID.randomUUID().toString), connectionConfig.connectionUrl)
    }
    )
  }


  private def ums2Kafka(tupleList: Seq[Seq[String]], kafkaLimitNum: Int, protocol: UmsProtocol, schema: UmsSchema, protocolType: UmsProtocolType, sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig): Unit = {
    logger.info(s"start write to kafka, tupleList size is: ${tupleList.size}")
    tupleList.sliding(kafkaLimitNum, kafkaLimitNum).foreach(tuple => {
      val seqUmsTuple: Seq[UmsTuple] = tuple.map(payload => UmsTuple(payload))
      //logger.info(s"start write to kafka, seqUmsTuple size is: ${seqUmsTuple.size}")
      val kafkaMessage: String = toJsonCompact(Ums(
        protocol,
        schema,
        payload = Some(seqUmsTuple)))
      WormholeKafkaProducer.sendMessage(kafkaTopic, kafkaMessage, Some(protocolType + "." + sinkNamespace + "..." + UUID.randomUUID().toString), connectionConfig.connectionUrl)
    })
  }
}
