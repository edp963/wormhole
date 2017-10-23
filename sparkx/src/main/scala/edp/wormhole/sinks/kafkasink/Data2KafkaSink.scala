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

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.WormholeUms._
import edp.wormhole.common.util.JsonUtils._
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums._
import edp.wormhole.ums.UmsFieldType._

class Data2KafkaSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logInfo("In Data2KafkaSink")
    WormholeKafkaProducer.init(connectionConfig.connectionUrl, connectionConfig.parameters)
    val sinkSpecificConfig = json2caseClass[KafkaConfig](sinkProcessConfig.specialConfig.get)
    val kafkaTopic = sinkNamespace.split("\\.")(2)
    val schemaList: Seq[(String, (Int, UmsFieldType, Boolean))] = schemaMap.toSeq.sortBy(_._2._1)
    val protocol: UmsProtocol = UmsProtocol(protocolType)
    val format = sinkSpecificConfig.messageFormat.trim
    format match {
      case "ums" =>
        val seqUmsField: Seq[UmsField] = schemaList.map(kv => UmsField(kv._1, kv._2._2, Some(kv._2._3)))
        val schema = UmsSchema(sinkNamespace, Some(seqUmsField))
        val kafkaLimitNum = sinkSpecificConfig.limitNum
        ums2Kafka(tupleList, kafkaLimitNum, protocol, schema, protocolType, sinkNamespace, kafkaTopic, connectionConfig)
      case "flattenJson" =>
        val hasSystemField = sinkSpecificConfig.hasSystemField
        if (hasSystemField) flattenJson2KafkaWithSystemValue(tupleList, schemaList, sinkNamespace, kafkaTopic, connectionConfig,protocol.`type`.toString)
        else flattenJson2KafkaWithoutSystemValue(tupleList, schemaList, sinkNamespace, kafkaTopic, connectionConfig)
      case "userDefinedJson" =>
        logError("not support yet")
      case _ =>
        logError("cannot recognize " + format)
    }
  }

  private def flattenJson2KafkaWithSystemValue(tupleList: Seq[Seq[String]], schemaList: Seq[(String, (Int, UmsFieldType, Boolean))], sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig,protocol:String): Unit = {
    tupleList.foreach(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        flattenJson.put(schemaList(index)._1, UmsFieldType.umsFieldValue(t, schemaList(index)._2._2))
        index += 1
      })
      flattenJson.put("namespace", sinkNamespace)
      flattenJson.put("protocol", protocol)
      WormholeKafkaProducer.sendMessage(kafkaTopic, flattenJson.toJSONString, None, connectionConfig.connectionUrl)
    }
    )
  }


  private def flattenJson2KafkaWithoutSystemValue(tupleList: Seq[Seq[String]], schemaList: Seq[(String, (Int, UmsFieldType, Boolean))], sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig): Unit = {
    tupleList.foreach(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        if (!schemaList(index)._1.startsWith("ums_")) {
          flattenJson.put(schemaList(index)._1,UmsFieldType.umsFieldValue(t,  schemaList(index)._2._2))
        }
        index += 1
      })
      WormholeKafkaProducer.sendMessage(kafkaTopic, flattenJson.toJSONString, None, connectionConfig.connectionUrl)
    }
    )
  }


  private def ums2Kafka(tupleList: Seq[Seq[String]], kafkaLimitNum: Int, protocol: UmsProtocol, schema: UmsSchema, protocolType: UmsProtocolType, sinkNamespace: String, kafkaTopic: String, connectionConfig: ConnectionConfig): Unit = {
    tupleList.sliding(kafkaLimitNum, kafkaLimitNum).foreach(tuple => {
      val seqUmsTuple: Seq[UmsTuple] = tuple.map(payload => UmsTuple(payload))
      val kafkaMessage: String = toJsonCompact(Ums(
        protocol,
        schema,
        payload = Some(seqUmsTuple)))
      WormholeKafkaProducer.sendMessage(kafkaTopic, kafkaMessage, Some(protocolType + "." + sinkNamespace), connectionConfig.connectionUrl)
    })
  }
}
