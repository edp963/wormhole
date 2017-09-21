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
                       connectionConfig:ConnectionConfig): Unit = {
    logInfo("In Data2KafkaSink")
    WormholeKafkaProducer.init(connectionConfig.connectionUrl,connectionConfig.parameters)
    val sinkSpecificConfig=json2caseClass[KafkaConfig](sinkProcessConfig.specialConfig.get)
    val kafkaTopic=sinkNamespace.split("\\.")(2)
    val kafkaLimitNum=sinkSpecificConfig.limitNum
    val schemaList: Seq[(String, (Int, UmsFieldType, Boolean))] =schemaMap.toSeq.sortBy(_._2._1)
    val seqUmsField: Seq[UmsField] =schemaList.map(kv=>UmsField(kv._1,kv._2._2,Some(kv._2._3)))
    val protocol = UmsProtocol(protocolType)
    val schema = UmsSchema(sinkNamespace, Some(seqUmsField))
    tupleList.sliding(kafkaLimitNum,kafkaLimitNum).foreach(tuple=>{
      val seqUmsTuple: Seq[UmsTuple] =tuple.map(payload=>UmsTuple(payload))
      val kafkaMessage: String =toJsonCompact(Ums(
        protocol,
        schema,
        payload = Some(seqUmsTuple)))
      WormholeKafkaProducer.sendMessage(kafkaTopic,kafkaMessage, Some(protocolType + "." + sinkNamespace),connectionConfig.connectionUrl)
    })

  }
}
