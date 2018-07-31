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

package edp.wormhole.sink

import edp.wormhole.common.util.JsonUtils.json2caseClass
import edp.wormhole.config.{KafkaConfig, SinkProcessConfig}
import edp.wormhole.swifts.SwiftsConfMemoryStorage
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.slf4j.LoggerFactory


class Data2KafkaSink {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def process(dataStream: DataStream[String], sinkNamespace: String, sinkProcessConfig: SinkProcessConfig): DataStreamSink[String] = {
    val kafkaProducer = getKafkaProducer(sinkNamespace, sinkProcessConfig)
    dataStream.addSink(kafkaProducer)
  }

  private def getKafkaProducer(sinkNamespace: String, sinkProcessConfig: SinkProcessConfig): FlinkKafkaProducer010[String] = {
    val sinkSpecificConfig = if (sinkProcessConfig.specialConfig.isDefined) {
      logger.info("sinkSpecificConfig " + sinkProcessConfig.specialConfig.get)
      json2caseClass[KafkaConfig](sinkProcessConfig.specialConfig.get)
    } else KafkaConfig(None, None, None, None)

    val kafkaTopic: String = if (sinkSpecificConfig.sinkKafkaTopic.nonEmpty && sinkSpecificConfig.sinkKafkaTopic.get.nonEmpty) {
      logger.info("kafkaTopic " + sinkSpecificConfig.sinkKafkaTopic.get)
      sinkSpecificConfig.sinkKafkaTopic.get
    }
    else sinkNamespace.split("\\.")(2)
    val connectionUrl = SwiftsConfMemoryStorage.getDataStoreConnections(sinkNamespace).connectionUrl
    logger.info(s"connectionUrl $connectionUrl")
    val kafkaProducer = new FlinkKafkaProducer010[String](
      connectionUrl,
      kafkaTopic,
      new SimpleStringSchema)
    kafkaProducer
  }


}
