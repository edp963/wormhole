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


package edp.wormhole.sparkx.batchflow

import edp.wormhole.common._
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.common.SparkContextUtils.createKafkaStream
import edp.wormhole.sparkx.common.{SparkContextUtils, SparkUtils}
import edp.wormhole.sparkx.directive.{DirectiveFlowWatch, UdfWatch}
import edp.wormhole.sparkx.memorystorage.OffsetPersistenceManager
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{KafkaInputConfig, WormholeConfig}
import edp.wormhole.util.JsonUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BatchflowStarter extends App with EdpLogging {
  SparkContextUtils.setLoggerLevel()

  logInfo("swiftsConfig:" + args(0))
  val config: WormholeConfig = JsonUtils.json2caseClass[WormholeConfig](args(0))
  val appId = SparkUtils.getAppId
  WormholeKafkaProducer.initWithoutAcksAll(config.kafka_output.brokers, config.kafka_output.config,config.kafka_output.kerberos)
  val sparkConf = new SparkConf()
    .setMaster(config.spark_config.master)
    .set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
    .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    .set("spark.streaming.stopGracefullyOnShutdown","true")
    .set("spark.sql.shuffle.partitions", config.spark_config.`spark.sql.shuffle.partitions`.toString)
    .set("spark.debug.maxToStringFields", 500.toString)
    .set(if (SparkUtils.isLocalMode(config.spark_config.master)) "spark.sql.warehouse.dir" else "",
      if (SparkUtils.isLocalMode(config.spark_config.master)) "file:///" else "")
    .setAppName(config.spark_config.stream_name)
  val sparkContext = new SparkContext(sparkConf)
  val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(config.kafka_input.batch_duration_seconds))

  UdfWatch.initUdf(config, appId,session)

  DirectiveFlowWatch.initFlow(config, appId)

  val kafkaInput: KafkaInputConfig = OffsetPersistenceManager.initOffset(config, appId)
  val kafkaStream = createKafkaStream(ssc, kafkaInput)
  BatchflowMainProcess.process(kafkaStream, config,kafkaInput, session, appId,ssc)

  logInfo("all init finish,to start spark streaming")
  SparkContextUtils.startSparkStreaming(ssc)


}
