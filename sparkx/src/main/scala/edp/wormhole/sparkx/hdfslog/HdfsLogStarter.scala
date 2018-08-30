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


package edp.wormhole.sparkx.hdfslog

import edp.wormhole.sparkx.common.SparkContextUtils.createKafkaStream
import edp.wormhole.common._
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.common.{KafkaInputConfig, SparkContextUtils, SparkUtils, WormholeConfig}
import edp.wormhole.sparkx.directive.DirectiveFlowWatch
import edp.wormhole.sparkx.memorystorage.OffsetPersistenceManager
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.util.JsonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsLogStarter extends App with EdpLogging { //todo set hdfslog metadata to 1 if kill application or die last time
  SparkContextUtils.setLoggerLevel()
  logInfo("hdfsLogConfig:" + args(0))
  val config: WormholeConfig = JsonUtils.json2caseClass[WormholeConfig](args(0))
  val appId = SparkUtils.getAppId
  WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)
  val sparkConf = new SparkConf()
    .setMaster(config.spark_config.master)
    .set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
    .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    .setAppName(config.spark_config.stream_name)
  val sparkContext = new SparkContext(sparkConf)
  val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(config.kafka_input.batch_duration_seconds))

  DirectiveFlowWatch.initFlow(config, appId)

  val kafkaInput: KafkaInputConfig = OffsetPersistenceManager.initOffset(config, appId)
  val kafkaStream = createKafkaStream(ssc, kafkaInput)
  HdfsMainProcess.process(kafkaStream, config)

  SparkContextUtils.checkSparkRestart(config.zookeeper_path, config.spark_config.stream_id, appId)
  SparkContextUtils.deleteZookeeperOldAppidPath(appId, config.zookeeper_path, config.spark_config.stream_id)
  WormholeZkClient.createPath(config.zookeeper_path, WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + "/" + appId)

  logInfo("all init finish,to start spark streaming")
  SparkContextUtils.startSparkStreaming(ssc)

}
