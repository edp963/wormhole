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


package edp.wormhole.batchflow

import edp.wormhole.common.SparkContextUtils.createKafkaStream
import edp.wormhole.common.util.JsonUtils
import edp.wormhole.common.zookeeper.WormholeZkClient
import edp.wormhole.common.{SparkContextUtils, SparkUtils, WormholeConstants}
import edp.wormhole.core.{DirectiveFlowWatch, KafkaInputConfig, WormholeConfig}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.memorystorage.OffsetPersistenceManager
import edp.wormhole.spark.log.EdpLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import edp.wormhole.udf.UdfWatch

object BatchflowStarter extends App with EdpLogging {
  SparkContextUtils.setLoggerLevel()

  logInfo("swiftsConfig:" + args(0))
  val config: WormholeConfig = JsonUtils.json2caseClass[WormholeConfig](args(0))
  val appId = SparkUtils.getAppId
  WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)

  val sparkConf = new SparkConf()
    .setMaster(config.spark_config.master)
    .set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
    .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    .set("spark.sql.shuffle.partitions", config.spark_config.`spark.sql.shuffle.partitions`.toString)
    .set(if (SparkUtils.isLocalMode(config.spark_config.master)) "spark.sql.warehouse.dir" else "",
      if (SparkUtils.isLocalMode(config.spark_config.master)) "file:///" else "")
    .setAppName(config.spark_config.stream_name)
  val sparkContext = new SparkContext(sparkConf)
  val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(config.kafka_input.batch_duration_seconds))

  UdfWatch.initUdf(config, appId,session)

//  if (config.udf.isDefined) {
//    import collection.JavaConversions._
//    new UdfRegister().udfRegister(config.udf.get, session.sqlContext)
//  }

  DirectiveFlowWatch.initFlow(config, appId)

  val kafkaInput: KafkaInputConfig = OffsetPersistenceManager.initOffset(config, appId)
  val kafkaStream = createKafkaStream(ssc, kafkaInput)
  BatchflowMainProcess.process(kafkaStream, config, session)

  SparkContextUtils.checkSparkRestart(config.zookeeper_path, config.spark_config.stream_id, appId)
  SparkContextUtils.deleteZookeeperOldAppidPath(appId, config.zookeeper_path, config.spark_config.stream_id)
  WormholeZkClient.createPath(config.zookeeper_path, WormholeConstants.CheckpointRootPath + config.spark_config.stream_id + "/" + appId)

  logInfo("all init finish,to start spark streaming")
  SparkContextUtils.startSparkStreaming(ssc)


}
