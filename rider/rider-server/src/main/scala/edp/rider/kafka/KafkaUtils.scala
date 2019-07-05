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

package edp.rider.kafka

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.wormhole.kafka.WormholeTopicCommand

import scala.language.postfixOps


object KafkaUtils extends RiderLogger {

  def createRiderKafkaTopic(): Unit = {

    if (!RiderConfig.kerberos.kafkaEnabled) {
      try {
        WormholeTopicCommand.createOrAlterTopic(RiderConfig.consumer.zkUrl, RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.partitions, RiderConfig.consumer.refactor)
        riderLogger.info(s"initial create ${RiderConfig.consumer.feedbackTopic} topic success")
      } catch {
        case _: Exception =>
          riderLogger.warn(s"initial create ${RiderConfig.consumer.feedbackTopic} topic failed, " +
            s"please check the ${RiderConfig.consumer.feedbackTopic} topic does exist, " +
            s"if doesn't, please create it manually with 1 partition")
      }
      try {
        WormholeTopicCommand.createOrAlterTopic(RiderConfig.consumer.zkUrl, RiderConfig.spark.wormholeHeartBeatTopic, 1, RiderConfig.consumer.refactor)
        riderLogger.info(s"initial create ${RiderConfig.spark.wormholeHeartBeatTopic} topic success")
      } catch {
        case _: Exception =>
          riderLogger.warn(s"initial create ${RiderConfig.spark.wormholeHeartBeatTopic} topic failed, " +
            s"please check the ${RiderConfig.spark.wormholeHeartBeatTopic} topic does exist, " +
            s"if doesn't, please create it manually with 1 partition")
      }
    }
  }

  def getPartNumByOffset(offset: String): Int = offset.split(",").length
}
