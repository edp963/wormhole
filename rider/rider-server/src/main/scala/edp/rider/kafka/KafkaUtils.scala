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

import edp.rider.common.RiderLogger
import edp.wormhole.kafka.WormholeGetOffsetShell

import scala.language.postfixOps


object KafkaUtils extends RiderLogger {

  def getKafkaLatestOffset(brokers: String, topic: String): String = {
    try {
      val offset = WormholeGetOffsetShell.getTopicOffsets(brokers, topic)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        throw ex
    }
  }

  def getKafkaLatestOffset(brokers: String, topic: String, partition: Int): String = {
    try {
      val offsets = WormholeGetOffsetShell.getTopicOffsets(brokers, topic)
      val offset = offsets.split(",")(partition).split(":")(1)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        throw ex
    }
  }

  def getKafkaEarliestOffset(brokers: String, topic: String): String = {
    try {
      val offset = WormholeGetOffsetShell.getTopicOffsets(brokers, topic, -2)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka earliest offset failed", ex)
        throw ex
    }
  }

  def offsetValid(offset: String): Boolean = {
    if (offset == "") false else true
  }

  def getKafkaOffsetByGroupId(brokers: String, topic: String, groupId: String): String = {
    try {
      val groupOffset = WormholeGetOffsetShell.getConsumerOffset(brokers, groupId)
      groupOffset(topic)
    } catch {
      case ex: Exception =>
        riderLogger.warn(s"get group id $groupId consumed offset failed", ex)
        throw ex
    }
  }

  def formatConsumedOffsetByLatestOffset(consumedOffset: String, latestOffset: String): String = {
    val consumedPartition = consumedOffset.split(",").size
    val currentPartition = latestOffset.split(",").size
    if (consumedPartition == currentPartition) {
      consumedOffset
    } else if (consumedPartition > currentPartition) {
      consumedOffset.split(",").slice(0, currentPartition).mkString(",")
    } else {
      consumedOffset + "," + (consumedPartition until currentPartition).map(part => s"$part:0").mkString(",")
    }
  }

}
