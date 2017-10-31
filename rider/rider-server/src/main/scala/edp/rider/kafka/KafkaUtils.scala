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

case class GetKafkaLatestOffsetException(message: String) extends Exception(message)

object KafkaUtils extends RiderLogger {

  def getKafkaLatestOffset(brokers: String, topic: String): String = {
    try {
      WormholeGetOffsetShell.getTopicOffsets(brokers, topic)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        ""
    }
  }

  def getKafkaLatestOffset(brokers: String, topic: String, partition: Int): String = {
    try {
      val offsets = WormholeGetOffsetShell.getTopicOffsets(brokers, topic)
      offsets.split(",")(partition).split(":")(1)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        ""
    }
  }

}
