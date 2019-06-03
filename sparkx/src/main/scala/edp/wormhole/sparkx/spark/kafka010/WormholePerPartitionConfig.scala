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


package edp.wormhole.sparkx.spark.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.PerPartitionConfig

import scala.collection.mutable

class WormholePerPartitionConfig(perPartition:mutable.HashMap[TopicPartition,Long]) extends PerPartitionConfig {
  def maxRatePerPartition(topicPartition: TopicPartition): Long = perPartition(topicPartition)

  def setPerPartitionRate(newPerPartition:Map[TopicPartition,Long]): Unit ={
    perPartition.clear()
    perPartition ++= newPerPartition
  }

  def getPerPartitionRate:mutable.HashMap[TopicPartition,Long]={
    perPartition
  }
}
