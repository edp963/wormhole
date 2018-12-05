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

import java.util

import edp.rider.common.RiderLogger
import edp.wormhole.kafka.WormholeGetOffsetShell
import edp.wormhole.kafka.WormholeGetOffsetShell.logger
import joptsimple.OptionParser
import kafka.client.ClientUtils
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


object KafkaUtils extends RiderLogger {

  def getKafkaLatestOffset(brokers: String, topic: String,kerberos:Boolean): String = {
    try {
      val offset = WormholeGetOffsetShell.getTopicOffsets(brokers, topic,kerberos)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        throw ex
    }
  }

  def getKafkaLatestOffset(brokers: String, topic: String, partition: Int,kerberos:Boolean): String = {
    try {
      val offsets = WormholeGetOffsetShell.getTopicOffsets(brokers, topic,kerberos)
      val offset = offsets.split(",")(partition).split(":")(1)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka latest offset failed", ex)
        throw ex
    }
  }

  def getKafkaEarliestOffset(brokers: String, topic: String ,kerberos: Boolean): String = {
    try {
      val offset = WormholeGetOffsetShell.getTopicOffsets(brokers, topic, kerberos, -2)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka earliest offset failed", ex)
        throw ex
    }
  }

  def getKafkaEarliestOffsetOnActor(brokers: String, topic: String ,consumer:KafkaConsumer[String,String]): String = {
    try {
      val offset =getTopicOffsetsOnActor(brokers, topic, consumer, -2)
      if (offsetValid(offset)) offset
      else throw new Exception(s"query topic $topic offset result is '', please check it.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get kafka earliest offset failed", ex)
        throw ex
    }
  }

  def getTopicOffsetsOnActor(brokerList: String, topic: String, consumer:KafkaConsumer[String,String], time: Long = -1, maxWaitMs: Int = 60000)={
    try {
      val parser = new OptionParser
      ToolsUtils.validatePortOrDie(parser, brokerList)

        val topicMap=consumer.listTopics()
        val offsetSeq = new ListBuffer[String]()

        if(!topicMap.isEmpty&&topicMap.containsKey(topic)&&topicMap.get(topic)!=null&&topicMap.get(topic).size()>0){
          val it=topicMap.get(topic).iterator()

          while(it.hasNext){
            val partition=it.next

            for(partitionId <- 0 to partition.partition){
              try {
                val topicAndPartition=new TopicPartition(topic,partitionId)
                val map=time match {
                  case -1 =>consumer.endOffsets(util.Arrays.asList(topicAndPartition))
                  case _ =>consumer.beginningOffsets(util.Arrays.asList(topicAndPartition))
                }

                if(!offsetSeq.contains(partitionId + ":" + map.get(topicAndPartition)))
                   offsetSeq += partitionId + ":" + map.get(topicAndPartition)
              } catch {
                case e: Exception =>
                  consumer.close()
                  throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
              }
            }

          }
        }
        val offset = offsetSeq.sortBy(offset => offset.split(":")(0).toLong).mkString(",")
        if (offset == "")
          throw new Exception(s"query topic $topic offset result is '', please check it.")
        offset
    }catch {
      case ex: Exception =>
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
