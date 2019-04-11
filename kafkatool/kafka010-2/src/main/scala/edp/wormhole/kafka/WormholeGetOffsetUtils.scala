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

package edp.wormhole.kafka

import java.util
import java.util.Properties

import joptsimple.OptionParser
import kafka.admin.AdminClient
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse}
import kafka.common.TopicAndPartition
import kafka.network.BlockingChannel
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

object WormholeGetOffsetUtils {

  private val logger = Logger.getLogger(this.getClass)

  def getLatestOffset(brokerList: String, topic: String, kerberos: Boolean = false): String = {
    getTopicOffset(brokerList, topic, kerberos, -1)
  }

  def getEarliestOffset(brokerList: String, topic: String, kerberos: Boolean = false): String = {
    getTopicOffset(brokerList, topic, kerberos, -2)
  }

  def getTopicOffset(brokerList: String, topic: String, kerberos: Boolean = false, time: Long = -1, maxWaitMs: Int = 60000) = {
    try {
      val parser = new OptionParser
      ToolsUtils.validatePortOrDie(parser, brokerList)

      val props = new Properties()

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // key反序列化方式
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // value反系列化方式
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList) // 指定broker地址，来找到group的coordinator
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, maxWaitMs.toString)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "80000")

      if (kerberos) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        props.put("sasl.kerberos.service.name", "kafka")
      }

      val consumer = new KafkaConsumer[String, String](props)
      val offsetSeq = new ListBuffer[String]()
      val topicMap = consumer.listTopics()
      if (!topicMap.isEmpty && topicMap.containsKey(topic) && topicMap.get(topic) != null && topicMap.get(topic).size() > 0) {
        val it = topicMap.get(topic).iterator()
        while (it.hasNext) {
          val partition = it.next
          try {
            val topicAndPartition = new TopicPartition(topic, partition.partition())
            val map = time match {
              case -1 => consumer.endOffsets(util.Arrays.asList(topicAndPartition))
              case _ => consumer.beginningOffsets(util.Arrays.asList(topicAndPartition))
            }
            offsetSeq += partition.partition() + ":" + map.get(topicAndPartition)
          } catch {
            case e: Exception =>
              logger.info(e.printStackTrace())
              consumer.close()
              throw new Exception(s"brokerList $brokerList topic $topic partition ${partition.partition()} doesn't have a leader, please verify it.")
          }
        }
        consumer.close()
      }
      val offset = offsetPartitionSort(offsetSeq.toList)
      if (offset == "")
        throw new Exception(s"topic $topic maybe not exists, query latest/earliest offset result is '', please check it.")
      offset
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getConsumerOffset(brokers: String, groupId: String, topic: String, kerberos: Boolean): String = {
    val latestOffset = getTopicOffset(brokers, topic, kerberos, -1)
    getConsumerOffset(brokers, groupId, topic, latestOffset.split(",").length, kerberos)
  }

  def getConsumerOffset(brokers: String, groupId: String, topic: String, partitions: Int, kerberos: Boolean): String = {
    try {
      val props = new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
      if (kerberos) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        props.put("sasl.kerberos.service.name", "kafka")
      }
      val adminClient = AdminClient.create(props)

      val offsetMap: Map[TopicPartition, Long] = adminClient.listGroupOffsets(groupId)
      adminClient.close()

      Range(0, partitions).map(partition => {
        val topicPartition = new TopicPartition(topic, partition)
        if (offsetMap.contains(topicPartition))
          partition + ":" + offsetMap(topicPartition)
        else
          partition + ":"
      }).mkString(",")
    } catch {
      case ex: Exception =>
        logger.error(s"get consumer groupId $groupId for topic $topic offset failed", ex)
        Range(0, partitions).mkString(":,").concat(":")
    }
  }

  private def offsetPartitionSort(offset: String): String = {
    offsetPartitionSort(offset.split(",").toList)
  }

  private def offsetPartitionSort(partOffsetSeq: List[String]): String = {
    partOffsetSeq.sortBy(partOffset => partOffset.split(":")(0).toLong).mkString(",")
  }
}

