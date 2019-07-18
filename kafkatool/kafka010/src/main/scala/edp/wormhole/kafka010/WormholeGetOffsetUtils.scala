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

package edp.wormhole.kafka010

import java.util.Properties

import joptsimple.OptionParser
import kafka.admin.AdminClient
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.network.BlockingChannel
import kafka.utils.ToolsUtils
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.RequestFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{GroupCoordinatorRequest, GroupCoordinatorResponse}
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

  def getTopicOffset(brokerList: String, topic: String, kerberos: Boolean = false, time: Long = -1, maxWaitMs: Int = 30000) = {
    try {
      val parser = new OptionParser
      val clientId = "GetOffsetShell"
      ToolsUtils.validatePortOrDie(parser, brokerList)
      val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
      val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
      if (topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
        throw new Exception(s"brokerList $brokerList topic $topic doesn't exist, please verify it.")
      } else {
        val partitions = topicsMetadata.head.partitionsMetadata.map(_.partitionId)
        val offsetSeq = new ListBuffer[String]()
        partitions.foreach { partitionId =>
          val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
          partitionMetadataOpt match {
            case Some(metadata) =>
              metadata.leader match {
                case Some(leader) =>
                  val consumer = new SimpleConsumer(leader.host, leader.port, maxWaitMs, 100000, clientId)
                  try {
                    val topicAndPartition = TopicAndPartition(topic, partitionId)
                    val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, 1)))
                    val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
                    offsetSeq += partitionId + ":" + offsets.mkString("").trim
                  } catch {
                    case _: Exception =>
                      throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
                  } finally {
                    consumer.close()
                  }
                case None =>
                  throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
              }
            case None =>
              throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't exist, please verify it.")
          }
        }
        val offset = offsetPartitionSort(offsetSeq.toList)
        if (offset == "")
          throw new Exception(s"topic $topic maybe not exists, query latest/earliest offset result is '', please check it.")
        offset
      }
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
    val topicPartitions = ListBuffer.empty[TopicAndPartition]
    Range(0, partitions).foreach(part => topicPartitions.append(TopicAndPartition(topic, part)))
    val coordinatorNode = findCoordinator(brokers, groupId, kerberos)
    if (coordinatorNode != null) {
      val brokerHost = coordinatorNode.host()
      val brokerPort = coordinatorNode.port()
      val channel = new BlockingChannel(brokerHost,
        brokerPort,
        BlockingChannel.UseDefaultBufferSize,
        BlockingChannel.UseDefaultBufferSize,
        60000)
      val fetchRequest = OffsetFetchRequest(groupId, topicPartitions)
      channel.connect()
      try {
        channel.send(fetchRequest)
        val fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())
        channel.disconnect()
        val result = fetchResponse.requestInfo
        val errorInfo = result.values.toList
        val offset = if (result.isEmpty || errorInfo.exists(_.error != 0)) {
          logger.error(s"get consumer groupId $groupId for topic $topic offset failed, $errorInfo")
          Range(0, partitions).mkString(":,").concat(":")
        } else {
          result.keySet.map(topicAndPartition =>
            if (result(topicAndPartition).offset != -1)
              topicAndPartition.partition + ":" + result(topicAndPartition).offset
            else
              topicAndPartition.partition + ":"
          ).mkString(",")
        }
        offsetPartitionSort(offset)
      } catch {
        case ex: Exception =>
          logger.error(s"get consumer groupId $groupId for topic $topic offset failed", ex)
          channel.disconnect()
          Range(0, partitions).mkString(":,").concat(":")
      }
    } else {
      logger.error(s"get consumer groupId $groupId for topic $topic offset failed")
      Range(0, partitions).mkString(":,").concat(":")
    }
  }

  private def offsetPartitionSort(offset: String): String = {
    offsetPartitionSort(offset.split(",").toList)
  }

  private def offsetPartitionSort(partOffsetSeq: List[String]): String = {
    partOffsetSeq.sortBy(partOffset => partOffset.split(":")(0).toLong).mkString(",")
  }

  private def findCoordinator(brokers: String, groupId: String, kerberos: Boolean = false): Node = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
    if (kerberos) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
      props.put("sasl.kerberos.service.name", "kafka")
    }
    val adminClient = AdminClient.create(props)
    val requestBuilder = new GroupCoordinatorRequest(groupId)
    var response: GroupCoordinatorResponse = null
    adminClient.bootstrapBrokers.foreach { broker =>
      try {
        var future: RequestFuture[ClientResponse] = null
        future = adminClient.client.send(broker, ApiKeys.GROUP_COORDINATOR, requestBuilder)
        adminClient.client.poll(future)
        if (future.succeeded()) {
          response = new GroupCoordinatorResponse(future.value().responseBody())
        }
      } catch {
        case e: Exception =>
          logger.error(s"get consumer group $groupId coordinator failed against node $broker", e)
      }
    }
    if (response != null) response.node() else null
  }
}

