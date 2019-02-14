package edp.wormhole.kafka

import joptsimple.OptionParser
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.network.BlockingChannel
import kafka.utils.ToolsUtils
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
        val offset = offsetSeq.sortBy(offset => offset.split(":")(0).toLong).mkString(",")
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
    val fetchRequest = OffsetFetchRequest(groupId, topicPartitions)
    val brokerSplit = brokers.split(",")(0).split(":")
    val brokerHost = brokerSplit(0).trim
    val brokerPort = brokerSplit(1).trim.toInt
    val channel = new BlockingChannel(brokerHost,
      brokerPort,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      60000)
    try {
      channel.connect()
      channel.send(fetchRequest)
      val fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())
      channel.disconnect()
      val result = fetchResponse.requestInfo
      val errorInfo = result.values.toList
      val offset = if (errorInfo.exists(_.error != 0)) {
        Range(0, partitions).mkString(":,").concat(":")
      } else {
        result.keySet.map(topicAndPartition =>
          if (result(topicAndPartition).offset != -1)
            topicAndPartition.partition + ":" + result(topicAndPartition).offset
          else
            topicAndPartition.partition + ":"
        ).mkString(",")
      }
      offset
    } catch {
      case ex: Exception =>
        logger.error(s"get consumer groupId $groupId for topic $topic offset failed", ex)
        channel.disconnect()
        Range(0, partitions).mkString(":,").concat(":")
    }
  }


}

