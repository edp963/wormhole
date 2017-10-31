package edp.wormhole.kafka

import joptsimple.OptionParser
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ToolsUtils

import scala.collection.mutable.ListBuffer

object WormholeGetOffsetShell {

  def getTopicOffsets(brokerList: String, topic: String, time: Long = -1, maxWaitMs: Int = 10000) = {
    try {
      val parser = new OptionParser
      val clientId = "GetOffsetShell"
      ToolsUtils.validatePortOrDie(parser, brokerList)
      val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
      val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
      if (topicsMetadata.size != 1 || !topicsMetadata(0).topic.equals(topic)) {
        throw new Exception(s"brokerList $brokerList topic $topic doesn't exist, please verify it.")
        ""
      } else {
        val partitions = topicsMetadata.head.partitionsMetadata.map(_.partitionId)
        val offsetSeq = new ListBuffer[String]()
        partitions.foreach { partitionId =>
          val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
          partitionMetadataOpt match {
            case Some(metadata) =>
              metadata.leader match {
                case Some(leader) =>
                  val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
                  try {
                    val topicAndPartition = TopicAndPartition(topic, partitionId)
                    val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, 1)))
                    val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
                    offsetSeq += partitionId + ":" + offsets.mkString("").trim
                  } catch {
                    case _: Exception =>
                      throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
                      ""
                  } finally {
                    consumer.close()
                  }
                case None =>
                  throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
                  ""
              }
            case None =>
              throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't exist, please verify it.")
              ""
          }
        }

        offsetSeq.reverse.mkString(",")
      }
    } catch {
      case ex: Exception =>
        throw new Exception(ex)
        ""
    }
  }
}

