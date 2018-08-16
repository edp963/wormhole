package edp.wormhole.kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.admin.AdminClient
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object WormholeGetOffsetShell {

  def getTopicOffsets(brokerList: String, topic: String, time: Long = -1, maxWaitMs: Int = 30000) = {
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
          throw new Exception(s"query topic $topic offset result is '', please check it.")
        offset
      }
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getConsumerOffset(broker:String,groupid:String): Map[String, String] ={
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker)
    val adminClient = AdminClient.create(props)

    val offsetMap: Map[TopicPartition, Long] = adminClient.listGroupOffsets(groupid)
    val r = if(offsetMap==null||offsetMap.isEmpty) null.asInstanceOf[Map[String, String]]
    else{
      val map = mutable.HashMap.empty[String,ListBuffer[(Int,Long)]]
      offsetMap.foreach{case(k,v)=>
          if(map.contains(k.topic())){
            map(k.topic) += ((k.partition(),v))
          }else{
            val list = ListBuffer.empty[(Int,Long)]
            list += ((k.partition(),v))
            map(k.topic) = list
          }
      }

      val tpMap = mutable.HashMap.empty[String,String]
      map.foreach{case (k,v)=>
        tpMap(k)=v.sortBy(_._1).map(ele=>{
          s"${ele._1}:${ele._2}"
        }).mkString(",")
      }
      tpMap.toMap
    }
    adminClient.close()
    r
  }
}

