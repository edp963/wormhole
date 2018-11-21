package edp.wormhole.kafka

import java.util
import java.util.Properties

import edp.wormhole.kafka.WormholeGetOffsetShell.logger
import joptsimple.OptionParser
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

object WormholeGetOffsetShell {

  def getTopicOffsets(brokerList: String, topic: String, kerberos:Boolean=false,time: Long = -1, maxWaitMs: Int = 30000)={
    try {
      val parser = new OptionParser
      ToolsUtils.validatePortOrDie(parser, brokerList)
      val props=new Properties()

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // key反序列化方式
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // value反系列化方式
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList) // 指定broker地址，来找到group的coordinator
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,maxWaitMs.toString)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
      if(kerberos){
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT")
        props.put("sasl.kerberos.service.name", "kafka")
      }

      val consumer = new KafkaConsumer[String, String](props)
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

              offsetSeq += partitionId + ":" + map.get(topicAndPartition)
            } catch {
              case e: Exception =>
                logger.info(e.printStackTrace())
                throw new Exception(s"brokerList $brokerList topic $topic partition $partitionId doesn't have a leader, please verify it.")
            } finally {
              consumer.close()
            }
          }
        }
      }
      val offset = offsetSeq.sortBy(offset => offset.split(":")(0).toLong).mkString(",")
      if (offset == "")
        throw new Exception(s"query topic $topic offset result is '', please check it.")
      logger.info(s"offset is:$offset")
      offset
    }catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getConsumerOffset(broker:String,groupid:String): Map[String, String] ={
    throw new Exception("not support with kafka version 0.10.0.* or 0.10.1.*")
  }
}

