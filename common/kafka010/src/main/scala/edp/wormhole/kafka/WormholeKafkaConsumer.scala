package edp.wormhole.kafka

import java.util.Properties

import edp.wormhole.common.KVConfig
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object WormholeKafkaConsumer {

  private def getConsumerProps: Properties = {
    val props = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("compression.type", "lz4")
    props
  }

  def initConsumer(brokers: String, groupid: String, kvConfig: Option[Seq[KVConfig]]): KafkaConsumer[String,String] = {

    val props = getConsumerProps
    if (kvConfig.nonEmpty) {
      kvConfig.get.foreach(kv => {
        props.put(kv.key, kv.value)
      })
    }
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupid)
    new KafkaConsumer(props)
  }

  def consumerRecords(consumer: KafkaConsumer[String,String],timeout:Long): ConsumerRecords[String,String] ={
    consumer.poll(10000)
  }

  def close(consumer: KafkaConsumer[String,String]): Unit ={
    consumer.close()
  }

  def subscribeTopicFromBeginning(consumer: KafkaConsumer[String,String], topicPartitionCount:Map[String,Int]): Unit = {
    consumer.subscribe(topicPartitionCount.map(_._1).toList)
    val tpList = getAllTopicPartition(topicPartitionCount)
    consumer.seekToBeginning(tpList)
  }

  def subscribeTopicFromEnd(consumer: KafkaConsumer[String,String], topicPartitionCount:Map[String,Int]): Unit = {
    consumer.subscribe(topicPartitionCount.map(_._1).toList)
    val tpList = getAllTopicPartition(topicPartitionCount)
    consumer.seekToEnd(tpList)
  }

  def subscribeTopicFromOffset(consumer: KafkaConsumer[String,String], topicPartitions: Map[String,Seq[(Int, Long)]]): Unit = {
    consumer.subscribe(topicPartitions.map(_._1))
    getAllTopicPartitionOffset(topicPartitions).foreach(tp=>{
      consumer.seek(tp._1,tp._2)
    })

  }

  private def getAllTopicPartition(topicPartitionCount:Map[String,Int]): Seq[TopicPartition] ={
    topicPartitionCount.foldLeft(ListBuffer.empty[TopicPartition])((tps,tpNum)=>tps ++= getTopicPartition(tpNum._1,tpNum._2))
  }

  private def getAllTopicPartitionOffset(topicPartitions: Map[String,Seq[(Int, Long)]]): Map[TopicPartition,Long] ={
    topicPartitions.flatMap{case (topicName,partitionOffsetList)=>{
      partitionOffsetList.map{case(partition,offset)=>{
        (new TopicPartition(topicName, partition),offset)
      }}
    }}
  }

  private def getTopicPartition(topicName:String,partitionCount:Int): Seq[TopicPartition] ={
    (0 to (partitionCount-1)).map(partitionNum=>{
      new TopicPartition(topicName, partitionNum)
    })
  }

}
