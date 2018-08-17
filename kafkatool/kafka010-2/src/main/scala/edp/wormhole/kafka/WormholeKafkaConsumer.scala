package edp.wormhole.kafka

import java.util
import java.util.Properties

import edp.wormhole.util.config.KVConfig
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object WormholeKafkaConsumer {

  def initConsumer(brokers: String, groupid: String, kvConfig: Option[Seq[KVConfig]]): KafkaConsumer[String, String] = {

    val props = new Properties()
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("acks", "all")
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupid)

    if (kvConfig.nonEmpty) {
      kvConfig.get.foreach(kv => {
        props.put(kv.key, kv.value)
      })
    }

    new KafkaConsumer[String, String](props)
  }

  def consumerRecords(consumer: KafkaConsumer[String, String], timeout: Long): ConsumerRecords[String, String] = {
    consumer.poll(timeout)
  }

  def close(consumer: KafkaConsumer[String, String]): Unit = {
    consumer.close()
  }

  def subscribeTopicFromBeginning(consumer: KafkaConsumer[String, String], topicPartitionCount: Map[String, Int]): Unit = {
    consumer.subscribe(topicPartitionCount.keys)
//    consumer.poll(0)
    val tpList = getAllTopicPartition(topicPartitionCount)
    consumer.seekToBeginning(tpList)
  }

  def subscribeTopicFromEnd(consumer: KafkaConsumer[String, String], topicPartitionCount: Map[String, Int]): Unit = {
    consumer.subscribe(topicPartitionCount.keys)
//    consumer.poll(0)
    val tpList = getAllTopicPartition(topicPartitionCount)
    consumer.seekToEnd(tpList)
  }

  def subscribeTopicFromOffset(consumer: KafkaConsumer[String, String], topicPartitions: Map[String, Seq[(Int, Long)]]): Unit = {
    val tpMap: Map[TopicPartition, Long] = getAllTopicPartitionOffset(topicPartitions)
    consumer.subscribe(topicPartitions.keys,new ConsumerRebalanceListener(){
      def onPartitionsRevoked( partitions:util.Collection[TopicPartition]):Unit= {

      }

      @Override
      def onPartitionsAssigned(partitions:util.Collection[TopicPartition]):Unit= {
        val it = partitions.iterator()
        it.foreach(tp=>{

          consumer.seek(tp, tpMap.get(tp).get)

        })
      }

    })

  }

  private def getAllTopicPartition(topicPartitionCount: Map[String, Int]): Seq[TopicPartition] = {
    topicPartitionCount.foldLeft(ListBuffer.empty[TopicPartition])((tps, tpNum) => tps ++= getTopicPartition(tpNum._1, tpNum._2))
  }

  private def getAllTopicPartitionOffset(topicPartitions: Map[String, Seq[(Int, Long)]]): Map[TopicPartition, Long] = {
    topicPartitions.flatMap { case (topicName, partitionOffsetList) =>
      partitionOffsetList.map { case (partition, offset) =>
        (new TopicPartition(topicName, partition), offset)
      }
    }
  }

  private def getTopicPartition(topicName: String, partitionCount: Int): Seq[TopicPartition] = {
    (0 until partitionCount).map(partitionNum => {
      new TopicPartition(topicName, partitionNum)
    })
  }

}
