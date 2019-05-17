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

import edp.wormhole.util.config.KVConfig
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object WormholeKafkaConsumer {
  private val logger = Logger.getLogger(this.getClass)

  def initConsumer(brokers: String, groupid: String, kvConfig: Option[Seq[KVConfig]],kerberos:Boolean=false): KafkaConsumer[String, String] = {

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

    if(kerberos){
      props.put("security.protocol","SASL_PLAINTEXT")
      props.put("sasl.kerberos.service.name","kafka")
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

  def consumeRecordsBetweenOffsetRange(consumer: KafkaConsumer[String, String], topicPartition: TopicPartition, fromOffset: Long,untilOffset: Long, readTimeout:Int) : ConsumerRecords[String, String]={
    val consumeRecordList=new util.ArrayList[ConsumerRecord[String,String]]()
    val consumeRecordMap=new util.HashMap[TopicPartition,util.List[ConsumerRecord[String,String]]]()
    var currentOffset=fromOffset
    consumer.assign(JavaConversions.seqAsJavaList(Seq(topicPartition)))
    consumer.seek(topicPartition,fromOffset)
    while(currentOffset<untilOffset){
      val consumerRecordIterator=consumer.poll(readTimeout).iterator()

      while(consumerRecordIterator.hasNext && currentOffset<untilOffset){
        val consumeRecord=consumerRecordIterator.next()
        currentOffset=consumeRecord.offset()
        if(currentOffset<untilOffset)
          consumeRecordList.add(consumeRecord)
      }
    }
    consumeRecordMap.put(topicPartition,consumeRecordList)
    return new ConsumerRecords[String, String](consumeRecordMap)
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
