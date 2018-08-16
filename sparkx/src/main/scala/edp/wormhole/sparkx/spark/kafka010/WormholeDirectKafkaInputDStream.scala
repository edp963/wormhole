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



package org.apache.spark.streaming.kafka010

import edp.wormhole.sparkx.spark.kafka010.WormholePerPartitionConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class WormholeDirectKafkaInputDStream[K, V](
                                             _ssc: StreamingContext,
                                             locationStrategy: LocationStrategy,
                                             consumerStrategy: ConsumerStrategy[K, V],
                                             ppc: PerPartitionConfig
                                           ) extends DirectKafkaInputDStream[K, V](_ssc, locationStrategy, consumerStrategy, ppc) {

  @transient private var ekc: Consumer[K, V] = null

  def getStreamingContext(): StreamingContext ={
    _ssc
  }

  override def stop(): Unit = this.synchronized {
    if (ekc != null) {
      ekc.close()
    }
  }

  override def consumer(): Consumer[K, V] = this.synchronized {
    if (null == ekc) {
      ekc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava)
    }
    ekc
  }

  def updateTopicOffset(addTopicOffset: Map[(String, Int), (Long, Long)] = Map[(String, Int), (Long, Long)](),
                        delTopic: Set[String] = Set[String]()): Unit = this.synchronized {
    //first Long is offset,second Long is rate
    val addTpOffset: Map[TopicPartition, (Long, Long)] = if (addTopicOffset != null) addTopicOffset.map(tp => new TopicPartition(tp._1._1, tp._1._2) -> (tp._2._1, tp._2._2)) else Map.empty[TopicPartition, (Long, Long)]
    val prevTpOffset: Map[TopicPartition, (Long, Long)] = currentOffsets.map(elem => {
      elem._1 -> (elem._2, ppc.maxRatePerPartition(elem._1))
    })

    val curTpOffset: Map[TopicPartition, (Long, Long)] = if (delTopic != null && delTopic.nonEmpty) {
      (prevTpOffset ++ addTpOffset).filter(tp => {
        !delTopic.contains(tp._1.topic())
      })
    } else prevTpOffset ++ addTpOffset

    logWarning("updateTopicOffset before topic " + prevTpOffset)
    logWarning("updateTopicOffset addtopic:" + addTpOffset + " deltopic:" + delTopic)
    logWarning("updateTopicOffset after topic " + curTpOffset)

    //if kc is null,  create reconfig kafka consumer - rkc
    val rkc = consumer

    //check input topic valid or not, new topic must create first
    val rkcTpList = ListBuffer.empty[TopicPartition]
    rkc.listTopics.asScala.foreach(partitions => {
      val topicName = partitions._1
      partitions._2.asScala.foreach(partition => {
        rkcTpList += new TopicPartition(topicName, partition.partition())
      })
    })
    val invalidTopics = curTpOffset.keySet.diff(rkcTpList.toSet)
    if (0 != invalidTopics.size) {
      logWarning("input a invalid topics: " + invalidTopics)
      throw new Exception("input invalid topics:" + invalidTopics)
    }

    val oldPerPartitionRate = ppc.asInstanceOf[WormholePerPartitionConfig].getPerPartitionRate

    try {
      ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(curTpOffset.map(tp => {
        (tp._1, tp._2._2)
      }))
      rkc.subscribe(curTpOffset.keySet.map(_.topic()).asJava)
      rkc.poll(0)
      //check offset valid or not
      rkc.seekToEnd(curTpOffset.keySet.asJava)
      //get the max offset end
      val curTpOffsetEnd: Map[TopicPartition, Long] = curTpOffset.keySet.map(tp => new TopicPartition(tp.topic(), tp.partition()) -> rkc.position(tp)).toMap
      val invalidAddTpOffset = curTpOffset.filter(elem => {
        if (curTpOffsetEnd.contains(elem._1)) curTpOffsetEnd(elem._1) < elem._2._1
        else true
      })
      if (0 != invalidAddTpOffset.size) {
        logError("input invalid offset: " + invalidAddTpOffset + " end:" + curTpOffsetEnd)
        throw new Exception("input invalid topics:" + invalidAddTpOffset + " end:" + curTpOffsetEnd)
      }
    }
    catch {
      case any: Exception =>
        logError("check failed for catching:", any)
        logError("stack is: " + any.printStackTrace())
        ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(oldPerPartitionRate.toMap)
        rkc.subscribe(prevTpOffset.keySet.map(_.topic()).asJava)
        rkc.poll(0)
        currentOffsets.foreach { case (topicPartition, offset) =>
          rkc.seek(topicPartition, offset)
        }
        throw any
    }

    logWarning("updateTopicOffset before topicoffset " + currentOffsets)

    val backupOffset: Map[TopicPartition, Long] = currentOffsets
    try {
      //update to currentOffsets
      currentOffsets = curTpOffset.map(tp => {
        (tp._1, tp._2._1)
      })
      logWarning("updateTopicOffset after topicoffset " + currentOffsets)
      //set to seek position
      currentOffsets.foreach { case (topicPartition, offset) =>
        rkc.seek(topicPartition, offset)
      }
    }
    catch {
      case any: Exception =>
        currentOffsets = backupOffset //set back to previous value
        ppc.asInstanceOf[WormholePerPartitionConfig].setPerPartitionRate(oldPerPartitionRate.toMap)
        rkc.subscribe(prevTpOffset.keySet.map(_.topic()).asJava)
        rkc.poll(0)
        currentOffsets.foreach { case (topicPartition, offset) =>
          rkc.seek(topicPartition, offset)
        }
        logError("set back offset for catching: ", any)
        throw any
    }
    finally {
      logWarning("updateTopicOffset seek to end")
    }


  }

}
