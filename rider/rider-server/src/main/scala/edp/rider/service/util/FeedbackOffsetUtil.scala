package edp.rider.service.util

import edp.rider.common.RiderLogger
import edp.rider.module.{ConfigurationModuleImpl, PersistenceModuleImpl}
import edp.rider.rest.persistence.entities._
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object FeedbackOffsetUtil extends RiderLogger with ConfigurationModuleImpl with PersistenceModuleImpl {

  def getOffsetFromFeedback(streamId: Long): Seq[FeedbackOffsetInfo] = {
    val list = new ListBuffer[FeedbackOffsetInfo]
    val a = Await.result(feedbackOffsetDal.getDistinctStreamTopicList(streamId), Duration.Inf)
    getLatestTopicOffset(a).foreach(
      topic => {
        if (topic.partitionOffsets != "") {
          topic.partitionOffsets.split(",").map(
            offset => {
              val parOffset = offset.split(":")
                list += FeedbackOffsetInfo(streamId, topic.topicName, parOffset(0).toInt, parOffset(1).toLong)
            }
          )
        }
      }
    )
    list
  }

  def getPartitionNumber(partitionOffsets: String): Int = partitionOffsets.split(",").length

  def getOffsetFromPartitionOffsets(partitionOffset: String, partitionId: Int): Long = {
    var offset: Long = 0L
    partitionOffset.split(",").foreach { e =>
      val index = e.indexOf(":")
      if (index > 0) {
        if (e.substring(0, index).toInt == partitionId) offset = e.substring((index + 1)).toLong
      }
    }
    offset
  }

  def getLatestTopicOffset(topics: Seq[StreamTopicPartitionId]): Seq[StreamTopicOffset] = {
    val topicList: ListBuffer[StreamTopicOffset] = new ListBuffer()
    try {
      topics.foreach { topic =>
        val recordOpt = Await.result(feedbackOffsetDal.getLatestOffset(topic.streamId, topic.topicName), Duration.Inf)
        recordOpt match {
          case Some(record) => topicList += StreamTopicOffset(topic.streamId, topic.topicName, record.partitionOffsets)
          case None => topicList += StreamTopicOffset(topic.streamId, topic.topicName, "")
        }
      }
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to get latest offset", e)
    }
    //riderLogger.info(s"getLatestTopicOffset $topicList")
    topicList.toList
  }

  def getPartitionOffsetStrFromMap(streamId: Long, topicName: String, partitionNum: Int): String = {
    var pid: Int = 0
    var partitionOffsetStr = ""
    while (pid < partitionNum) {
      val offset = CacheMap.getOffsetValue(streamId, topicName, pid)
      if (offset >= 0) {
        if (pid == 0)
          partitionOffsetStr = partitionOffsetStr + s"$pid:$offset"
        else
          partitionOffsetStr = partitionOffsetStr + s",$pid:$offset"
      }
      pid += 1
    }
    partitionOffsetStr
  }

  def getTopicMapForDB(streamId: Long, topicName: String, partitions: Int): scala.collection.mutable.Map[TopicPartition, Long] = {
    val topicMap = scala.collection.mutable.Map[TopicPartition, Long]()
    riderLogger.info(s"Rider Feedback Topic: $topicName, partition num: $partitions")
    val recordOpt = Await.result(feedbackOffsetDal.getLatestOffset(streamId, topicName), Duration.Inf)
    val partitionOffsets = recordOpt match {
      case Some(record) => record.partitionOffsets
      case None => "0:0,1:0,2:0,3:0"
    }
    var pid = 0
    while (pid < partitions) {
      try {
        val offset: Long = FeedbackOffsetUtil.getOffsetFromPartitionOffsets(partitionOffsets, pid)
        if (offset >= 0) topicMap.put(new TopicPartition(topicName, pid), offset)
      } catch {
        case e: Exception =>
          riderLogger.error(s"Failed to get latest offset", e)
      }
      pid += 1
    }
    topicMap
  }

  def deleteFeedbackOffsetHistory(pastNdays: String) = {
    val topics = Await.result(feedbackOffsetDal.getDistinctList, Duration.Inf)
    val topicList: ListBuffer[Long] = new ListBuffer()
    topics.foreach { topic =>
      val record = Await.result(feedbackOffsetDal.getLatestOffset(topic.streamId, topic.topicName), Duration.Inf)
      if (record.nonEmpty && record.get.id > 0) topicList.append(record.get.id)
    }
    feedbackOffsetDal.deleteHistory(pastNdays, topicList.toList)
  }
}