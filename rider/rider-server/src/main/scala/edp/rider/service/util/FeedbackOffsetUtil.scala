package edp.rider.service.util

import edp.rider.common.RiderLogger
import edp.rider.module.{ConfigurationModuleImpl, PersistenceModuleImpl}
import edp.rider.rest.persistence.entities.{StreamTopicPartition, FeedbackOffsetInfo}
import edp.rider.service.util.CacheMap._
import org.apache.kafka.common.TopicPartition
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object FeedbackOffsetUtil extends RiderLogger with ConfigurationModuleImpl with PersistenceModuleImpl
{

  def getOffsetFromFeedback(streamId: Long): Seq[FeedbackOffsetInfo] = getLatestTopicOffset( Await.result(feedbackOffsetDal.getDistinctStreamTopicList(streamId), Duration.Inf))

  def getPartitionNumber( partitionOffsets:String):Int =partitionOffsets.split(",").length

  def getOffsetFromPartitionOffsets(partitionOffset:String, partitionId:Int ):Long={
    var offset = -1
    partitionOffset.split(",").foreach{e=>
      if ( e.substring((e.indexOf(":")+1)).toLong == partitionId)
        offset = e.substring(0,e.indexOf(":")-1).toInt
    }
    offset
  }

  def getLatestTopicOffset(topics: Seq[StreamTopicPartition]): Seq[FeedbackOffsetInfo] = {
    val topicList: ListBuffer[FeedbackOffsetInfo] = new ListBuffer()
    topics.foreach{topic =>
      val record = Await.result(feedbackOffsetDal.getLatestOffset(topic.streamId, topic.topicName), Duration.Inf)
      var pid:Int = 0
      while (pid < topic.partitions.getOrElse(1)) {
        try {
          val offset = getOffsetFromPartitionOffsets(record.partitionOffsets, pid)
          if (offset >= 0) topicList.append(FeedbackOffsetInfo(topic.streamId,topic.topicName, pid, offset))
        } catch {
          case e: Exception =>
            riderLogger.error(s"Failed to get latest offset", e)
        }
        pid += 1
      }
    }
    topicList
  }

  def getPartitionOffsetStrFromMap(streamId:Long,topicName:String,partitionNum:Int):String={
    var pid:Int = 0
    var partitionOffsetStr = ""
    while(pid < partitionNum){
      val offset = CacheMap.getOffsetValue(streamId,topicName,pid)
      if(offset>=0) {
        if(pid ==0 )
          partitionOffsetStr = partitionOffsetStr + s"pid:$offset"
        else
          partitionOffsetStr = partitionOffsetStr + s",pid:$offset"
      }
      pid+=1
    }
    partitionOffsetStr
  }

  def getTopicMapForDB(streamId:Long, topicName: String, partitions: Int): scala.collection.mutable.Map[TopicPartition, Long] = {
    val topicMap = scala.collection.mutable.Map[TopicPartition, Long]()
    riderLogger.info(s"Rider Feedback Topic: $topicName, partition num: $partitions")
    val record = Await.result(feedbackOffsetDal.getLatestOffset(streamId, topicName), Duration.Inf)
    var pid = 0
    while (pid < partitions) {
      try {
        val offset: Long = FeedbackOffsetUtil.getOffsetFromPartitionOffsets(record.partitionOffsets, pid)
        if (offset >= 0) topicMap.put(new TopicPartition(topicName, pid), offset)
      } catch {
        case e: Exception =>
          riderLogger.error(s"Failed to get latest offset", e)
      }
      pid += 1
    }
    riderLogger.info(s"Rider Consumer Topic: " + topicMap.toString)
    topicMap
  }

  def deleteFeedbackOffsetHistory(pastNdays : String ) = {
    val topics = Await.result(feedbackOffsetDal.getDistinctList, Duration.Inf)
    val topicList: ListBuffer[Long] = new ListBuffer()
    topics.foreach{topic =>
      val record = Await.result(feedbackOffsetDal.getLatestOffset(topic.streamId, topic.topicName), Duration.Inf)
      if(record.id>0) topicList.append(record.id)
    }
    feedbackOffsetDal.deleteHistory(pastNdays,topicList.toList)
  }
}