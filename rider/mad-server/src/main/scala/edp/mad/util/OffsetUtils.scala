package edp.mad.util

import edp.mad.module.{ConfigModuleImpl, DBDriverModuleImpl, PersistenceModuleImpl}
import edp.wormhole.kafka.WormholeGetOffsetShell
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.language.postfixOps

object OffsetUtils extends ConfigModuleImpl with DBDriverModuleImpl with PersistenceModuleImpl{

  private val logger = Logger.getLogger(this.getClass)

  def getKafkaLatestOffset(brokers: String, topic: String): String = {
    try {
      WormholeGetOffsetShell.getTopicOffsets(brokers, topic)
    } catch {
      case ex: Exception =>
        logger.error(s"get kafka latest offset failed", ex)
        ""
    }
  }

  def getMadConsumerStartOffset(streamId: Long, brokerList: String, topicName: String, fromOffset: String): Map[TopicPartition,Long] = {
    var partitionOffset = ""
    val topicMap = scala.collection.mutable.Map[TopicPartition, Long]()
    try {
      if(fromOffset == "latest"){
          partitionOffset = getKafkaLatestOffset(brokerList, topicName)
      }else{
        val recordOpt = Await.result(offsetSavedDal.getLatestOffset(streamId,topicName), FiniteDuration(180, SECONDS))
        recordOpt match {
          case Some(record) =>
            if (record.partitionOffsets != "") partitionOffset = record.partitionOffsets
          case None =>
            partitionOffset = getKafkaLatestOffset(brokerList, topicName)
        }
      }
      logger.info(s" getMadConsumerStartOffset brokers: ${brokerList}  Topic: ${topicName}  Partition offsets : $partitionOffset ")
    }catch{
      case e: Exception =>
        logger.error(s"error Message \n",e)
    }

    if( partitionOffset != "" ){
      partitionOffset.split(",").map(
        offset => {
          val parOffset = offset.split(":")
          topicMap.put( new TopicPartition( topicName,parOffset(0).toInt),parOffset(1).toLong)
        }
      )
    }
    logger.info(s" getMadConsumerStartOffset topicMap  $topicMap")
    topicMap.toMap
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


}