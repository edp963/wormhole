package edp.rider.kafka

import edp.rider.common.KafkaVersion
import org.apache.log4j.Logger



object WormholeGetOffsetUtils extends Serializable{

  private lazy val logger = Logger.getLogger(this.getClass)
  private var currentVersion=KafkaVersion.KAFKA_UNKOWN

  def getLatestOffset(brokerList: String, topic: String, kerberos: Boolean = false): String = {
    edp.wormhole.kafka010.WormholeGetOffsetUtils.getLatestOffset(brokerList,topic,kerberos)
  }

  def getEarliestOffset(brokerList: String, topic: String, kerberos: Boolean = false): String = {
    edp.wormhole.kafka010.WormholeGetOffsetUtils.getEarliestOffset(brokerList,topic,kerberos)
  }

  def getConsumerOffset(brokers: String, groupId: String, topic: String, partitions: Int, kerberos: Boolean): String = {
    if(currentVersion==KafkaVersion.KAFKA_UNKOWN){
      logger.info("get consumer offset:unkown version")
      try{
        currentVersion=KafkaVersion.KAFKA_010
        edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
      }catch {
        case ex: Exception =>
          currentVersion=KafkaVersion.KAFKA_0102
          edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
      }
    }else{
      logger.info(s"get consumer offset version:${currentVersion}")
      currentVersion match {
        case KafkaVersion.KAFKA_010 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
        case KafkaVersion.KAFKA_0102 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
      }
    }
  }

/*  private def adaptKafkaVersion(version: String)={
    val versions=version.split("\\.")
    if(versions(0).toInt > 0 || versions(1).toInt > 10 || versions(2).toInt >= 2)
      KafkaVersion.KAFKA_0102
    else
      KafkaVersion.KAFKA_010
  }*/
}
