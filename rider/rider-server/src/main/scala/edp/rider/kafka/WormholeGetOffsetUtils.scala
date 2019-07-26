package edp.rider.kafka

import java.io.EOFException

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
        case ex: EOFException =>
          currentVersion=KafkaVersion.KAFKA_0102
          try{
            edp.wormhole.kafka.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
          }catch {
            case ex =>
              currentVersion=KafkaVersion.KAFKA_UNKOWN
              throw ex
          }
        case _ =>
          try{
            edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
          }catch {
            case ex: Throwable =>
              currentVersion=KafkaVersion.KAFKA_UNKOWN
              throw ex
          }
      }
    }else{
      logger.info(s"get consumer offset version:${currentVersion}")
      currentVersion match {
        case KafkaVersion.KAFKA_010 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
        case KafkaVersion.KAFKA_0102 => edp.wormhole.kafka.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
      }
    }
  }
}
