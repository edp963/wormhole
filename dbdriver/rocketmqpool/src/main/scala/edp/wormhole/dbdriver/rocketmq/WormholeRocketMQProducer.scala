package edp.wormhole.dbdriver.rocketmq

import java.util

import edp.wormhole.util.config.KVConfig
import org.apache.log4j.Logger
import org.apache.rocketmq.client.producer.{DefaultMQProducer, SendResult, SendStatus}
import org.apache.rocketmq.common.message.Message
import org.apache.rocketmq.remoting.common.RemotingHelper
import scala.collection.JavaConversions._

import scala.collection.mutable

object WormholeRocketMQProducer extends Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)

  @volatile private var producerMap = new mutable.HashMap[String, DefaultMQProducer]()

  def init(namesrvAddr: String, producerGroup: String, kvConfig: Option[Seq[KVConfig]]=None): Unit = {
    if(!producerMap.contains(namesrvAddr) || null == producerMap.get(namesrvAddr)) {
      synchronized{
        if(!producerMap.contains(namesrvAddr) || null == producerMap.get(namesrvAddr)) {
          val producer = new DefaultMQProducer(producerGroup)
          producer.setNamesrvAddr(namesrvAddr)
          producer.start()
          producerMap.put(namesrvAddr, producer)
        }
      }
    }
  }

  def genMqMessage(mqMessage: MqMessage): Message = {
    val message = new Message()
    message.setTopic(mqMessage.topic)
    if(mqMessage.tags.isDefined) {
      message.setTags(mqMessage.tags.get)
    }
    if(mqMessage.keys.isDefined) {
      message.setKeys(mqMessage.keys.get)
    }
    if(mqMessage.flag.isDefined) {
      message.setFlag(mqMessage.flag.get)
    }
    if(mqMessage.waitStoreMsgOK.isDefined) {
      message.setWaitStoreMsgOK(mqMessage.waitStoreMsgOK.get)
    }
    if(mqMessage.kvConfig.isDefined) {
      mqMessage.kvConfig.get.foreach(kv => message.putUserProperty(kv.key, kv.value))
    }
    message.setBody(mqMessage.body.getBytes(RemotingHelper.DEFAULT_CHARSET))
    message
  }

  private def getProducer(namesrvAddr: String, producerGroup: String): DefaultMQProducer = {
    if(!producerMap.contains(namesrvAddr)) {
      init(namesrvAddr, producerGroup)
    }
    val mqProducer = producerMap(namesrvAddr)
    if(null == mqProducer) {
      logger.error(s"get kafkaProducer failed, producerMap not contain $namesrvAddr")
    }
    mqProducer
  }

  def close(namesrvAddr: String): Unit ={
    try {
      if(producerMap.contains(namesrvAddr) && null != producerMap(namesrvAddr)) {
        producerMap(namesrvAddr).shutdown()
        producerMap -= namesrvAddr
      }
    } catch {
      case ex: Throwable =>{
        logger.error("close - ERROR", ex)
      }
    }
  }

  def send(namesrvAddr: String, producerGroup: String, messages: Seq[Message], timeout: Long = 3000): Unit = {
    //val messagesSend: util.List[Message] = messages.toList
    if(null != messages && !messages.isEmpty) {
      try {
        val listSplitter = new ListSplitter(messages)
        while(listSplitter.hasNext) {
          val messagesItem = listSplitter.next()
          val sendResult: SendResult = getProducer(namesrvAddr, producerGroup).send(messagesItem, timeout)
          sendResult.getSendStatus match {
            case SendStatus.SEND_OK =>
            case _ =>
              logger.warn(s"""send to mq message warn: namesrvAddr = $namesrvAddr, messages = ${messages.toString}, sendResult ${sendResult.toString}""")
          }
        }
      } catch {
        case ex: Throwable => {
            logger.error("send - send ERROR:", ex)
            try {
              close(namesrvAddr)
            } catch {
              case closeError: Throwable =>
                logger.error("sendInternal - close ERROR,", closeError)
                producerMap -= namesrvAddr
            }
            throw ex
        }
      }
    }
  }
}
