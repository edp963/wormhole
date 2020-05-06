package edp.wormhole.dbdriver.rocketmq

import java.util

import edp.wormhole.util.config.KVConfig
import org.apache.log4j.Logger
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
import org.apache.rocketmq.client.consumer.listener.{ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently}
import org.apache.rocketmq.common.consumer.ConsumeFromWhere
import org.apache.rocketmq.common.message.MessageExt
import scala.collection.JavaConversions._

object WormholeRocketMQConsumer {
  private val logger = Logger.getLogger(this.getClass)
  def initConsumer(namesrvAddr: String, consumerGroup: String, topic: String, kvConfig: Option[Seq[KVConfig]]=None, consumerMode: Option[String]=None): Unit = {
    initPushConsumer(namesrvAddr, consumerGroup, topic, kvConfig)
  }

  def initPushConsumer(namesrvAddr: String, consumerGroup: String, topic: String, kvConfig: Option[Seq[KVConfig]]=None): Unit ={
    val consumer = new DefaultMQPushConsumer(consumerGroup)
    consumer.setNamesrvAddr(namesrvAddr)
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    consumer.subscribe(topic, "*")
    consumer.registerMessageListener(new MessageListenerConcurrently {
      override def consumeMessage(msgs: util.List[MessageExt], context: ConsumeConcurrentlyContext): ConsumeConcurrentlyStatus = {
        //logger.info(s"""${Thread.currentThread.getName} Receive New Messages: $msgs""")
        println(s"""${Thread.currentThread.getName} Receive New Messages: """)
        for(m <- msgs) {
          println(new String(m.getBody) + ";")
        }
        ConsumeConcurrentlyStatus.CONSUME_SUCCESS
      }
    })
    consumer.start()
    //logger.info("Consumer Started")
    println("Consumer Started")
  }

  def initPullConsumer(namesrvAddr: String, consumerGroup: String, kvConfig: Option[Seq[KVConfig]]=None): Unit ={

  }
}
