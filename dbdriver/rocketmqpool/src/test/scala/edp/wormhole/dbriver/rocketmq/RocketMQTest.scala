package edp.wormhole.dbriver.rocketmq

import edp.wormhole.dbdriver.rocketmq.{MqMessage, WormholeRocketMQConsumer, WormholeRocketMQProducer}

object RocketMQTest extends App {
  val nameServer = "10.143.131.33:9976"
  val topic = "test_lwl"
  val groupConsumer = "test_lwl_consumer"
  val groupProducer = "test_lwl_producer"


  val mqMessage: MqMessage = MqMessage(topic, s"""{"test":"test"}""")
  val sendMessage = WormholeRocketMQProducer.genMqMessage(mqMessage)

  WormholeRocketMQProducer.init(nameServer, groupProducer)
  WormholeRocketMQProducer.send(nameServer, groupProducer, Iterator(sendMessage))
  WormholeRocketMQConsumer.initConsumer(nameServer, groupConsumer, topic)
  println("end")
}
