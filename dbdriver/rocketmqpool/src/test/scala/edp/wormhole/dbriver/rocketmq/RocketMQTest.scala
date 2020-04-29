package edp.wormhole.dbriver.rocketmq

import edp.wormhole.dbdriver.rocketmq.{MqMessage, WormholeRocketMQConsumer, WormholeRocketMQProducer}

object RocketMQTest extends App {
  val nameServer = ""
  val topic = "test_lwl"
  val groupConsumer = "test_lwl_consumer"
  val groupProducer = "test_lwl_producer"


  val mqMessage1: MqMessage = MqMessage(topic, s"""{"test1":"test1"}""")
  val sendMessage1 = WormholeRocketMQProducer.genMqMessage(mqMessage1)

  val mqMessage2: MqMessage = MqMessage(topic, s"""{"test2":"test2"}""")
  val sendMessage2 = WormholeRocketMQProducer.genMqMessage(mqMessage2)

  WormholeRocketMQProducer.init(nameServer, groupProducer)
  WormholeRocketMQProducer.send(nameServer, groupProducer, Seq(sendMessage1, sendMessage2))
  WormholeRocketMQConsumer.initConsumer(nameServer, groupConsumer, topic)
  println("end")
}
