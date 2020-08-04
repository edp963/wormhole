import com.alibaba.fastjson.JSON
import edp.wormhole.kafka.{WormholeGetOffsetUtils, WormholeKafkaConsumer}
import edp.wormhole.util.config.KVConfig
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions

object groupTest extends App {
  val brokers = "ip:port"
  val aaa = WormholeGetOffsetUtils.getConsumerOffset(brokers, "wormhole_demo_hdfslog_17", "topicl", false)
  println(aaa)
  println(aaa)
}


object consumerTest extends App {
  val brokers = "ip:port"
  val consumer = WormholeKafkaConsumer.initConsumer(brokers, "wormhole_demo_hdfslog_22",Some(Seq(KVConfig("enable.auto.commit", "true"), KVConfig("auto.commit.interval.ms", 100.toString))))
  //WormholeKafkaConsumer.subscribeTopicFromBeginning(consumer, Map("topicl" -> 0))
  //consumer.commitSync()
  val topicPartition = new TopicPartition("topicl", 0)
  //consumer.assign(JavaConversions.seqAsJavaList(Seq(topicPartition)))
  consumer.assign(JavaConversions.seqAsJavaList(Seq(topicPartition)))
  consumer.seek(topicPartition, 1025)
  while(true) {
    consumer.poll(10000)
    consumer.commitAsync()
  }

}
