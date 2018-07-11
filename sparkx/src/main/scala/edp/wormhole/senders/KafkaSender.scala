package edp.wormhole.senders

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Author: lukong
  * Date: 2018/7/11
  * Description:
  */
object KafkaSender {

  def main(args: Array[String]): Unit = {

    val topic = "kafka-source"
    val key = "data_increment_data.kafka.kafka-test.kafka-source.ums_extension.*.*.*"
    val broker = "10.234.129.144:9092"
    val message = "{\"id\": 2,\"name\": \"bjkonglu\",\"phone\":\"18074546423\",\"address\": \"Beijing\",\"time\": \"2017-12-22 10:00:00\"}"

    val conf = new Properties()
    conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    conf.put("acks", "all")
    conf.put("compression.type", "lz4")
//    conf.put("parse.key", "true")
//    conf.put("key.separator", "@@@")

    conf.put("bootstrap.servers", broker)

    val sender = new KafkaProducer[String, String](conf)
    var count = 1
    while (true) {
      val record = new ProducerRecord[String, String](topic, key, message)
      sender.send(record)
      println(s"send message: $count")
//      Thread.sleep(100)
      count = count + 1
      sender.flush()
    }
  }
}
