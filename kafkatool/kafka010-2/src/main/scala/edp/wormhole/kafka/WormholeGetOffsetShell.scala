package edp.wormhole.kafka

import java.util
import java.util.Properties

import joptsimple.OptionParser
import kafka.admin.AdminClient
import kafka.client.ClientUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object WormholeGetOffsetShell {

  private val logger = Logger.getLogger(this.getClass)

  def getTopicOffsets(brokerList: String, topic: String, kerberos: Boolean = false, time: Long = -1, maxWaitMs: Int = 60000) = {
    try {
      val parser = new OptionParser
      ToolsUtils.validatePortOrDie(parser, brokerList)

      val props = new Properties()

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // key反序列化方式
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // value反系列化方式
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList) // 指定broker地址，来找到group的coordinator
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, maxWaitMs.toString)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "80000")
      if (kerberos) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        props.put("sasl.kerberos.service.name", "kafka")
      }

      val consumer = new KafkaConsumer[String, String](props)
      val offsetSeq = new ListBuffer[String]()
      val topicMap = consumer.listTopics()

      if (!topicMap.isEmpty && topicMap.containsKey(topic) && topicMap.get(topic) != null && topicMap.get(topic).size() > 0) {
        val it = topicMap.get(topic).iterator()
        while (it.hasNext) {
          val partition = it.next
          try {
            val topicAndPartition = new TopicPartition(topic, partition.partition())
            val map = time match {
              case -1 => consumer.endOffsets(util.Arrays.asList(topicAndPartition))
              case _ => consumer.beginningOffsets(util.Arrays.asList(topicAndPartition))
            }
            offsetSeq += partition.partition() + ":" + map.get(topicAndPartition)
          } catch {
            case e: Exception =>
              logger.info(e.printStackTrace())
              consumer.close()
              throw new Exception(s"brokerList $brokerList topic $topic partition ${partition.partition()} doesn't have a leader, please verify it.")
          }
        }
        consumer.close()
      }
      val offset = offsetSeq.sortBy(offset => offset.split(":")(0).toLong).mkString(",")
      if (offset == "")
        throw new Exception(s"query topic $topic offset result is '', please check it.")
      logger.info(s"offset is:$offset")
      offset
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getConsumerOffset(broker: String, groupid: String): Map[String, String] = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker)
    val adminClient = AdminClient.create(props)

    val offsetMap: Map[TopicPartition, Long] = adminClient.listGroupOffsets(groupid)
    val r = if (offsetMap == null || offsetMap.isEmpty) null.asInstanceOf[Map[String, String]]
    else {
      val map = mutable.HashMap.empty[String, ListBuffer[(Int, Long)]]
      offsetMap.foreach { case (k, v) =>
        if (map.contains(k.topic())) {
          map(k.topic) += ((k.partition(), v))
        } else {
          val list = ListBuffer.empty[(Int, Long)]
          list += ((k.partition(), v))
          map(k.topic) = list
        }
      }

      val tpMap = mutable.HashMap.empty[String, String]
      map.foreach { case (k, v) =>
        tpMap(k) = v.sortBy(_._1).map(ele => {
          s"${ele._1}:${ele._2}"
        }).mkString(",")
      }
      tpMap.toMap
    }
    adminClient.close()
    r
  }
}

