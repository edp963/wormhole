/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.rider.kafka

import java.util
import java.util.Properties

import akka.actor.Actor
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.kafka.KafkaUtils.{offsetValid, riderLogger}
import edp.rider.kafka.TopicSource._
import edp.rider.module._
import edp.rider.rest.persistence.entities.FeedbackOffset
import edp.rider.rest.util.CommonUtils._
import edp.rider.service.MessageService
import edp.rider.service.util.{CacheMap, FeedbackOffsetUtil}
import edp.wormhole.kafka.WormholeGetOffsetShell.logger
import edp.wormhole.kafka.{WormholeGetOffsetShell, WormholeTopicCommand}
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums._
import joptsimple.OptionParser
import kafka.client.ClientUtils
import kafka.utils.ToolsUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

class RiderConsumer(modules: ConfigurationModule with PersistenceModule with ActorModuleImpl)
  extends Actor with RiderLogger{

  import RiderConsumer._

  implicit val materializer = ActorMaterializer()
  lazy val messageService = new MessageService(modules)

  val props=new Properties()

  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // key反序列化方式
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // value反系列化方式
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RiderConfig.consumer.brokers) // 指定broker地址，来找到group的coordinator
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
  props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,"80000")
  if(RiderConfig.kerberos.enabled){
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name", "kafka")
  }
  lazy val consumer:KafkaConsumer[String, String]=new KafkaConsumer[String, String](props)

  override def preStart(): Unit = {

    if(!RiderConfig.kerberos.enabled){
      try {
        WormholeTopicCommand.createOrAlterTopic(RiderConfig.consumer.zkUrl, RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.partitions, RiderConfig.consumer.refactor)
        riderLogger.info(s"initial create ${RiderConfig.consumer.feedbackTopic} topic success")
      } catch {
        //      case _: kafka.common.TopicExistsException =>
        //        riderLogger.info(s"${RiderConfig.consumer.feedbackTopic} topic already exists")
        case ex: Exception =>
          riderLogger.error(s"initial create ${RiderConfig.consumer.feedbackTopic} topic failed", ex)
      }
      try {
        WormholeTopicCommand.createOrAlterTopic(RiderConfig.consumer.zkUrl, RiderConfig.spark.wormholeHeartBeatTopic, 1, RiderConfig.consumer.refactor)
        riderLogger.info(s"initial create ${RiderConfig.spark.wormholeHeartBeatTopic} topic success")
      } catch {
        //      case _: kafka.common.TopicExistsException =>
        //        riderLogger.info(s"${RiderConfig.spark.wormholeHeartBeatTopic} topic already exists")
        case ex: Exception =>
          riderLogger.error(s"initial create ${RiderConfig.spark.wormholeHeartBeatTopic} topic failed", ex)
      }
    }

    super.preStart()
    self ! Start
  }

  override def receive: Receive = {
    case Start =>
      riderLogger.info("Initializing RiderConsumer")

      try {

        createFromOffset(RiderConfig.consumer.group_id).foreach(
          source => {
            val (control, future) = source.mapAsync(1)(processMessage)
              .toMat(Sink.ignore)(Keep.both)
              .run()
            context.become(running(control))

            future.onFailure {
              case ex =>
                riderLogger.error(s"RiderConsumer stream failed due to error", ex)
                throw ex
                self ! Stop
            }
          }
        )

        riderLogger.info("RiderConsumer started")
      } catch {
        case ex: Exception =>
          riderLogger.error("RiderConsumer started failed", ex)
          self ! Stop
      }
  }

  def createFromOffset(groupId: String): Seq[Source[ConsumerRecord[Array[Byte], String], Control]] = {
    //    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    //      .withBootstrapServers(RiderConfig.consumer.brokers)
    //      .withGroupId(RiderConfig.consumer.group_id)
    val propertyMap = new mutable.HashMap[String, String]()
    propertyMap("session.timeout.ms") = RiderConfig.getIntConfig("kafka.consumer.session.timeout.ms", 60000).toString
    propertyMap("heartbeat.interval.ms") = RiderConfig.getIntConfig("kafka.consumer.heartbeat.interval.ms", 50000).toString
    propertyMap("max.poll.records") = RiderConfig.getIntConfig("kafka.consumer.max.poll.records", 500).toString
    propertyMap("request.timeout.ms") = RiderConfig.getIntConfig("kafka.consumer.request.timeout.ms", 80000).toString
    propertyMap("max.partition.fetch.bytes") = RiderConfig.getIntConfig("kafka.consumer.max.partition.fetch.bytes", 10485760).toString
    propertyMap("fetch.min.bytes") = 0.toString
    propertyMap("enable.auto.commit") = "false"
    if(RiderConfig.kerberos.enabled){
      propertyMap("security.protocol")="SASL_PLAINTEXT"
      propertyMap("sasl.kerberos.service.name")="kafka"
    }

    val consumerSettings = new ConsumerSettings(propertyMap.toMap, Some(RiderConfig.consumer.keyDeserializer),
      Some(RiderConfig.consumer.valueDeserializer),
      RiderConfig.consumer.pollInterval,
      RiderConfig.consumer.pollTimeout,
      RiderConfig.consumer.stopTimeout,
      RiderConfig.consumer.closeTimeout,
      RiderConfig.consumer.commitTimeout,
      RiderConfig.consumer.wakeupTimeout,
      RiderConfig.consumer.maxWakeups,
      RiderConfig.consumer.dispatcher)
      .withBootstrapServers(RiderConfig.consumer.brokers)
      .withGroupId(RiderConfig.consumer.group_id)
    val topicMap: mutable.Map[TopicPartition, Long] = FeedbackOffsetUtil.getTopicMapForDB(0, RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.partitions)
    val earliestMap = {
      try {
        KafkaUtils.getKafkaEarliestOffsetOnActor(RiderConfig.consumer.brokers, RiderConfig.consumer.feedbackTopic,consumer)
          .split(",").map(partition => {
          val partitionOffset = partition.split(":")
          (new TopicPartition(RiderConfig.consumer.feedbackTopic, partitionOffset(0).toInt), partitionOffset(1).toLong)
        }).toMap[TopicPartition, Long]
      } catch {
        case ex: Exception =>
          "0:0,1:0,2:0,3:0".split(",").map(partition => {
            val partitionOffset = partition.split(":")
            (new TopicPartition(RiderConfig.consumer.feedbackTopic, partitionOffset(0).toInt), partitionOffset(1).toLong)
          }).toMap[TopicPartition, Long]
      }
    }
    topicMap.foreach(partition => {
      if (partition._2 < earliestMap(partition._1))
        topicMap(partition._1) = earliestMap(partition._1)
    })

    topicMap.toMap.map(
      topic => Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topic))
    ).toSeq
  }

  def running(control: Control): Receive = {
    case Stop =>
      riderLogger.info("RiderConsumerShutting stop ")

      control.shutdown().andThen {
        case _ =>
          context.stop(self)
      }

  }

  def getProtocolFromKey(key: String): UmsProtocolType = {
    val protocolTypeStr: String = key.substring(0, key.indexOf(".") - 1)
    UmsProtocolType.umsProtocolType(protocolTypeStr)
  }

  private def json2Ums(json: String): Ums = {
    try {
      UmsSchemaUtils.toUms(json)
    } catch {
      case e: Throwable =>
        riderLogger.error(s"feedback $json convert to case class failed", e)
        Ums(UmsProtocol(UmsProtocolType.FEEDBACK_DIRECTIVE), UmsSchema("defaultNamespace"))
    }
  }

  private def processMessage(msg: Message): Future[Message] = {
    riderLogger.debug(s"Consumed: [topic,partition,offset](${msg.topic()}, ${msg.partition()}), ${msg.offset()}]")
    if (msg.key() != null)
      riderLogger.debug(s"Consumed key: ${msg.value().toString}")
    val curTs = currentMillSec
    val defaultStreamIdForRider = 0
    CacheMap.setOffsetMap(defaultStreamIdForRider, msg.topic(), msg.partition(), msg.offset())
    val partitionOffsetStr = CacheMap.getPartitionOffsetStrFromMap(defaultStreamIdForRider, msg.topic(), RiderConfig.consumer.partitions)
    modules.feedbackOffsetDal.insert(FeedbackOffset(1, UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET.toString, curTs, 0, msg.topic(), RiderConfig.consumer.partitions, partitionOffsetStr, curTs))

    if (msg.value() == null || msg.value() == "") {
      riderLogger.error(s"feedback message value is null: ${msg.toString}")
    } else {
      try {
        val ums: Ums = json2Ums(msg.value())
        riderLogger.debug(s"Consumed protocol: ${ums.protocol.`type`.toString}")
        ums.protocol.`type` match {
          case FEEDBACK_DATA_INCREMENT_HEARTBEAT =>
            messageService.doFeedbackHeartbeat(ums)
          case FEEDBACK_DATA_INCREMENT_TERMINATION =>
            messageService.doFeedbackHeartbeat(ums)
          case FEEDBACK_DATA_BATCH_TERMINATION =>
            messageService.doFeedbackHeartbeat(ums)
          case FEEDBACK_DIRECTIVE =>
            messageService.doFeedbackDirective(ums)
          case FEEDBACK_FLOW_ERROR =>
            messageService.doFeedbackFlowError(ums)
          case FEEDBACK_FLOW_STATS =>
            if (RiderConfig.es != null)
              messageService.doFeedbackFlowStats(ums)
          case FEEDBACK_STREAM_BATCH_ERROR =>
            messageService.doFeedbackStreamBatchError(ums)
          case FEEDBACK_STREAM_TOPIC_OFFSET =>
            messageService.doFeedbackStreamTopicOffset(ums)
          case _ => riderLogger.info(s"illegal protocol type ${ums.protocol.`type`.toString}")
        }
      } catch {
        case e: Exception =>
          riderLogger.error(s"parse protocol error key: ${msg.key()} value: ${msg.value()}", e)
      }
    }
    Future.successful(msg)
  }

}

object RiderConsumer extends RiderLogger {
  type Message = ConsumerRecord[Array[Byte], String]

  case object Start

  case object Stop

}
