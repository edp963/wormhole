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

import akka.NotUsed
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.module._
import edp.rider.service.MessageService
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global

class RiderConsumer(modules: ConfigurationModule with PersistenceModule with ActorModuleImpl) extends RiderLogger {

  lazy val messageService = new MessageService(modules)
  implicit val system = modules.kafkaConsumerSystem
  implicit val materializer = ActorMaterializer()

  def feedbackConsume(): Unit = {
    try {
      createFromOffset(RiderConfig.consumer.group_id)
        .flatMapMerge(4, _._2)
        .mapAsync(4)(processMessage)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      riderLogger.info("RiderConsumer started")
    } catch {
      case ex: Exception =>
        riderLogger.error("RiderConsumer started failed", ex)
    }
  }

  def createFromOffset(groupId: String): Source[(TopicPartition, Source[ConsumerRecord[String, String], NotUsed]), Control] = {
    val propertyMap = new mutable.HashMap[String, String]()
    propertyMap(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.session.timeout.ms", 60000).toString
    propertyMap(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.heartbeat.interval.ms", 50000).toString
    propertyMap(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.max.poll.records", 500).toString
    propertyMap(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.request.timeout.ms", 80000).toString
    propertyMap(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.max.partition.fetch.bytes", 10485760).toString
    propertyMap(ConsumerConfig.FETCH_MIN_BYTES_CONFIG) = 0.toString
    propertyMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) = "true"
    propertyMap(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.auto.commit.interval.ms", 60000).toString
    propertyMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) = RiderConfig.getStringConfig("kafka.consumer.auto.offset.reset", "latest")
    if (RiderConfig.kerberos.enabled) {
      propertyMap("security.protocol") = "SASL_PLAINTEXT"
      propertyMap("sasl.kerberos.service.name") = "kafka"
    }

    val consumerSettings = new ConsumerSettings(propertyMap.toMap, Option(RiderConfig.consumer.keyDeserializer),
      Option(RiderConfig.consumer.valueDeserializer),
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

    Consumer.plainPartitionedSource(consumerSettings, Subscriptions.topics(RiderConfig.consumer.feedbackTopic))
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

  private def processMessage(msg: ConsumerRecord[String, String]): Future[Unit] = {
    riderLogger.debug(s"Consumed: [topic,partition,offset](${msg.topic()}, ${msg.partition()}), ${msg.offset()}]")
    if (msg.key() != null)
      riderLogger.debug(s"Consumed key: ${msg.value().toString}")

    if (msg.value() == null || msg.value() == "") {
      riderLogger.error(s"feedback message value is null: ${msg.toString}")
      Future(Success)
    } else {
      try {
        val ums: Ums = json2Ums(msg.value())
        riderLogger.debug(s"Consumed protocol: ${ums.protocol.`type`.toString}")
        ums.protocol.`type` match {
          //          case FEEDBACK_DATA_INCREMENT_HEARTBEAT =>
          //            messageService.doFeedbackHeartbeat(ums)
          //          case FEEDBACK_DATA_INCREMENT_TERMINATION =>
          //            messageService.doFeedbackHeartbeat(ums)
          //          case FEEDBACK_DATA_BATCH_TERMINATION =>
          //            messageService.doFeedbackHeartbeat(ums)
          case FEEDBACK_DIRECTIVE =>
            messageService.doFeedbackDirective(ums)
          case FEEDBACK_FLOW_SPARKX_ERROR =>
            messageService.doFeedbackFlowError(ums)
          case FEEDBACK_FLOW_STATS =>
            if (RiderConfig.es != null)
              messageService.doFeedbackFlowStats(ums)
          case FEEDBACK_STREAM_BATCH_ERROR =>
            messageService.doFeedbackStreamBatchError(ums)
          //          case FEEDBACK_STREAM_TOPIC_OFFSET =>
          //            messageService.doFeedbackStreamTopicOffset(ums)
          case _ =>
          //            riderLogger.info(s"illegal protocol type ${ums.protocol.`type`.toString}")
        }
        Future(Success)
      } catch {
        case e: Exception =>
          riderLogger.error(s"parse protocol error key: ${msg.key()} value: ${msg.value()}", e)
          Future(Success)
      }
    }
  }
}
