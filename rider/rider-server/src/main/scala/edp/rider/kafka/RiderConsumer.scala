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

import akka.actor.Actor
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import edp.rider.common.{Consume, RiderConfig, RiderLogger, Stop}
import FeedbackProcess._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RiderConsumer extends Actor with RiderLogger {

  implicit val materializer = ActorMaterializer()

  private def initKerberos(): Unit = {
    if (RiderConfig.kerberos.kafkaEnabled) {
      System.setProperty("java.security.auth.login.config", RiderConfig.kerberos.riderJavaAuthConf)
      System.setProperty("java.security.krb5.conf", RiderConfig.kerberos.javaKrb5Conf)
    }
  }

  override def receive: Receive = {
    case Consume =>
      feedbackConsume()
    case Stop =>
      super.postStop()
  }

  def feedbackConsume(): Unit = {
    try {
      initKerberos()
      createConsumer(RiderConfig.consumer.group_id)
        .groupedWithin(RiderConfig.consumer.batchRecords, RiderConfig.consumer.batchDuration)
        .map(processMessage)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      riderLogger.info("RiderConsumer started")
    } catch {
      case ex: Exception =>
        riderLogger.error("RiderConsumer started failed", ex)
    }
  }

  def createConsumer(groupId: String): Source[ConsumerRecord[String, String], Control] = {
    val propertyMap = new mutable.HashMap[String, String]()
    propertyMap(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.session.timeout.ms", 60000).toString
    propertyMap(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.heartbeat.interval.ms", 50000).toString
    propertyMap(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.max.poll.records", 500).toString
    propertyMap(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.request.timeout.ms", 80000).toString
    propertyMap(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG) = RiderConfig.getIntConfig("kafka.consumer.max.partition.fetch.bytes", 10485760).toString
    propertyMap(ConsumerConfig.FETCH_MIN_BYTES_CONFIG) = 0.toString
    propertyMap(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) = "true"
    propertyMap(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) = RiderConfig.consumer.autoCommitIntervalMs.toString
    propertyMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) = RiderConfig.getStringConfig("kafka.consumer.auto.offset.reset", "latest")
    if (RiderConfig.kerberos.kafkaEnabled) {
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

    Consumer.plainSource(consumerSettings, Subscriptions.topics(RiderConfig.consumer.feedbackTopic))
  }


  private def processMessage(records: Seq[ConsumerRecord[String, String]]): Unit = {
    try {
      //      val sparkxFlowErrorBuffer = new ListBuffer[Ums]
      //     // val sparkxStreamErrorBuffer = new ListBuffer[Ums]
      val sparkxFlowStatsBuffer = new ListBuffer[Ums]
      //      val flinkxFlowErrorBuffer = new ListBuffer[Ums]
      val feedbackErrorBuffer = new ListBuffer[Ums]
      val feedbackFlowStartBuffer = new ListBuffer[Ums]

      records.foreach(record => {
        if (record != null) {
          val ums = json2Ums(record.value())
          if (ums != null) {
            val key = ums.protocol.`type`.toString
            if (key.startsWith(FEEDBACK_FLOW_ERROR.toString)) {
              feedbackErrorBuffer.append(ums)
            } else if (key.startsWith(FEEDBACK_FLOW_STATS.toString)) {
              sparkxFlowStatsBuffer.append(ums)
            } else if (key.startsWith(FEEDBACK_DIRECTIVE.toString)) {
              feedbackFlowStartBuffer.append(ums)
            }
          }
        }
      })

      doFeedbackError(feedbackErrorBuffer.toList)
      //doStreamBatchError(sparkxStreamErrorBuffer.toList)
      doSparkxFlowStats(sparkxFlowStatsBuffer.toList)
      doFeedbackDirective(feedbackFlowStartBuffer.toList)

    } catch {
      case e: Exception =>
        riderLogger.error(s"process feedback batch message, $records", e)
    }
  }

  private def json2Ums(json: String): Ums = {
    try {
      UmsSchemaUtils.toUms(json)
    } catch {
      case e: Throwable =>
        riderLogger.error(s"feedback $json convert to ums failed", e)
        null
    }
  }

}
