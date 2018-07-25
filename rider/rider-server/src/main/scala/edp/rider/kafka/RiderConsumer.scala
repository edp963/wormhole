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
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.kafka.TopicSource._
import edp.rider.module._
import edp.rider.rest.persistence.entities.FeedbackOffset
import edp.rider.rest.util.CommonUtils._
import edp.rider.service.MessageService
import edp.rider.service.util.CacheMap
import edp.wormhole.kafka.WormholeTopicCommand
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums._
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

class RiderConsumer(modules: ConfigurationModule with PersistenceModule with ActorModuleImpl)
  extends Actor with RiderLogger {

  import RiderConsumer._

  implicit val materializer = ActorMaterializer()

  override def preStart(): Unit = {
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

    super.preStart()
    self ! Start
  }

  override def receive: Receive = {
    case Start =>
      riderLogger.info("Initializing RiderConsumer")

      try {

        createFromOffset(RiderConfig.consumer.group_id)(context.system).foreach(
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
      riderLogger.info(s"Consumed key: ${msg.value().toString}")
    val curTs = currentMillSec
    val defaultStreamIdForRider = 0
    CacheMap.setOffsetMap(defaultStreamIdForRider, msg.topic(), msg.partition(), msg.offset())
    val partitionOffsetStr = CacheMap.getPartitionOffsetStrFromMap(defaultStreamIdForRider, msg.topic(), RiderConfig.consumer.partitions)
    modules.feedbackOffsetDal.insert(FeedbackOffset(1, UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET.toString, curTs, 0, msg.topic(), RiderConfig.consumer.partitions, partitionOffsetStr, curTs))

    if (msg.value() == null || msg.value() == "") {
      riderLogger.error(s"feedback message value is null: ${msg.toString}")
    } else {
      val messageService = new MessageService(modules)
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
