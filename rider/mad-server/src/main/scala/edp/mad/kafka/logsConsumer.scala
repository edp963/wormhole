package edp.mad.kafka

import akka.actor.{Actor, ActorSystem}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
//import edp.mad.kafka.._
import edp.mad.module._
import edp.mad.util.OffsetUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class LogsConsumer extends Actor {
  import LogsConsumer._
  implicit val materializer = ActorMaterializer()
  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

  def createFromOffset(tps : scala.Predef.Map[org.apache.kafka.common.TopicPartition, scala.Long])(implicit system: ActorSystem): Source[CommittableMessage[Array[Byte], String], Control] = {
    val consumerSettings = ConsumerSettings(modules.system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(modules.madKafka.feedbackBrokers)
      .withGroupId(modules.madKafka.logsGroupId)
      .withPollInterval(FiniteDuration(50, MILLISECONDS))
      .withPollTimeout( FiniteDuration(50, MILLISECONDS))
      .withStopTimeout(FiniteDuration(30, SECONDS))
      .withCloseTimeout(FiniteDuration(30, SECONDS))
      .withCommitTimeout( FiniteDuration(15, SECONDS))
      .withWakeupTimeout(FiniteDuration(10, SECONDS))
      .withDispatcher("akka.kafka.default-dispatcher")

    Consumer.committableSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))
  }

  def createFromLatest(tps : String)(implicit system: ActorSystem): Source[CommittableMessage[Array[Byte], String], Control] = {
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(modules.madKafka.logsBrokers)
      .withGroupId(modules.madKafka.logsGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .withPollInterval(FiniteDuration(50, MILLISECONDS))
      .withPollTimeout( FiniteDuration(50, MILLISECONDS))
      .withStopTimeout(FiniteDuration(30, SECONDS))
      .withCloseTimeout(FiniteDuration(30, SECONDS))
      .withCommitTimeout( FiniteDuration(15, SECONDS))
      .withWakeupTimeout(FiniteDuration(10, SECONDS))
      .withDispatcher("akka.kafka.default-dispatcher")

    Consumer.committableSource(consumerSettings, Subscriptions.topics(tps))
  }

  override def preStart(): Unit = {
    /*
    try {
      WormholeTopicCommand.createOrAlterTopic(madKafka.logsZkUrl, madKafka.logsTopic, madKafka.logsTopicPartitions)
      logger.info(s"/ ${madKafka.logsTopic} topic success")
    } catch {
      case _: kafka.common.TopicExistsException =>
        logger.info(s"${madKafka.logsTopic} topic already exists")
      case ex: Exception =>
        logger.error(s"initial create ${madKafka.logsTopic} topic failed", ex)
    }
    */
    super.preStart()
    self ! Start
  }

  override def receive: Receive = {
    case Start =>
      logger.info(s"Initializing logs Consumer broker ${modules.madKafka.logsBrokers} topic: ${modules.madKafka.logsTopic} \n ")
      if ( modules.madKafka.logsFromOffset == "latest"){
        val (control, future) = createFromLatest(modules.madKafka.logsTopic)(context.system)
          .mapAsync(2) { msg => LogsProcesser.processMessage(msg) }
          .map(_.committableOffset)
          .groupedWithin(10, 180.seconds)
          .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
          .mapAsync(1)(_.commitScaladsl())
          .toMat(Sink.ignore)(Keep.both)
          .run()

        context.become(running(control))

        future.onFailure {
          case ex =>
            logger.error(s"RiderConsumer stream failed due to error, restarting ${ex.getMessage}")
            throw ex
        }
      }else {
        val topicMap = OffsetUtils.getMadConsumerStartOffset((-1), modules.madKafka.logsBrokers, modules.madKafka.logsTopic, modules.madKafka.logsFromOffset)
        logger.info(s" Topic Map $topicMap")

        val (control, future) = createFromOffset(topicMap.toMap)(context.system)
          .mapAsync(2) { msg => LogsProcesser.processMessage(msg) }
          .map(_.committableOffset)
          .groupedWithin(10, 180.seconds)
          .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
          .mapAsync(1)(_.commitScaladsl())
          .toMat(Sink.ignore)(Keep.both)
          .run()

        context.become(running(control))

        future.onFailure {
          case ex =>
            logger.error(s"RiderConsumer stream failed due to error, restarting ${ex.getMessage}")
            throw ex
        }
      }

      logger.info("RiderConsumer started")
  }

  def running(control: Control): Receive = {
    case Stop =>
      logger.info("RiderConsumerShutting stop ")
      println(s" =================Logs Consumer running stop \n ")
      control.shutdown().andThen {
        case _ =>
          context.stop(self)
      }

  }
}

object LogsConsumer {
  type Message = CommittableMessage[Array[Byte], String]

  case object Start
  case object Stop
}
