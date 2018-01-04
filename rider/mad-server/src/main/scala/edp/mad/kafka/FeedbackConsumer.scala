package edp.mad.kafka

import akka.actor.{Actor, ActorSystem}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import edp.mad.module._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import edp.mad.util.OffsetUtils

class FeedbackConsumer() extends Actor {
  import FeedbackConsumer._
  implicit val materializer = ActorMaterializer()
  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

  def createFromOffset(tps : scala.Predef.Map[org.apache.kafka.common.TopicPartition, scala.Long])(implicit system: ActorSystem): Source[CommittableMessage[Array[Byte], String], Control] = {
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(modules.madKafka.feedbackBrokers)
      .withGroupId(modules.madKafka.feedbackGroupId)
      .withPollInterval(FiniteDuration(50, MILLISECONDS))
      .withPollTimeout( FiniteDuration(50, MILLISECONDS))
      .withStopTimeout(FiniteDuration(30, SECONDS))
      .withCloseTimeout(FiniteDuration(30, SECONDS))
      .withCommitTimeout( FiniteDuration(15, SECONDS))
      .withWakeupTimeout(FiniteDuration(10, SECONDS))
      .withDispatcher("akka.kafka.default-dispatcher")

    Consumer.committableSource(consumerSettings, Subscriptions.assignmentWithOffset(tps))
  }

  override def preStart(): Unit = {
    /*
    try {
      WormholeTopicCommand.createOrAlterTopic(madKafka.feedbackZkUrl, madKafka.feedbackTopic, madKafka.feedbackPartitions)
      logger.info(s"initial create ${madKafka.feedbackTopic} topic success")
    } catch {
      case _: kafka.common.TopicExistsException =>
        logger.info(s"${madKafka.feedbackTopic} topic already exists")
      case ex: Exception =>
        logger.error(s"initial create ${madKafka.feedbackTopic} topic failed", ex)
    }
*/
    super.preStart()
    self ! Start
  }

  override def receive: Receive = {
    case Start =>
      logger.info(s"Initializing Consumer broker ${modules.madKafka.feedbackBrokers} topic: ${modules.madKafka.feedbackTopic} \n ")
      val topicMap = OffsetUtils.getMadConsumerStartOffset(modules.feedbackConsumerStreamId, modules.madKafka.feedbackBrokers, modules.madKafka.feedbackTopic, modules.madKafka.feedbackFromOffset)
        logger.info(s" Topic Map $topicMap")
        val (control, future) = createFromOffset(topicMap)(context.system)
          .mapAsync(2) { msg => FeedbackProcessor.processMessage(msg) }
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

      logger.info("RiderConsumer started")
  }

  def running(control: Control): Receive = {
    case Stop =>
      logger.info("RiderConsumerShutting stop ")
      println(s" ===============Feedback Consumer running stop \n ")
      control.shutdown().andThen {
        case _ =>
          context.stop(self)
      }

  }
}

object FeedbackConsumer {
  type Message = CommittableMessage[Array[Byte], String]

  case object Start
  case object Stop
}
