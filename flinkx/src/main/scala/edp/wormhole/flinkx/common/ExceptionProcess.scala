package edp.wormhole.flinkx.common

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.flinkx.common.ExceptionProcessMethod.ExceptionProcessMethod
import edp.wormhole.kafka.WormholeKafkaProducer
import org.apache.flink.types.Row
import org.apache.log4j.Logger

object ExceptionProcess {
  private lazy val logger = Logger.getLogger(this.getClass)
  def doExceptionProcess(exceptionProcessMethod: ExceptionProcessMethod, feedbackFlowFlinkxError: String, config: WormholeFlinkxConfig) = {
    exceptionProcessMethod match {
      case ExceptionProcessMethod.INTERRUPT =>
        throw new Throwable("process error")
      case ExceptionProcessMethod.FEEDBACK =>
        WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)
        WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, feedbackFlowFlinkxError, None, config.kafka_output.brokers)
      case _ =>
        logger.info("exception process method is: " + exceptionProcessMethod)
    }
  }
  def feedbackDataInfo(schemaName: String, pos: Int, value: Row): String = {
    if(value.getArity > pos) {
      schemaName + ":" + value.getField(pos).toString
    } else {
      schemaName + ":" + "null"
    }
  }
}
