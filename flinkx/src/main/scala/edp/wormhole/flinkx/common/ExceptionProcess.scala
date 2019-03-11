package edp.wormhole.flinkx.common

import java.util.UUID

import edp.wormhole.common.feedback.ErrorPattern
import edp.wormhole.flinkx.common.ExceptionProcessMethod.ExceptionProcessMethod
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.UmsProtocolType
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.log4j.Logger

class ExceptionProcess(exceptionProcessMethod: ExceptionProcessMethod, config: WormholeFlinkxConfig, exceptionConfig: ExceptionConfig) extends RichMapFunction[String, String] with java.io.Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def map(feedbackFlowFlinkxError: String): String = doExceptionProcess(feedbackFlowFlinkxError)

  def doExceptionProcess(feedbackFlowFlinkxError: String): String = {
    logger.info("--------------------exception stream:" + feedbackFlowFlinkxError)
    exceptionProcessMethod match {
      case ExceptionProcessMethod.INTERRUPT =>
        throw new Throwable("process error")
      case ExceptionProcessMethod.FEEDBACK =>
        WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)
        FlinkxUtils.setFlowErrorMessage(config.kafka_input.kafka_topics.map(_.topic_name),
          null, config, exceptionConfig.sourceNamespace, exceptionConfig.sinkNamespace, -1,
          feedbackFlowFlinkxError, UUID.randomUUID().toString, UmsProtocolType.DATA_BATCH_DATA.toString + "," + UmsProtocolType.DATA_INCREMENT_DATA.toString + "," + UmsProtocolType.DATA_INITIAL_DATA.toString,
          exceptionConfig.flowId,exceptionConfig.streamId, ErrorPattern.FlowError)
      case _ =>
        logger.info("exception process method is: " + exceptionProcessMethod)
    }
    feedbackFlowFlinkxError
  }
}
