package edp.wormhole.flinkx.common

import edp.wormhole.flinkx.common.ExceptionProcessMethod.ExceptionProcessMethod
import edp.wormhole.kafka.WormholeKafkaProducer
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
        WormholeKafkaProducer.initWithoutAcksAll(config.kafka_output.brokers, config.kafka_output.config, config.kafka_output.kerberos)
        FlinkxUtils.sendFlowErrorMessage(feedbackFlowFlinkxError,  config, exceptionConfig.flowId)
      case _ =>
        logger.info("exception process method is: " + exceptionProcessMethod)
    }
    feedbackFlowFlinkxError
  }
}
