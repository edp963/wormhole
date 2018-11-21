package edp.wormhole.flinkx.util

import java.util.UUID

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.flinkx.common.WormholeFlinkxConfig
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.UmsProtocolUtils
import edp.wormhole.util.DateUtils
import org.apache.flink.metrics.MetricConfig
import org.apache.flink.metrics.reporter.{AbstractReporter, Scheduled}

class FeedbackMetricsReporter(config:WormholeFlinkxConfig,sourceNamespace: String,sinkNamespace:String,streamId:Long) extends AbstractReporter with Scheduled{


  override def open(config: MetricConfig): Unit = {

  }

  override def close(): Unit = {

  }

  override def report(): Unit = {
    val topics=firstUmsTs=lastUmsTs=protocolType=""
    val batchId = UUID.randomUUID().toString
    var payloadSize=0

    WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config,config.kerberos)
    val iterator=this.gauges.keySet().iterator()
    if(iterator.hasNext)(protocolType,topics,firstUmsTs,lastUmsTs)=iterator.next().getValue().asInstanceOf[String].split("~")

    val counterIterator=this.counters.keySet().iterator()
    if(counterIterator.hasNext) payloadSize=counterIterator.next().getCount

    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
      UmsProtocolUtils.feedbackFlowStats(sourceNamespace,protocolType,DateUtils.currentDateTime,streamId,batchId,sinkNamespace,topics,payloadSize,txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(lastUmsTs),txt2Long(lastUmsTs),lastUmsTs), None, config.kafka_output.brokers)
  }

  def txt2Long(str: String):Long={
     return java.lang.Long.parseLong(str)
  }

  override def filterCharacters(str:String):String={
     return str;
  }
}
