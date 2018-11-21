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
    val iterator=this.gauges.keySet.iterator
    if(iterator.hasNext){
      val (protocolType,topics,firstUmsTs,lastUmsTs) = iterator.next.getValue.asInstanceOf[String].split("~").array match{
        case Array(protocolType,topics,firstUmsTs,lastUmsTs)=>(protocolType,topics,firstUmsTs,lastUmsTs)
      }
      val batchId = UUID.randomUUID().toString
      var payloadSize=0L
      WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config,config.kerberos)
      val counterIterator=this.counters.keySet.iterator
      if(counterIterator.hasNext) payloadSize=counterIterator.next.getCount

//      WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
//        UmsProtocolUtils.feedbackFlowStats(sourceNamespace,protocolType,DateUtils.currentDateTime,streamId,batchId,sinkNamespace,topics,payloadSize,txt2Int(firstUmsTs),txt2Int(firstUmsTs),txt2Int(firstUmsTs),txt2Int(firstUmsTs),txt2Int(lastUmsTs),txt2Int(lastUmsTs),lastUmsTs), None, config.kafka_output.brokers)
    }
  }

  def txt2Int(str: String):Int={
     return str.toInt
  }

  override def filterCharacters(str:String):String={
     return str;
  }
}
