package edp.wormhole.reporter

import java.util.UUID

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.UmsProtocolUtils
import edp.wormhole.util.DateUtils
import org.apache.flink.metrics.MetricConfig
import org.apache.flink.metrics.reporter.{AbstractReporter, Scheduled}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

class FeedbackMetricsReporter extends AbstractReporter with Scheduled{
  private lazy val logger = Logger.getLogger(this.getClass)

  private lazy val sourceNamespace=new StringBuffer()
  private lazy val sinkNamespace=new StringBuffer()
  private lazy val streamId=new StringBuffer()
  private lazy val topic=new StringBuffer()
  private lazy val kerberos=new StringBuffer()
  private lazy val brokers=new StringBuffer()
  private lazy val feedbackCount=new ArrayBuffer[Int]()
  override def open(config: MetricConfig): Unit = {
    sourceNamespace.append(config.getString("sourceNamespace",""))
    sinkNamespace.append(config.getString("sinkNamespace",""))
    streamId.append(config.getString("streamId",""))
    topic.append(config.getString("topic",""))
    brokers.append(config.getString("brokers",""))
    kerberos.append(config.getString("kerberos",""))
    feedbackCount.append(config.getInteger("feedbackCount",0))
  }

  override def close(): Unit = {

  }

  override def report(): Unit = {
    val iterator=this.gauges.keySet.iterator
    if(iterator.hasNext){
      val (protocolType,topics,firstUmsTs,lastUmsTs) = iterator.next.getValue.asInstanceOf[String].split("~") match{
        case Array(protocolType,topics,firstUmsTs,lastUmsTs)=>(protocolType,topics,firstUmsTs,lastUmsTs)
      }

      var payloadSize=0L
      val counterIterator=this.counters.keySet.iterator
      if(counterIterator.hasNext) payloadSize=counterIterator.next.getCount
      if(payloadSize.toInt>=feedbackCount.head){
        val batchId = UUID.randomUUID().toString
        Thread.currentThread.setContextClassLoader(null)  //当kafkaProducer在单独线程里时，会存在由于classLoader加载问题，导致的StringSerilizer加载异常问题，故这里做此操作
        WormholeKafkaProducer.init(brokers.toString, None,kerberos.toString.toBoolean)
        WormholeKafkaProducer.sendMessage(topic.toString, FeedbackPriority.FeedbackPriority4,
          UmsProtocolUtils.feedbackFlowStats(sourceNamespace.toString,protocolType,DateUtils.currentDateTime,streamId.toString.toLong,batchId,sinkNamespace.toString,topics,payloadSize.toInt,txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(firstUmsTs),txt2Long(lastUmsTs),txt2Long(lastUmsTs),lastUmsTs), None, brokers.toString)
        this.counters.keySet.iterator.next.dec(payloadSize)
      }
    }
  }

  def txt2Long(str: String):Long={
    return java.lang.Long.parseLong(str)
  }

  override def filterCharacters(str:String):String={
    return str;
  }
}
