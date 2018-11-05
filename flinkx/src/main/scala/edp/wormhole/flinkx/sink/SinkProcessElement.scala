package edp.wormhole.flinkx.sink

import akka.protobuf.ByteString.Output
import edp.wormhole.flinkx.common.{ConfMemoryStorage, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.UmsFlowStartUtils
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.ums.{Ums, UmsProtocolUtils, UmsTuple}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

class SinkProcessElement(schemaMapWithUmsType: Map[String, (Int, UmsFieldType, Boolean)], sinkNamespace: String, sinkProcessConfig: SinkProcessConfig, umsFlowStart: Ums, connectionConfig: ConnectionConfig, config: WormholeFlinkxConfig, initialTs: Long, swiftsTs: Long, sinkTag: OutputTag[String]) extends ProcessFunction[Row, Seq[Row]] with java.io.Serializable{
  //private val outputTag = OutputTag[String]("sinkException")
  override def processElement(value: Row, ctx: ProcessFunction[Row, Seq[Row]]#Context, out: Collector[Seq[Row]]): Unit = {
    val sinkTs = System.currentTimeMillis
    val listBuffer = ListBuffer.empty[String]
    val rowSize = schemaMapWithUmsType.size
    val (streamId, sourceNamespace) = extractTupleFromUms(umsFlowStart)
    try {
      for (index <- 0 until rowSize) {
        val fieldValue = if (value.getField(index) == null) null.asInstanceOf[String] else value.getField(index).toString
        listBuffer.append(fieldValue)
      }
      val umsTuple = UmsTuple(listBuffer)

      val (sinkObject, sinkMethod) = ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
      sinkMethod.invoke(sinkObject, umsFlowStart.schema.namespace, sinkNamespace, sinkProcessConfig, schemaMapWithUmsType, Seq(umsTuple.tuple), connectionConfig)
      val doneTs = System.currentTimeMillis

      //ctx.output(sinkTag, "testtest")
      //    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
      //      UmsProtocolUtils.feedbackFlowStats(sourceNamespace, protocol.`type`.toString, DateUtils.currentDateTime, streamId.asInstanceOf[Long], "", sinkNamespace,
      //        count, DateUtils.dt2date(maxTs.split("\\+")(0).replace("T", " ")).getTime, initialTs, initialTs, initialTs, swiftsTs, sinkTs, doneTs), None, config.kafka_output.brokers)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        ctx.output(sinkTag, UmsProtocolUtils.WormholeExceptionMessage(sourceNamespace, streamId.toLong, new DateTime(), sinkNamespace, "", "sink exception"))
    }
    out.collect(Seq(value))
  }

  private def extractTupleFromUms(umsFlowStart: Ums) = {
    val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head)
    val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
    (streamId, sourceNamespace)
  }
}
