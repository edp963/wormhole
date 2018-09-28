package edp.wormhole.flinkx.sink

import edp.wormhole.flinkx.common.{ConfMemoryStorage, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.UmsFlowStartUtils
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.swifts.SwiftsConstants
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums._
import edp.wormhole.util.config.ConnectionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer


class SinkMapper(schemaMapWithUmsType: Map[String, (Int, UmsFieldType, Boolean)], sinkNamespace: String, sinkProcessConfig: SinkProcessConfig, umsFlowStart: Ums, connectionConfig: ConnectionConfig, config: WormholeFlinkxConfig, initialTs: Long, swiftsTs: Long) extends RichMapFunction[Row, Seq[Row]] with java.io.Serializable {

  override def map(value: Row): Seq[Row] = {
    val sinkTs = System.currentTimeMillis
    val listBuffer = ListBuffer.empty[String]
    val rowSize = schemaMapWithUmsType.size
    for (index <- 0 until rowSize) {
      val fieldValue = if (value.getField(index) == null) null.asInstanceOf[String] else value.getField(index).toString
      listBuffer.append(fieldValue)
    }
    val umsTuple = UmsTuple(listBuffer)

    val (sinkObject, sinkMethod) = ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
    sinkMethod.invoke(sinkObject, umsFlowStart.schema.namespace, sinkNamespace, sinkProcessConfig, schemaMapWithUmsType, Seq(umsTuple.tuple), connectionConfig)
    val doneTs = System.currentTimeMillis
    val (streamId, sourceNamespace) = extractTupleFromUms(umsFlowStart)
    //    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
    //      UmsProtocolUtils.feedbackFlowStats(sourceNamespace, protocol.`type`.toString, DateUtils.currentDateTime, streamId.asInstanceOf[Long], "", sinkNamespace,
    //        count, DateUtils.dt2date(maxTs.split("\\+")(0).replace("T", " ")).getTime, initialTs, initialTs, initialTs, swiftsTs, sinkTs, doneTs), None, config.kafka_output.brokers)

    Seq(value)
  }

  private def extractTupleFromUms(umsFlowStart: Ums) = {
    val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head)
    val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
    (streamId, sourceNamespace)
  }
}
