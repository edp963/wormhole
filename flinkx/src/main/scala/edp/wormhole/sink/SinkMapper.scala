package edp.wormhole.sink

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.swifts.SwiftsConfMemoryStorage
import edp.wormhole.ums.{Ums, UmsProtocol, UmsProtocolType, UmsTuple}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer
import edp.wormhole.ums.UmsFieldType.UmsFieldType

class SinkMapper(schemaMapWithUmsType:Map[String, (Int, UmsFieldType, Boolean)],sinkNamespace:String,sinkProcessConfig:SinkProcessConfig,umsFlowStart: Ums,connectionConfig:ConnectionConfig) extends RichMapFunction[Row, Seq[Row]] with java.io.Serializable{

  override def map(value: Row): Seq[Row] = {
    val listBuffer = ListBuffer.empty[String]
    val rowSize = schemaMapWithUmsType.size
    for (index <- 1 until rowSize) listBuffer.append(value.getField(index).toString)
    val umsTuple = UmsTuple(listBuffer)
    val protocol = UmsProtocol(UmsProtocolType.umsProtocolType(value.getField(0).toString))

    val (sinkObject, sinkMethod) =SwiftsConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
    val sinkSchemaMap=schemaMapWithUmsType.filter(_._2._1>0).map(entry=>(entry._1,(entry._2._1-1,entry._2._2,entry._2._3)))
    sinkMethod.invoke(sinkObject,protocol.`type`, umsFlowStart.schema.namespace, sinkNamespace, sinkProcessConfig, sinkSchemaMap, Seq(umsTuple.tuple), connectionConfig)
    Seq(value)
  }
}
