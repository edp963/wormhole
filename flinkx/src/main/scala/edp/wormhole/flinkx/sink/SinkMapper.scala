package edp.wormhole.flinkx.sink

import edp.wormhole.flinkx.common.ConfMemoryStorage
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.swifts.SwiftsConstants
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{Ums, UmsProtocol, UmsProtocolType, UmsTuple}
import edp.wormhole.util.config.ConnectionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer

class SinkMapper(schemaMapWithUmsType:Map[String, (Int, UmsFieldType, Boolean)],sinkNamespace:String,sinkProcessConfig:SinkProcessConfig,umsFlowStart: Ums,connectionConfig:ConnectionConfig) extends RichMapFunction[Row, Seq[Row]] with java.io.Serializable{

  override def map(value: Row): Seq[Row] = {
    val listBuffer = ListBuffer.empty[String]
    val rowSize = schemaMapWithUmsType.size
    val protocolIndex=schemaMapWithUmsType(SwiftsConstants.PROTOCOL_TYPE)._1
    for (index <- 0 until rowSize) {
       if(index != protocolIndex){
         val fieldValue = if(value.getField(index)==null) null.asInstanceOf[String] else value.getField(index).toString
         listBuffer.append(fieldValue)
       }
    }
    val umsTuple = UmsTuple(listBuffer)

    val protocol = UmsProtocol(UmsProtocolType.umsProtocolType(value.getField(protocolIndex).toString))

    val (sinkObject, sinkMethod) =ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
    val sinkSchemaMap=schemaMapWithUmsType.filter(_._2._1 != protocolIndex).map(entry=>if(entry._2._1>protocolIndex)(entry._1,(entry._2._1-1,entry._2._2,entry._2._3))
    else (entry._1,(entry._2._1,entry._2._2,entry._2._3)))
    sinkMethod.invoke(sinkObject,protocol.`type`, umsFlowStart.schema.namespace, sinkNamespace, sinkProcessConfig, sinkSchemaMap, Seq(umsTuple.tuple), connectionConfig)
    Seq(value)
  }
}
