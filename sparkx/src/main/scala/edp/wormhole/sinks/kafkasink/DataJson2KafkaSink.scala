package edp.wormhole.sinks.kafkasink

import java.util.UUID

import com.alibaba.fastjson.JSON
import edp.wormhole.common.{ConnectionConfig, JsonParseHelper}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocol
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType

class DataJson2KafkaSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logInfo("In DataJson2KafkaSink")
    WormholeKafkaProducer.init(connectionConfig.connectionUrl, connectionConfig.parameters)
    val kafkaTopic = sinkNamespace.split("\\.")(2)
    //val protocol: UmsProtocol = UmsProtocol(protocolType)
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    tupleList.foreach(tuple => {
      val value = JsonParseHelper.jsonObjHelper(tuple, schemaMap, targetSchemaArr)
      WormholeKafkaProducer.sendMessage(kafkaTopic, value.toJSONString, Some(protocolType.toString + "." + sinkNamespace+"..."+UUID.randomUUID().toString), connectionConfig.connectionUrl)
    }
    )
  }
}
