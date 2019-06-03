package edp.wormhole.sinks.kafkasink

import java.util.UUID

import com.alibaba.fastjson.JSON
import edp.wormhole.common.json.JsonParseHelper
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

class DataJson2KafkaSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)
  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logger.info("In DataJson2KafkaSink")
    val sinkSpecificConfig = if (sinkProcessConfig.specialConfig.isDefined) JsonUtils.json2caseClass[KafkaConfig](sinkProcessConfig.specialConfig.get) else KafkaConfig(None, None, None, None, None)
    WormholeKafkaProducer.init(connectionConfig.connectionUrl, connectionConfig.parameters,sinkSpecificConfig.kerberos.getOrElse(false))
    val kafkaTopic = sinkNamespace.split("\\.")(2)
    //val protocol: UmsProtocol = UmsProtocol(protocolType)
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    tupleList.foreach(tuple => {
      val value = JsonParseHelper.jsonObjHelper(tuple, schemaMap, targetSchemaArr)
      WormholeKafkaProducer.sendMessage(kafkaTopic, value.toJSONString, Some(UmsProtocolType.DATA_INCREMENT_DATA.toString + "." + sinkNamespace+"..."+UUID.randomUUID().toString), connectionConfig.connectionUrl)
    }
    )
  }
}
