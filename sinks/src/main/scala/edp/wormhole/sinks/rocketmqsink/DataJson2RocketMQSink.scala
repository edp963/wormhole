package edp.wormhole.sinks.rocketmqsink

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import edp.wormhole.common.json.JsonParseHelper
import edp.wormhole.dbdriver.rocketmq.{MqMessage, WormholeRocketMQProducer}
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

class DataJson2RocketMQSink extends SinkProcessor{
  private lazy val logger = Logger.getLogger(this.getClass)
  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logger.info("In DataJson2KafkaSink sinkProcessConfig" + sinkProcessConfig )
    val sourceNamespaceSeq = sourceNamespace.split("\\.")
    val sinkSpecificConfig = if (sinkProcessConfig.specialConfig.isDefined) JsonUtils.json2caseClass[RocketMQConfig](sinkProcessConfig.specialConfig.get) else RocketMQConfig(None, None, None, None, None, None)
    val producerGroup = sinkSpecificConfig.producerGroup.getOrElse(s"${sourceNamespaceSeq(0)}_${sourceNamespaceSeq(1)}_${sourceNamespaceSeq(2)}_${sourceNamespaceSeq(3)}")
    WormholeRocketMQProducer.init(connectionConfig.connectionUrl, producerGroup)
    val mqTopic = sinkNamespace.split("\\.")(2)
    //val protocol: UmsProtocol = UmsProtocol(protocolType)
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    val mqMessages = tupleList.map(tuple => {
      val value = JsonParseHelper.jsonObjHelper(tuple, schemaMap, targetSchemaArr)
      val mqMessage = MqMessage(mqTopic, JSON.toJSONString(value, SerializerFeature.WriteMapNullValue), sinkSpecificConfig.tags)
      WormholeRocketMQProducer.genMqMessage(mqMessage)
    })
    WormholeRocketMQProducer.send(connectionConfig.connectionUrl, producerGroup, mqMessages)
  }

}
