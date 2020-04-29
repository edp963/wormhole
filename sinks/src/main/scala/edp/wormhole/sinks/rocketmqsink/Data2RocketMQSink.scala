package edp.wormhole.sinks.rocketmqsink

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.dbdriver.rocketmq.{MqMessage, WormholeRocketMQProducer}
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.WormholeUms.toJsonCompact
import edp.wormhole.ums._
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger
import org.apache.rocketmq.common.message.Message

class Data2RocketMQSink extends SinkProcessor{
  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String,
                       sinkNamespaceOrg: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val sinkNamespaceSeq = sinkNamespaceOrg.split("\\.")
    val sourceNamespaceSeq = sourceNamespace.split("\\.")
    val sinkNamespace = s"${sinkNamespaceSeq(0)}.${sinkNamespaceSeq(1)}.${sinkNamespaceSeq(2)}.${sinkNamespaceSeq(3)}.${sourceNamespaceSeq(4)}.${sourceNamespaceSeq(5)}.${sourceNamespaceSeq(6)}"

    if(tupleList.nonEmpty) {
      logger.info(s"In Data2RocketMQSink ${tupleList.head}, size is ${tupleList.size}")
    }
    logger.info("Data2RocketMQSink sink config: " + sinkProcessConfig)
    val sinkSpecificConfig = if (sinkProcessConfig.specialConfig.isDefined) JsonUtils.json2caseClass[RocketMQConfig](sinkProcessConfig.specialConfig.get) else RocketMQConfig(None, None, None, None, None, None)
    val producerGroup = sinkSpecificConfig.producerGroup.getOrElse(s"${sourceNamespaceSeq(0)}_${sourceNamespaceSeq(1)}_${sourceNamespaceSeq(2)}_${sourceNamespaceSeq(3)}")
    WormholeRocketMQProducer.init(connectionConfig.connectionUrl, producerGroup)

    val protocol: UmsProtocol =
      if (sinkSpecificConfig.topic.nonEmpty && sinkSpecificConfig.topic.get.nonEmpty)
        UmsProtocol(UmsProtocolType.DATA_BATCH_DATA)
      else
        UmsProtocol(UmsProtocolType.DATA_INCREMENT_DATA)

    //自定义topic
    val mqTopic = if (sinkSpecificConfig.topic.nonEmpty && sinkSpecificConfig.topic.get.nonEmpty) sinkSpecificConfig.topic.get else sinkNamespace.split("\\.")(2)
    logger.info("sink topic: " + mqTopic + " sink namespace: " + sinkNamespace)
    logger.info("sink protocol: " + protocol.`type`.toString)

    val schemaList: Seq[(String, (Int, UmsFieldType, Boolean))] = schemaMap.toSeq.sortBy(_._2._1)

    val format = sinkSpecificConfig.messageFormat.trim
    format match {
      case "ums" =>
        val seqUmsField: Seq[UmsField] = schemaList.map(kv => UmsField(kv._1, kv._2._2, Some(kv._2._3)))
        val schema = UmsSchema(sinkNamespace, Some(seqUmsField))
        val mqLimitNum = sinkSpecificConfig.limitNum
        ums2RocketMQ(tupleList, mqLimitNum, protocol, schema, mqTopic, connectionConfig, sinkSpecificConfig, producerGroup)
      case "flattenJson" =>
        val hasSystemField = sinkSpecificConfig.hasSystemField
        if (hasSystemField) flattenJson2RocketMQWithSystemValue(tupleList, schemaList, sinkNamespace, mqTopic, connectionConfig, protocol.`type`.toString, sinkSpecificConfig, producerGroup)
        else flattenJson2RocketMQWithoutSystemValue(tupleList, schemaList, sinkNamespace, mqTopic, connectionConfig, protocol.`type`.toString, sinkSpecificConfig, producerGroup)
      case "userDefinedJson" =>
        logger.error("not support yet")
      case _ =>
        logger.error("cannot recognize " + format)
    }
  }


  private def flattenJson2RocketMQWithSystemValue(tupleList: Seq[Seq[String]],
                                               schemaList: Seq[(String, (Int, UmsFieldType, Boolean))],
                                               sinkNamespace: String,
                                               mqTopic: String,
                                               connectionConfig: ConnectionConfig,
                                               protocol: String,
                                               sinkSpecificConfig: RocketMQConfig,
                                               producerGroup: String): Unit = {
    val mqMessages = tupleList.map(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        val umsFieldType = schemaList(index)._2._2
        if (umsFieldType == DATETIME || umsFieldType == DATE)
          flattenJson.put(schemaList(index)._1, t)
        else flattenJson.put(schemaList(index)._1, UmsFieldType.umsFieldValue(t, schemaList(index)._2._2))
        index += 1
      })
      flattenJson.put("namespace", sinkNamespace)
      flattenJson.put("protocol", protocol)
      val mqMessage = MqMessage(mqTopic, JSON.toJSONString(flattenJson, SerializerFeature.WriteMapNullValue), sinkSpecificConfig.tags)
      WormholeRocketMQProducer.genMqMessage(mqMessage)
    })
    WormholeRocketMQProducer.send(connectionConfig.connectionUrl, producerGroup, mqMessages)
  }


  private def flattenJson2RocketMQWithoutSystemValue(tupleList: Seq[Seq[String]],
                                                  schemaList: Seq[(String, (Int, UmsFieldType, Boolean))],
                                                  sinkNamespace: String,
                                                  mqTopic: String,
                                                  connectionConfig: ConnectionConfig,
                                                  protocol: String,
                                                  sinkSpecificConfig: RocketMQConfig,
                                                  producerGroup: String): Unit = {
    val mqMessages = tupleList.map(tuple => {
      val flattenJson = new JSONObject
      var index = 0
      tuple.foreach(t => {
        if (!schemaList(index)._1.startsWith("ums_")) {
          val umsFieldType = schemaList(index)._2._2
          if (umsFieldType == DATETIME || umsFieldType == DATE)
            flattenJson.put(schemaList(index)._1, t)
          else flattenJson.put(schemaList(index)._1, UmsFieldType.umsFieldValue(t, schemaList(index)._2._2))
        }
        index += 1
      })
      val mqMessage = MqMessage(mqTopic, JSON.toJSONString(flattenJson, SerializerFeature.WriteMapNullValue), sinkSpecificConfig.tags)
      WormholeRocketMQProducer.genMqMessage(mqMessage)
    })
    WormholeRocketMQProducer.send(connectionConfig.connectionUrl, producerGroup, mqMessages)
  }


  private def ums2RocketMQ(tupleList: Seq[Seq[String]],
                           mqLimitNum: Int,
                           protocol: UmsProtocol,
                           schema: UmsSchema,
                           mqTopic: String,
                           connectionConfig: ConnectionConfig,
                           sinkSpecificConfig: RocketMQConfig,
                           producerGroup: String): Unit = {
    logger.info(s"start write to rocketMQ, tupleList size is: ${tupleList.size}")
    val mqMessages: Iterator[Message] = tupleList.sliding(mqLimitNum, mqLimitNum).map(tuple => {
      val seqUmsTuple: Seq[UmsTuple] = tuple.map(payload => UmsTuple(payload))
      //logger.info(s"start write to rocketMQ, seqUmsTuple size is: ${seqUmsTuple.size}")
      val umsMessage: String = toJsonCompact(Ums(
        protocol,
        schema,
        payload = Some(seqUmsTuple)))
      val mqMessage = MqMessage(mqTopic, umsMessage, sinkSpecificConfig.tags)
      WormholeRocketMQProducer.genMqMessage(mqMessage)
    })
    WormholeRocketMQProducer.send(connectionConfig.connectionUrl, producerGroup, mqMessages.toSeq)
  }


}
