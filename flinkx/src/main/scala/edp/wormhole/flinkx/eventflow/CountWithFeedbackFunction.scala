package edp.wormhole.flinkx.eventflow



import java.util.UUID

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.common.json.FieldInfo
import edp.wormhole.flinkx.common.{KafkaTopicConfig, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.FlinkSchemaUtils._
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.{UmsProtocolUtils, _}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.util.{DateUtils, JsonUtils}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CountWithFeedbackFunction(sourceSchemaMap: Map[String, (TypeInformation[_], Int)], sourceNamespace: String,sinkNamespace: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])],config:WormholeFlinkxConfig,streamId:Long,initialTs:Long) extends  RichMapFunction[(String,String),(String,String)] with Serializable{

//  lazy val state:ValueState[ListBuffer[Ums]]=getRuntimeContext.getState(new ValueStateDescriptor[ListBuffer[Ums]]("feedbackState",classOf[ListBuffer[Ums]]))
  lazy val mapState:MapState[String,ListBuffer[Ums]]=getRuntimeContext.getMapState(new MapStateDescriptor[String,ListBuffer[Ums]]("feedbackState",classOf[String],classOf[ListBuffer[Ums]]))

  @transient lazy val logger = Logger.getLogger(this.getClass)

  override def map(value:(String,String))={
    logger.info("ums feedback start=======================")
    WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config)

    val ums=UmsCommonUtils.json2Ums(value._2)

    val buffer=mapState.get(ums.protocol.`type`.toString) match{
      case null =>new ListBuffer[Ums]()
      case _=>mapState.get(ums.protocol.`type`.toString)
    }
    logger.info("buffer:"+buffer.toString)
    if(buffer.size>=config.feedback_state_count){
      logger.info("feedback message send start==========")
      val mainData=buffer.take(config.feedback_state_count).filterNot(ums=>checkOtherData(ums.protocol.`type`.toString))
      val payloadSize=mainData.map(tuple=>tuple.payload.size).reduce(_+_)
      val topics=config.kafka_input.kafka_topics.map(config=>JsonUtils.jsonCompact(JsonUtils.caseClass2json[KafkaTopicConfig](config))).mkString("[",",","]")

      val umsTs=UmsFieldType.umsFieldValue(mainData.head.payload.get.head.tuple,mainData.head.schema.fields.get,"ums_ts_").asInstanceOf[DateTime].getMillis
      val batchId = UUID.randomUUID().toString

      logger.info("topic:"+config.kafka_output.feedback_topic_name+",brokers:"+config.kafka_output.brokers)

      WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
        UmsProtocolUtils.feedbackFlowStats(sourceNamespace,ums.protocol.`type`.toString,DateUtils.currentDateTime,streamId,batchId,sinkNamespace,payloadSize,umsTs,umsTs,initialTs,initialTs,initialTs,initialTs,initialTs), None, config.kafka_output.brokers)

      buffer.remove(0,config.feedback_state_count)
      logger.info("feedback message send successfully===========")
    }

    buffer+=ums
    mapState.put(ums.protocol.`type`.toString,buffer)

    value
  }
}
