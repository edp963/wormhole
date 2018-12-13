package edp.wormhole.flinkx.eventflow

import java.io.Serializable

import edp.wormhole.common.json.{FieldInfo, JsonParseUtils}
import edp.wormhole.flinkx.common.{ExceptionConfig, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.ums.{UmsCommonUtils, UmsField, UmsFieldType, UmsProtocolUtils}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.SimpleCounter
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup
import org.apache.flink.runtime.metrics.{MetricRegistryConfiguration, MetricRegistryImpl}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

class UmsProcessElement(sourceSchemaMap: Map[String, (TypeInformation[_], Int)], config:WormholeFlinkxConfig, exceptionConfig: ExceptionConfig, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])], kafkaDataTag: OutputTag[String], mConfig:Configuration) extends ProcessFunction[(String, String, String, Int, Long), Row] {
  //private val outputTag = OutputTag[String]("kafkaDataException")
  private lazy val logger = Logger.getLogger(this.getClass)

  private lazy val registry=new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(mConfig)) with Serializable

  private lazy val metricsGroup=new TaskManagerMetricGroup(registry,"localhost","tmId") with Serializable

  private lazy val summary=new SimpleCounter() with Serializable

  @transient private var lastTopicInfo:String=""

  @transient private var firstUmsTs:Long=0L
  @transient private var lastUmsTs:Long=0L

  override def processElement(value: (String, String, String, Int, Long), ctx: ProcessFunction[(String, String, String, Int, Long), Row]#Context, out: Collector[Row]): Unit = {
    logger.info("in UmsProcessElement source data from kafka " + value._2)
    try {
      val (protocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(value._1)
      if(config.feedback_enabled)startMetricsMoinitoring(protocolType.toString)
      if (jsonSourceParseMap.contains((protocolType, namespace))) {
        val mapValue: (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)]) = jsonSourceParseMap((protocolType, namespace))
        val umsTuple = JsonParseUtils.dataParse(value._2, mapValue._2, mapValue._3)
        umsTuple.foreach(tuple => {
          createRow(tuple.tuple, protocolType.toString, out,mapValue._1)
        })
      }
      else {
        val ums = UmsCommonUtils.json2Ums(value._2)
        logger.info("in UmsProcessElement " + sourceSchemaMap.size)
        if (FlinkSchemaUtils.matchNamespace(ums.schema.namespace, exceptionConfig.sourceNamespace) && ums.payload.nonEmpty && ums.schema.fields.nonEmpty)
          ums.payload_get.foreach(tuple => {
            createRow(tuple.tuple, protocolType.toString, out,ums.schema.fields.get)
          })
      }
    } catch {
      case ex: Throwable =>
        logger.error("in doFlinkSql table query", ex)
        //out.collect(new Row(0))
        ctx.output(kafkaDataTag, UmsProtocolUtils.feedbackFlowFlinkxError(exceptionConfig.sourceNamespace, exceptionConfig.streamId, exceptionConfig.flowId, exceptionConfig.sinkNamespace, new DateTime(), value._2, ex.getMessage))
    }
  }

  def createRow(tuple: Seq[String], protocolType: String, out: Collector[Row],schema:Seq[UmsField]): Unit = {
    val row = new Row(tuple.size)
    for (i <- tuple.indices)
      row.setField(i, FlinkSchemaUtils.getRelValue(i, tuple(i), sourceSchemaMap))
    out.collect(row)
    if(config.feedback_enabled)moinitorRow(tuple,protocolType,schema)
  }

  def moinitorRow(tuple: Seq[String], protocolType:String, schema:Seq[UmsField]):Unit={
    val umsTs=UmsFieldType.umsFieldValue(tuple,schema,"ums_ts_").asInstanceOf[DateTime].getMillis

    summary.inc()
    lastTopicInfo=config.kafka_input.kafka_topics.map(config=>config.topic_name+":"+config.topic_partition).mkString("[",",","]")
    if(firstUmsTs==0L||umsTs<firstUmsTs)firstUmsTs=umsTs
    if(lastUmsTs==0L||umsTs>lastUmsTs)lastUmsTs=umsTs
  }

  def startMetricsMoinitoring(protocolType:String):Unit={
    metricsGroup.counter("summary",summary)
    metricsGroup.gauge[String,ScalaGauge[String]]("lastTopicOffset",ScalaGauge(()=>protocolType+"~"+lastTopicInfo+"~"+firstUmsTs+"~"+lastUmsTs))
  }
}

