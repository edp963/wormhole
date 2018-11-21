/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package edp.wormhole.flinkx.eventflow

import java.util.{Date, UUID}

import edp.wormhole.common.json.{FieldInfo, JsonParseUtils}
import edp.wormhole.flinkx.common.{KafkaTopicConfig, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsCommonUtils, UmsField, UmsFieldType}
import edp.wormhole.util.JsonUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup
import org.apache.flink.runtime.metrics.{MetricRegistryConfiguration, MetricRegistryImpl}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class UmsFlatMapper(sourceSchemaMap: Map[String, (TypeInformation[_], Int)], config:WormholeFlinkxConfig, sourceNamespace: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])], sinkNamespace:String,streamId:Long,outputTag:OutputTag[String]) extends ProcessFunction[(String, String, String, Int, Long), Row] with Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)
  @transient private var lastTopicInfo:String=""
//  @transient private var summary:Counter[Int]
  @transient private var firstUmsTs:Long=0L
  @transient private var lastUmsTs:Long=0L

  override def processElement(value: (String, String, String, Int, Long), ctx: ProcessFunction[(String, String, String, Int, Long), Row]#Context, out: Collector[Row]): Unit = {
    logger.info("in UmsFlatMapper source data from kafka " + value._2)
    val (protocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(value._1)
    startMetricsMoinitoring(protocolType.toString)
    if (jsonSourceParseMap.contains((protocolType, namespace))) {
      val mapValue: (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)]) = jsonSourceParseMap((protocolType, namespace))
      val umsTuple = JsonParseUtils.dataParse(value._2, mapValue._2, mapValue._3)
      umsTuple.foreach(tuple => {
        createRow(tuple.tuple, protocolType.toString, out,mapValue._1)
      })
    }
    else {
      val ums = UmsCommonUtils.json2Ums(value._2)
      logger.info("in UmsFlatMapper " + sourceSchemaMap.size)
      if (FlinkSchemaUtils.matchNamespace(ums.schema.namespace, sourceNamespace) && ums.payload.nonEmpty && ums.schema.fields.nonEmpty)
        ums.payload_get.foreach(tuple => {
          createRow(tuple.tuple, protocolType.toString, out, ums.schema.fields.get)
        })
    }
    ctx.output(outputTag,value._2)
  }
  def createRow(tuple: Seq[String], protocolType:String, out: Collector[Row],schema:Seq[UmsField]): Unit = {
    val row = new Row(tuple.size)
    for (i <- tuple.indices)
      row.setField(i, FlinkSchemaUtils.getRelValue(i, tuple(i), sourceSchemaMap))
    out.collect(row)
    moinitorRow(tuple,protocolType,schema)
  }

  def moinitorRow(tuple: Seq[String], protocolType:String, schema:Seq[UmsField]):Unit={
    val umsTs=UmsFieldType.umsFieldValue(tuple,schema,"ums_ts_").asInstanceOf[DateTime].getMillis

   // summary.inc()
    lastTopicInfo=config.kafka_input.kafka_topics.map(config=>JsonUtils.jsonCompact(JsonUtils.caseClass2json[KafkaTopicConfig](config))).mkString("[",",","]")
    if(firstUmsTs==0L||umsTs<firstUmsTs)firstUmsTs=umsTs
    if(lastUmsTs==0L||umsTs>lastUmsTs)lastUmsTs=umsTs
  }

  def startMetricsMoinitoring(protocolType:String):Unit={
    val mConfig=new Configuration()
    mConfig.setString("metrics.reporters","feedbackState")
    mConfig.setString("metrics.reporter.feedbackState.class","edp.wormhole.flinkx.util.FeedbackMetricsReporter")
    mConfig.setString("metrics.reporter.feedbackState.interval","60 SECONDS")
    mConfig.setString("metrics.scope.delimiter",".")

    val metricConfig=MetricRegistryConfiguration.fromConfiguration(mConfig)
    val registry=new MetricRegistryImpl(metricConfig)

    //    val metricsGroup=new FeedbackMetricGroup(registry,getRuntimeContext.getMetricGroup)
//
//
//    summary=metricsGroup.counter("summary")
//    metricsGroup.gauge[String,ScalaGauge[String]]("lastTopicOffset",ScalaGauge(()=>protocolType+"~"+lastTopicInfo+"~"+firstUmsTs+"~"+lastUmsTs))
  }
}
