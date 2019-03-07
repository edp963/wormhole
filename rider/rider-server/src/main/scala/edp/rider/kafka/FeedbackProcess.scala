/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
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


package edp.rider.kafka

import java.util.Date

import edp.rider.RiderStarter.modules
import edp.rider.common.FlowStatus._
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils
import edp.rider.rest.util.CommonUtils._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums._
import edp.wormhole.util.{DateUtils, DtFormat}

import scala.concurrent.Await

object FeedbackProcess extends RiderLogger {

  @Deprecated
  def doFeedbackHeartbeat(message: Ums) = {
    val protocolType: String = message.protocol.`type`.toString
    val srcNamespace: String = message.schema.namespace.toLowerCase
    val curTs = currentMillSec
    riderLogger.debug("start process FeedbackHeartbeat feedback")
    val fields = message.schema.fields_get
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        if (umsTsValue != null && streamIdValue != null) {
          val feedbackHeartbeat = FeedbackHeartbeat(1, protocolType.toString, streamIdValue.toString.toLong, srcNamespace, umsTsValue.toString, curTs)
          riderLogger.debug(s"FeedbackHeartbeat: $feedbackHeartbeat")
          Await.result(modules.feedbackHeartbeatDal.insert(feedbackHeartbeat), minTimeOut)
        } else {
          riderLogger.error(s"FeedbackHeartbeat can't found the value", tuple)
        }
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackHeartbeat feedback message", e)
    }
  }

  @Deprecated
  def doFeedbackDirective(records: List[Ums]): Unit = {
    records.foreach(message => {
      val fields = message.schema.fields_get
      try {
        message.payload_get.foreach(tuple => {
          val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
          val directiveIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_id")
          val statusValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status")
          val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
          val resultDescValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc")
          if (umsTsValue != null && directiveIdValue != null && statusValue != null && streamIdValue != null && resultDescValue != null) {
            modules.directiveDal.getDetail(directiveIdValue.toString.toLong) match {
              case Some(records) =>
                val pType: UmsProtocolType.Value = UmsProtocolType.umsProtocolType(records.protocolType.toString)
                pType match {
                  case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START | UmsProtocolType.DIRECTIVE_ROUTER_FLOW_START =>
                    if (statusValue.toString == UmsFeedbackStatus.SUCCESS.toString) {
                      modules.flowDal.updateStatusByFeedback(records.flowId, RUNNING.toString)
                    } else
                      modules.flowDal.updateStatusByFeedback(records.flowId, FAILED.toString)
                  case UmsProtocolType.DIRECTIVE_FLOW_STOP =>
                    if (statusValue.toString == UmsFeedbackStatus.SUCCESS.toString)
                      modules.flowDal.updateStatusByFeedback(records.flowId, STOPPED.toString)
                    else
                      modules.flowDal.updateStatusByFeedback(records.flowId, FAILED.toString)
                  case _ => riderLogger.debug(s"$pType not supported now.")
                }
              case None => riderLogger.warn(s"directive id doesn't exist.")
            }
          } else {
            riderLogger.error(s"FeedbackDirective can't found the value", tuple)
          }
        })
      }
      catch {
        case e: Exception =>
          riderLogger.error(s"Failed to process FeedbackDirective feedback message: $message")
          riderLogger.error(s"Failed to process FeedbackDirective feedback message", e)
      }
    })
  }

  def doSparkxFlowError(records: List[Ums]): Unit = {
    try {
      val insertSeq = records.flatMap(record => {
        val protocolType = record.protocol.`type`.toString
        val srcNamespace: String = record.schema.namespace.toLowerCase
        val fields = record.schema.fields_get
        val curTs = currentMillSec
        record.payload_get.map(tuple => {
          val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
          val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
          val flowIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "flow_id")
          val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace")
          val errMaxWaterMarkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_max_watermark_ts")
          val errMinWaterMarkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_min_watermark_ts")
          val errorCountValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_count")
          val errorInfoValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_info").toString
          val topics =
            if (UmsFieldType.umsFieldValue(tuple.tuple, fields, "topics") != null)
              UmsFieldType.umsFieldValue(tuple.tuple, fields, "topics").toString
            else null
/*          FeedbackFlowErr(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong,
            srcNamespace, sinkNamespaceValue.toString, errorCountValue.toString.toInt,
            errMaxWaterMarkTsValue.toString, errMinWaterMarkTsValue.toString,
            errorInfoValue.toString, topics, curTs)*/
//          FeedbackErr(1,1,streamIdValue.toString.toLong,flowIdValue.toString.toLong,srcNamespace,sinkNamespaceValue,,"sparkx",topics,errorCountValue.toString.toInt,
//            errMaxWaterMarkTsValue.toString,  errMinWaterMarkTsValue.toString,errorInfoValue,umsTsValue.toString,(new Date()).toString)
        })
      })
      Await.result(modules.feedbackErrDal.insert(insertSeq), minTimeOut)
    } catch {
      case ex: Exception =>
        records.foreach(record => riderLogger.error(s"-----$record------"))
        riderLogger.error(s"process $FEEDBACK_SPARKX_FLOW_ERROR message failed", ex)
    }
  }

/*  def doStreamBatchError(records: List[Ums]): Unit = {
    try {
      val insertSeq = records.flatMap(record => {
        val protocolType = record.protocol.`type`.toString
        val fields = record.schema.fields_get
        val curTs = currentMillSec
        record.payload_get.map(tuple => {
          val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
          val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
          val statusValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status")
          val resultDescValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc")
          val topics =
            if (UmsFieldType.umsFieldValue(tuple.tuple, fields, "topics") != null)
              UmsFieldType.umsFieldValue(tuple.tuple, fields, "topics").toString
            else null
          FeedbackStreamErr(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong,
            statusValue.toString, resultDescValue.toString, topics, curTs)
        })
      })
      Await.result(modules.feedbackErrDal.insert(insertSeq), minTimeOut)
    } catch {
      case ex: Exception => riderLogger.error(s"process $FEEDBACK_STREAM_BATCH_ERROR message failed", ex)
    }
  }*/

  @Deprecated
  def doFeedbackStreamTopicOffset(message: Ums): Unit = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    riderLogger.debug("start process FeedbackStreamTopicOffset feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val topicNameValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name")
        val partitionOffsetValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets")
        if (umsTsValue != null && streamIdValue != null && topicNameValue != null && partitionOffsetValue != null) {
          val partitionOffset = partitionOffsetValue.toString
          val partitionNum: Int = KafkaUtils.getPartNumByOffset(partitionOffset)
          Await.result(modules.feedbackOffsetDal.insert(FeedbackOffset(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong,
            topicNameValue.toString, partitionNum, partitionOffset, currentMicroSec)), minTimeOut)
        } else {
          riderLogger.error(s"FeedbackStreamTopicOffset can't found the value", tuple)
        }
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackStreamTopicOffset feedback message ${
          message
        }", e)
    }
  }

  def doSparkxFlowStats(records: List[Ums]): Unit = {
    try {
      val insertSeq = records.flatMap(record => {
        val srcNamespace = record.schema.namespace
        val riderNamespace = namespaceRiderString(srcNamespace)
        val fields = record.schema.fields_get
        var throughput: Long = 0
        record.payload_get.map(tuple => {
          val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
          val flowIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "flow_id")
          val batchIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "batch_id")
          val dataTypeValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_type")
          val topics = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topics")
          val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace").toString
          val rddCountValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_count").toString.toInt
          val feedbackTime =  UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
          //todo 兼容0.6.0及之前版本stream feedback数据
          val cdcTsValue =
            if (UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_generated_ts") != null)
              UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_generated_ts").toString.toLong
            else UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_genereated_ts").toString.toLong
          val rddTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_generated_ts").toString.toLong
         val mainDataTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_process_start_ts").toString.toLong
          val swiftsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "swifts_start_ts").toString.toLong
          val sinkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_start_ts").toString.toLong
          val doneTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "done_ts").toString.toLong

          val riderSinkNamespace = if (sinkNamespaceValue.toString == "") riderNamespace else namespaceRiderString(sinkNamespaceValue.toString)

          val interval_data_process_dataums = (mainDataTsValue.toString.toLong - cdcTsValue.toString.toLong) / 1000
          val interval_data_process_rdd = (rddTsValue.toString.toLong - mainDataTsValue.toString.toLong) / 1000
          val interval_data_process_done = (doneTsValue.toString.toLong - mainDataTsValue.toString.toLong) / 1000
          val interval_rdd_swifts = (swiftsTsValue.toString.toLong - rddTsValue.toString.toLong) / 1000
          val interval_rdd_done = (doneTsValue.toString.toLong - rddTsValue.toString.toLong) / 1000
          val interval_data_swifts_sink = (sinkTsValue.toString.toLong - swiftsTsValue.toString.toLong) / 1000
          val interval_data_sink_done = (doneTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000

          if (interval_rdd_done == 0L) {
            throughput = rddCountValue.toString.toInt
          } else throughput = rddCountValue.toString.toInt / interval_rdd_done

          val monitorInfo = MonitorInfo(0L, batchIdValue.toString,
            streamIdValue.toString.toLong,flowIdValue.toString.toLong,
            riderNamespace,riderSinkNamespace,dataTypeValue,
            rddCountValue.toString.toInt, if (topics == null) "" else topics.toString, throughput,
            string2EsDateString(DateUtils.dt2string(cdcTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            string2EsDateString(DateUtils.dt2string(rddTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            string2EsDateString(DateUtils.dt2string(mainDataTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            string2EsDateString(DateUtils.dt2string(swiftsTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            string2EsDateString(DateUtils.dt2string(sinkTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            string2EsDateString(DateUtils.dt2string(doneTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC)),
            Interval(interval_data_process_dataums, interval_data_process_rdd, interval_rdd_swifts,  interval_data_process_done,
                interval_data_swifts_sink, interval_data_sink_done),feedbackTime,new Date())
          monitorInfo
        })
      })
      if (RiderConfig.monitor.databaseType.toLowerCase.equals("es"))
        ElasticSearch.insertFlowStatToES(insertSeq)
      else Await.result(modules.monitorInfoDal.insert(insertSeq), minTimeOut)
    } catch {
      case ex: Exception => riderLogger.error(s"process $FEEDBACK_SPARKX_FLOW_STATS message failed", ex)
    }
  }


  private def namespaceRiderString(ns: String): String = {
    val array = ns.split("\\.")
    List(array(0), array(1), array(2), array(3), "*", "*", "*").mkString(".")
  }

  private def string2EsDateString(string: String): String = {
    string.concat(CommonUtils.getTimeZoneId)
  }
}
