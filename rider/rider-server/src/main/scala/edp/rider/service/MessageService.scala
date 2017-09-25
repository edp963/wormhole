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


package edp.rider.service

import edp.rider.common.FlowStatus._
import edp.rider.common.{RiderLogger, TopicPartitionOffset}
import edp.rider.module.{ConfigurationModule, PersistenceModule}
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.service.util.{CacheMap, FeedbackOffsetUtil}
import edp.wormhole.common.util.{DateUtils, DtFormat}
import edp.wormhole.ums._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

class MessageService(modules: ConfigurationModule with PersistenceModule) extends RiderLogger {

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
        if(umsTsValue != null && streamIdValue != null) {
          val feedbackHeartbeat = FeedbackHeartbeat(1, protocolType.toString, streamIdValue.toString.toLong, srcNamespace, umsTsValue.toString, curTs)
          riderLogger.debug(s" FeedbackHeartbeat: $feedbackHeartbeat")
          val future = modules.feedbackHeartbeatDal.insert(feedbackHeartbeat)
          val result = Await.ready(future, minTimeOut).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackHeartbeat inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackHeartbeat inserted success.")
          }
        }else { riderLogger.error(s"FeedbackHeartbeat can't found the value", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackHeartbeat feedback message", e)
    }
  }

  def doFeedbackDirective(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val curTs = currentMillSec
    riderLogger.debug("start process FeedbackDirective feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val directiveIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_id")
        val statusValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val resultDescValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc")
        if( umsTsValue != null && directiveIdValue != null && statusValue != null && streamIdValue != null && resultDescValue != null) {
          val future = modules.feedbackDirectiveDal.insert(FeedbackDirective(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong, directiveIdValue.toString.toLong, statusValue.toString, resultDescValue.toString, curTs))
          val result = Await.ready(future, minTimeOut).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackDirective inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackDirective inserted success.")
          }
          Await.result(modules.directiveDal.findById(directiveIdValue.toString.toLong), minTimeOut) match {
            case Some(records) =>
              val pType: UmsProtocolType.Value = UmsProtocolType.umsProtocolType(records.protocolType.toString)
              pType match {
                case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START =>
                  if (statusValue.toString == UmsFeedbackStatus.SUCCESS.toString) {
                    modules.flowDal.updateFlowStatus(records.flowId, RUNNING.toString)
                  } else
                    modules.flowDal.updateFlowStatus(records.flowId, FAILED.toString)
                case UmsProtocolType.DIRECTIVE_FLOW_STOP =>
                  if (statusValue.toString == UmsFeedbackStatus.SUCCESS.toString)
                    modules.flowDal.updateFlowStatus(records.flowId, STOPPED.toString)
                  else
                    modules.flowDal.updateFlowStatus(records.flowId, FAILED.toString)
                case _ => riderLogger.error(s"$pType not supported now.")
              }
            case None => riderLogger.warn(s"directive id doesn't exist.")
          }
        }else { riderLogger.error(s"FeedbackDirective can't found the value", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackDirective feedback message: $message")
        riderLogger.error(s"Failed to process FeedbackDirective feedback message", e)
    }
  }

  def doFeedbackFlowError(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val srcNamespace: String = message.schema.namespace.toLowerCase
    val fields = message.schema.fields_get
    val curTs = currentMillSec
    riderLogger.debug("start process FeedbackFlowError feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace")
        val errMaxWaterMarkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_max_watermark_ts")
        val errMinWaterMarkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_min_watermark_ts")
        val errorCountValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_count")
        val errorInfoValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_info").toString
        if(umsTsValue != null && streamIdValue != null && sinkNamespaceValue != null && errMaxWaterMarkTsValue != null && errMinWaterMarkTsValue != null && errorCountValue  != null && errorInfoValue != null){
          val future = modules.feedbackFlowErrDal.insert(FeedbackFlowErr(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong, srcNamespace, sinkNamespaceValue.toString, errorCountValue.toString.toInt, errMaxWaterMarkTsValue.toString, errMinWaterMarkTsValue.toString, errorInfoValue.toString, curTs))
          val result = Await.ready(future, minTimeOut).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackFlowError inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackFlowError inserted success.")
          }
        }else { riderLogger.error(s"FeedbackFlowError can't found the value", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackFlowError feedback message", e)
    }
  }

  def doFeedbackStreamBatchError(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val curTs = currentMillSec
    riderLogger.debug("start process FeedbackStreamBatchError feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val statusValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status")
        val resultDescValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc")
        if(umsTsValue != null && streamIdValue != null && statusValue != null && resultDescValue != null) {
          val future = modules.feedbackStreamErrDal.insert(FeedbackStreamErr(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong, statusValue.toString, resultDescValue.toString, curTs))
          val result = Await.ready(future, Duration.Inf).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackStreamBatchError inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackStreamBatchError inserted success.")
          }
        }else { riderLogger.error(s"FeedbackStreamBatchError can't found the value", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackStreamBatchError feedback message", e)
    }
  }

  private def getTopicPartitionList(topicPartition: String): List[TopicPartitionOffset] = {
    val l: ListBuffer[TopicPartitionOffset] = new ListBuffer()
    topicPartition.split("\\)\\(").foreach {
      arr =>
        val records = arr.mkString.replaceAll("\\)", "").replaceAll("\\(", "").split("\\,")
        if (records.length > 1)
          l.append(TopicPartitionOffset(records.apply(0), records.apply(1).toInt, records.apply(2).toLong))
    }
    l.toList
  }



  def doFeedbackStreamTopicOffset(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val curTs = currentMillSec
    riderLogger.debug("start process FeedbackStreamTopicOffset feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val topicNameValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name")
        val partitionOffsetValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets")
        if(umsTsValue != null && streamIdValue != null && topicNameValue != null && partitionOffsetValue != null) {
          val partitionOffset = partitionOffsetValue.toString
          val partitionNum: Int = FeedbackOffsetUtil.getPartitionNumber(partitionOffset)
          val future = modules.feedbackOffsetDal.insert(FeedbackOffset(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong,
            topicNameValue.toString, partitionNum, partitionOffset, curTs))
          val result = Await.ready(future, Duration.Inf).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackStreamTopicOffset inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackStreamTopicOffset inserted success.")
          }
        }else { riderLogger.error(s"FeedbackStreamTopicOffset can't found the value", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to process FeedbackStreamTopicOffset feedback message ${message}", e)
    }
  }

  def namespaceRiderString(ns: String): String = {
    val array = ns.split("\\.")
    List(array(0), array(1), array(2), array(3), "*", "*", "*").mkString(".")
  }

  def doFeedbackFlowStats(message: Ums) = {
    val srcNamespace = message.schema.namespace.toLowerCase
    val riderNamespace = namespaceRiderString(srcNamespace)
    val fields = message.schema.fields_get
    var throughput: Long = 0
    riderLogger.debug("start process FeedbackFlowStats feedback")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val statsIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stats_id")
        val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace").toString
        val rddCountValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_count").toString.toInt
        val cdcTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_genereated_ts").toString.toLong
        val rddTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_generated_ts").toString.toLong
        val directiveTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_process_start_ts").toString.toLong
        val mainDataTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_process_start_ts").toString.toLong
        val swiftsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "swifts_start_ts").toString.toLong
        val sinkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_start_ts").toString.toLong
        val doneTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "done_ts").toString.toLong
        if(umsTsValue != null && streamIdValue != null && statsIdValue != null && sinkNamespaceValue != null && rddCountValue != null && cdcTsValue != null && rddTsValue  != null &&
          directiveTsValue != null && mainDataTsValue != null && swiftsTsValue != null && sinkTsValue != null && doneTsValue != null) {
          val riderSinkNamespace = if (sinkNamespaceValue.toString == "") riderNamespace else namespaceRiderString(sinkNamespaceValue.toString)
          val flowName = s"${riderNamespace}_${riderSinkNamespace}"
          val interval_data_process_dataums = (mainDataTsValue.toString.toLong - cdcTsValue.toString.toLong) / 1000
          val interval_data_process_rdd = (mainDataTsValue.toString.toLong - rddTsValue.toString.toLong) / 1000
          val interval_data_process_swifts = (mainDataTsValue.toString.toLong - swiftsTsValue.toString.toLong) / 1000
          val interval_data_process_sink = (mainDataTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000
          val interval_data_process_done = (mainDataTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000
          val interval_rdd_done: Long = (doneTsValue.toString.toLong - rddTsValue.toString.toLong) / 1000
          val interval_data_swifts_sink = (swiftsTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000
          val interval_data_sink_done = (sinkTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000

          if (interval_rdd_done == 0L) {
            throughput = rddCountValue.toString.toInt
          } else throughput = rddCountValue.toString.toInt / interval_rdd_done

          val monitorInfo = MonitorInfo(statsIdValue.toString, umsTsValue.toString, CacheMap.getProjectId(streamIdValue.toString.toLong), streamIdValue.toString.toLong, CacheMap.getStreamName(streamIdValue.toString.toLong), CacheMap.getFlowId(flowName), flowName, rddCountValue.toString.toInt, throughput,
            DateUtils.dt2string(cdcTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(rddTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(directiveTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(mainDataTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(swiftsTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(sinkTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            DateUtils.dt2string(doneTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
            interval_data_process_dataums, interval_data_process_rdd, interval_data_process_swifts, interval_data_process_sink, interval_data_process_done,
            interval_data_process_done, interval_data_swifts_sink, interval_data_sink_done)
          ElasticSearch.insertFlowStatToES(monitorInfo)
        }else {riderLogger.error(s"Failed to get value from FeedbackFlowStats", tuple)}
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to parse FeedbackFlowStats feedback message", e)
    }
  }
}
