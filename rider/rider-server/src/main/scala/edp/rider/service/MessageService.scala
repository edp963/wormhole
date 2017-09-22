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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val feedbackHeartbeat = FeedbackHeartbeat(1, protocolType.toString, streamId, srcNamespace, umsTs, curTs)
        riderLogger.debug(s" FeedbackHeartbeat: $feedbackHeartbeat")
        val future = modules.feedbackHeartbeatDal.insert(feedbackHeartbeat)
        val result = Await.ready(future, minTimeOut).value.get
        result match {
          case Failure(e) =>
            riderLogger.error(s"FeedbackHeartbeat inserted ${tuple.toString} failed", e)
          case Success(t) => riderLogger.debug("FeedbackHeartbeat inserted success.")
        }
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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_id").toString.toLong
        val status = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val resultDesc = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc").toString
        val future = modules.feedbackDirectiveDal.insert(FeedbackDirective(1, protocolType.toString, umsTs, streamId, directiveId, status, resultDesc, curTs))
        val result = Await.ready(future, minTimeOut).value.get
        result match {
          case Failure(e) =>
            riderLogger.error(s"FeedbackDirective inserted ${tuple.toString} failed", e)
          case Success(t) => riderLogger.debug("FeedbackDirective inserted success.")
        }
        Await.result(modules.directiveDal.findById(directiveId), minTimeOut) match {
          case Some(records) =>
            val pType: UmsProtocolType.Value = UmsProtocolType.umsProtocolType(records.protocolType.toString)
            pType match {
              case UmsProtocolType.DIRECTIVE_FLOW_START | UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START =>
                if (status == UmsFeedbackStatus.SUCCESS.toString) {
                  modules.flowDal.updateFlowStatus(records.flowId, RUNNING.toString)
                } else
                  modules.flowDal.updateFlowStatus(records.flowId, FAILED.toString)
              case UmsProtocolType.DIRECTIVE_FLOW_STOP =>
                if (status == UmsFeedbackStatus.SUCCESS.toString)
                  modules.flowDal.updateFlowStatus(records.flowId, STOPPED.toString)
                else
                  modules.flowDal.updateFlowStatus(records.flowId, FAILED.toString)
              case _ => riderLogger.error(s"$pType not supported now.")
            }
          case None => riderLogger.warn(s"directive $directiveId doesn't exist.")
        }
      })
    } catch {
      case e: Exception =>
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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val sinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace").toString
        val errMaxWaterMarkTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_max_watermark_ts").toString
        val errMinWaterMarkTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_min_watermark_ts").toString
        val errorCount = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_count").toString.toInt
        val errorInfo = UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_info").toString
        val future = modules.feedbackFlowErrDal.insert(FeedbackFlowErr(1, protocolType.toString, umsTs, streamId, srcNamespace, sinkNamespace, errorCount, errMaxWaterMarkTs, errMinWaterMarkTs, errorInfo, curTs))
        val result = Await.ready(future, minTimeOut).value.get
        result match {
          case Failure(e) =>
            riderLogger.error(s"FeedbackFlowError inserted ${tuple.toString} failed", e)
          case Success(t) => riderLogger.debug("FeedbackFlowError inserted success.")
        }
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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val status = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status").toString
        val resultDesc = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc").toString
        val future = modules.feedbackStreamErrDal.insert(FeedbackStreamErr(1, protocolType.toString, umsTs, streamId, status, resultDesc, curTs))
        val result = Await.ready(future, Duration.Inf).value.get
        result match {
          case Failure(e) =>
            riderLogger.error(s"FeedbackStreamBatchError inserted ${tuple.toString} failed", e)
          case Success(t) => riderLogger.debug("FeedbackStreamBatchError inserted success.")
        }
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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val topicName = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name").toString
        val partitionOffset = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets").toString
        val partitionNum: Int = FeedbackOffsetUtil.getPartitionNumber(partitionOffset)
        val future = modules.feedbackOffsetDal.insert(FeedbackOffset(1, protocolType.toString, umsTs, streamId,
          topicName, partitionNum, partitionOffset, curTs))
        val result = Await.ready(future, Duration.Inf).value.get
        result match {
          case Failure(e) =>
            riderLogger.error(s"FeedbackStreamTopicOffset inserted ${tuple.toString} failed", e)
          case Success(t) => riderLogger.debug("FeedbackStreamTopicOffset inserted success.")
        }
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
        val umsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_").toString
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id").toString.toLong
        val statsId = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stats_id").toString
        val sinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace").toString
        val riderSinkNamespace = if (sinkNamespace == "") riderNamespace else namespaceRiderString(sinkNamespace)
        val flowName = s"${riderNamespace}_${riderSinkNamespace}"
        val rddCount = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_count").toString.toInt
        val cdcTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_genereated_ts").toString.toLong
        val rddTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_generated_ts").toString.toLong
        val directiveTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_process_start_ts").toString.toLong
        val mainDataTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_process_start_ts").toString.toLong
        val swiftsTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "swifts_start_ts").toString.toLong
        val sinkTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_start_ts").toString.toLong
        val doneTs = UmsFieldType.umsFieldValue(tuple.tuple, fields, "done_ts").toString.toLong
        val interval_data_process_dataums = (mainDataTs - cdcTs) / 1000
        val interval_data_process_rdd = (mainDataTs - rddTs) / 1000
        val interval_data_process_swifts = (mainDataTs - swiftsTs) / 1000
        val interval_data_process_sink = (mainDataTs - sinkTs) / 1000
        val interval_data_process_done = (mainDataTs - doneTs) / 1000
        val interval_rdd_done: Long = (doneTs - rddTs) / 1000
        val interval_data_swifts_sink = (swiftsTs - sinkTs) / 1000
        val interval_data_sink_done = (sinkTs - doneTs) / 1000

        if (interval_rdd_done == 0L) {
          throughput = rddCount
        } else throughput = rddCount / interval_rdd_done

        val monitorInfo = MonitorInfo(statsId, umsTs, CacheMap.getProjectId(streamId), streamId, CacheMap.getStreamName(streamId), CacheMap.getFlowId(flowName), flowName, rddCount, throughput,
          DateUtils.dt2string(cdcTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(rddTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(directiveTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(mainDataTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(swiftsTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(sinkTs * 1000, DtFormat.TS_DASH_MICROSEC),
          DateUtils.dt2string(doneTs * 1000, DtFormat.TS_DASH_MICROSEC),
          interval_data_process_dataums, interval_data_process_rdd, interval_data_process_swifts, interval_data_process_sink, interval_data_process_done,
          interval_data_process_done, interval_data_swifts_sink, interval_data_sink_done)
        ElasticSearch.insertFlowStatToES(monitorInfo)
      })
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to parse FeedbackFlowStats feedback message", e)
    }
  }
}
