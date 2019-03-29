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


package edp.wormhole.ums

import edp.wormhole.ums.UmsFeedbackStatus.UmsFeedbackStatus
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.UmsSchemaUtils._
import edp.wormhole.util.{DateUtils, DtFormat}
import org.joda.time.DateTime

object UmsProtocolUtils extends UmsProtocolUtils

trait UmsProtocolUtils {
  private lazy val dtFormat = DtFormat.TS_DASH_MICROSEC

  // data_increment_heartbeat
  def dataIncrementHeartbeat(sourceNamespace: String, heartbeatTimestamp: DateTime) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DATA_INCREMENT_HEARTBEAT),
    schema = UmsSchema(sourceNamespace, Some(Seq(UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME)))),
    payload = Some(Seq(UmsTuple(Seq(DateUtils.dt2string(heartbeatTimestamp, dtFormat)))))))

  // data_increment_data
  def dataInitialData(sourceNamespace: String, fields: Seq[UmsField], payload: Seq[UmsTuple]) = dataToJson(
    UmsProtocolType.DATA_INITIAL_DATA, sourceNamespace, fields, payload)

  // data_increment_data
  def dataIncrementData(sourceNamespace: String, fields: Seq[UmsField], payload: Seq[UmsTuple]) = dataToJson(
    UmsProtocolType.DATA_INCREMENT_DATA, sourceNamespace, fields, payload)

  // data_increment_termination
  def dataIncrementTermination(sourceNamespace: String, terminationTimestamp: DateTime) = incrementTermination(
    UmsProtocolType.DATA_INCREMENT_TERMINATION, sourceNamespace, terminationTimestamp)

  // data_batch_data
  def dataBatchData(sourceNamespace: String, fields: Seq[UmsField], payload: Seq[UmsTuple]) = dataToJson(
    UmsProtocolType.DATA_BATCH_DATA, sourceNamespace, fields, payload)

  // data_batch_termination
  def dataBatchTermination(sourceNamespace: String, terminationTimestamp: DateTime) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DATA_BATCH_TERMINATION),
    schema = UmsSchema(sourceNamespace, Some(Seq(UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME)))),
    payload = Some(Seq(UmsTuple(Seq(DateUtils.dt2string(terminationTimestamp, dtFormat)))))))

  private def dataToJson(umsProtocolType: UmsProtocolType, sourceNamespace: String, fields: Seq[UmsField], payload: Seq[UmsTuple]) = toJsonCompact(Ums(
    protocol = UmsProtocol(umsProtocolType),
    schema = UmsSchema(sourceNamespace, Some(fields)),
    payload = Some(payload)))

  private def incrementTermination(umsProtocolType: UmsProtocolType, sourceNamespace: String, terminationTimestamp: DateTime) = toJsonCompact(Ums(
    protocol = UmsProtocol(umsProtocolType),
    schema = UmsSchema(sourceNamespace, Some(Seq(UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME)))),
    payload = Some(Seq(UmsTuple(Seq(DateUtils.dt2string(terminationTimestamp, dtFormat)))))))

  // directive_flow_start
  def WormholeDirectiveFlowStart(sourceNamespace: String,
                                 directiveId: Long,
                                 streamID: Long,
                                 timeNow: DateTime,
                                 sinkNamespace: String,
                                 swifts: String,
                                 sinks: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DIRECTIVE_FLOW_START),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("sink_namespace", UmsFieldType.STRING),
      UmsField("swifts", UmsFieldType.STRING),
      UmsField("sinks", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      directiveId.toString,
      streamID.toString,
      DateUtils.dt2string(timeNow, dtFormat),
      swifts,
      sinks))))))

  // directie_hdfslog_flow_start
  def WormholeDirectiveHdfsLogFlowStart(sourceNamespace: String,
                                        directiveId: Long,
                                        streamID: Long,
                                        timeNow: DateTime,
                                        namespaceRule: String,
                                        hourDuration: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DIRECTIVE_HDFSLOG_FLOW_START),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("namespace_rule", UmsFieldType.STRING),
      UmsField("hour_duration", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      directiveId.toString,
      streamID.toString,
      DateUtils.dt2string(timeNow, dtFormat),
      namespaceRule,
      hourDuration))))))

  // directive_topic_subscribe
  def directiveTopicSubscribe(directiveId: Long,
                              streamID: Long,
                              timeNow: DateTime,
                              topicName: String,
                              topicRate: Int,
                              partitionOffset: String
                             ) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DIRECTIVE_TOPIC_SUBSCRIBE),
    schema = UmsSchema("", Some(Seq(
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("topic_name", UmsFieldType.STRING),
      UmsField("topic_rate", UmsFieldType.INT),
      UmsField("partitions_offset", UmsFieldType.STRING)
    ))),
    payload = Some(Seq(UmsTuple(Seq(
      directiveId.toString,
      streamID.toString,
      DateUtils.dt2string(timeNow, dtFormat),
      topicName,
      topicRate.toString,
      partitionOffset))))))

  // directiveTopicUnsubscribe
  def directiveTopicUnsubscribe(directiveId: Long, streamID: Long, timeNow: DateTime, topicName: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DIRECTIVE_TOPIC_UNSUBSCRIBE),
    schema = UmsSchema("", Some(Seq(
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("topic_name", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      directiveId.toString,
      streamID.toString,
      DateUtils.dt2string(timeNow, dtFormat),
      topicName))))))

  // feedback_data_increment_termination
  def feedbackDataIncrementTermination(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_INCREMENT_TERMINATION),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  // feedback_data_increment_heartbeat
  def feedbackDataIncrementHeartbeat(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_INCREMENT_HEARTBEAT),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  // feedback_data_batch_termination
  def feedbackDataBatchTermination(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_BATCH_TERMINATION),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  //  feedback_directive
  def feedbackDirective(timeNow: DateTime,
                        directiveId: Long,
                        status: UmsFeedbackStatus,
                        streamId: Long,
                        flowId: Long,
                        resultDesc: String): String = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_START_DIRECTIVE),
    schema = UmsSchema("", Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("status", UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField("flow_id", UmsFieldType.LONG),
      UmsField("result_desc", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      DateUtils.dt2string(timeNow, dtFormat),
      directiveId.toString, status.toString, streamId.toString,flowId.toString, resultDesc))))))

  // feedback_flow_error
  def feedbackFlowError(sourceNamespace: String,
                        streamId: Long,
                        timeNow: DateTime,
                        sinkNamespace: String,
                        maxWatermark: UmsWatermark,
                        minWatermark: UmsWatermark,
                        errorCount: Int,
                        errorInfo: String,
                        batchId: String,
                        topicPartitionOffset: String,
                        dataType: String,
                        flowId: Long,
                        errorPattern: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_ERROR),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("sink_namespace", UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField("error_max_watermark_ts", UmsFieldType.STRING),
      UmsField("error_min_watermark_ts", UmsFieldType.STRING),
      UmsField("error_count", UmsFieldType.INT),
      UmsField("error_info", UmsFieldType.STRING),
      UmsField("batch_id", UmsFieldType.STRING),
      UmsField("topics", UmsFieldType.STRING),
      UmsField("data_type", UmsFieldType.STRING),
      UmsField("flow_id", UmsFieldType.LONG),
      UmsField("error_pattern", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      DateUtils.dt2string(timeNow, dtFormat),
      sinkNamespace,
      streamId.toString,
      DateUtils.dt2string(maxWatermark.ts, dtFormat),
      DateUtils.dt2string(minWatermark.ts, dtFormat),
      errorCount.toString,
      s""""${errorInfo}"""",
      batchId,
      topicPartitionOffset,
      s""""${dataType}"""",
      flowId.toString,
      errorPattern))))))

  // feedback_sparkx_flow_stats
  def feedbackFlowStats(sourceNamespace: String,
                        dataType: String,
                        timestamp: DateTime,
                        streamId: Long,
                        batchId: String,
                        sinkNamespace: String,
                        topics: String,
                        rddCount: Int,
                        cdcTs: String,
                        rddTs: String,
                        directiveTs: String,
                        mainDataTs: String,
                        swiftsTs: String,
                        sinkTs: String,
                        doneTs: String,
                        flowId:Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_STATS),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField("data_type", UmsFieldType.STRING),
      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
      UmsField("stream_id", UmsFieldType.STRING),
      UmsField("batch_id", UmsFieldType.STRING),
      UmsField("sink_namespace", UmsFieldType.STRING),
      UmsField("topics", UmsFieldType.STRING),
      UmsField("rdd_count", UmsFieldType.INT),
      UmsField("data_generated_ts", UmsFieldType.DATETIME),
      UmsField("rdd_generated_ts", UmsFieldType.DATETIME),
      UmsField("directive_process_start_ts", UmsFieldType.DATETIME),
      UmsField("data_process_start_ts", UmsFieldType.DATETIME),
      UmsField("swifts_start_ts", UmsFieldType.DATETIME),
      UmsField("sink_start_ts", UmsFieldType.DATETIME),
      UmsField("done_ts", UmsFieldType.DATETIME),
      UmsField("flow_id", UmsFieldType.LONG)
    ))),
    payload = Some(Seq(UmsTuple(Seq(
      dataType,
      DateUtils.dt2string(timestamp, dtFormat),
      streamId.toString,
      batchId,
      sinkNamespace,
      topics,
      rddCount.toString,
      cdcTs,
      rddTs,
      directiveTs,
      mainDataTs,
      swiftsTs,
      sinkTs,
      doneTs,
      flowId.toString
    ))))))

  // feedback_stream_batch_error
//  def feedbackStreamBatchError(streamID: Long, timeNow: DateTime, status: UmsFeedbackStatus, resultDesc: String, batchId: String, topicPartitionOffset: String) = toJsonCompact(Ums(
//    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAM_BATCH_ERROR),
//    schema = UmsSchema("", Some(Seq(
//      UmsField("stream_id", UmsFieldType.LONG),
//      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
//      UmsField("status", UmsFieldType.STRING),
//      UmsField("result_desc", UmsFieldType.STRING),
//      UmsField("batch_id", UmsFieldType.STRING),
//      UmsField("topics", UmsFieldType.STRING)))),
//    payload = Some(Seq(UmsTuple(Seq(
//      streamID.toString,
//      DateUtils.dt2string(timeNow, dtFormat),
//      status.toString,
//      resultDesc.toString,
//      batchId,
//      topicPartitionOffset))))))

  // feedback_stream_topic_offset
//  def feedbackStreamTopicOffset(timeNow: DateTime, streamID: Long, tp: Map[String, String], batchId: String) = {
//    toJsonCompact(Ums(
//      protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET),
//      schema = UmsSchema(WormholeDefault.empty, Some(Seq(
//        UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
//        UmsField("stream_id", UmsFieldType.INT),
//        UmsField("topic_name", UmsFieldType.STRING),
//        UmsField("partition_offsets", UmsFieldType.STRING),
//        UmsField("batch_id", UmsFieldType.STRING)
//      ))),
//      payload = Some(tp.map { case (topicName, partitionOffsets) => UmsTuple(Seq(DateUtils.dt2string(timeNow, dtFormat), streamID.toString, topicName, partitionOffsets, batchId)) }.toSeq)))
//  }

  // exceptionData
//  def feedbackFlinkxFlowError(sourceNamespace: String,
//                              streamId: Long,
//                              flowId: Long,
//                              sinkNamespace: String,
//                              timeNow: DateTime,
//                              dataInfo: String,
//                              errorInfo: String): String = toJsonCompact(Ums(
//    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLINKX_FLOW_ERROR),
//    schema = UmsSchema(
//      sourceNamespace, Some(Seq(
//        UmsField("stream_id", UmsFieldType.LONG),
//        UmsField("flow_id", UmsFieldType.LONG),
//        UmsField("sink_namespace", UmsFieldType.STRING),
//        UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
//        UmsField("data_info", UmsFieldType.STRING),
//        UmsField("error_info", UmsFieldType.STRING)))),
//    payload = Some(Seq(UmsTuple(Seq(
//      streamId.toString,
//      flowId.toString,
//      sinkNamespace,
//      DateUtils.dt2string(timeNow, dtFormat),
//      dataInfo,
//      errorInfo))))))
}


/*
  // dbus/other send to ss
  val DATA_INCREMENT_HEARTBEAT = Value("data_increment_heartbeat")
  val DATA_INCREMENT_DATA = Value("data_increment_data")

  // reload to ss
  val DATA_BATCH_DATA = Value("data_batch_data")

  // dbus send to ss / ss send to cc
  val DATA_INCREMENT_TERMINATION = Value("data_increment_termination")
  val FEEDBACK_INCREMENT_TERMINATION = Value("feedback_increment_termination")

  // reload to ss / ss send to cc
  val DATA_BATCH_TERMINATION = Value("data_batch_termination")
  val FEEDBACK_BATCH_TERMINATION = Value("feedback_batch_termination")

  // cc send to ss / ss send to cc
  val DIRECTIVE_FLOW_START = Value("directive_flow_start")
  val DIRECTIVE_FLOW_STOP = Value("directive_flow_stop")
  val DIRECTIVE_INCREMENT_QUERY_OFFSET = Value("directive_increment_query_offset")
  val DIRECTIVE_INCREMENT_QUERY_WATERMARK = Value("directive_increment_query_watermark")
  val FEEDBACK_DIRECTIVE = Value("feedback_directive")
 */
