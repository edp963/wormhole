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

import edp.wormhole.common.util.DtFormat
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.UmsSchemaUtils._
import org.joda.time.DateTime
import edp.wormhole.common.WormholeDefault._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.ums.UmsFeedbackStatus.UmsFeedbackStatus

object UmsProtocolUtils extends UmsProtocolUtils

trait UmsProtocolUtils {
  private lazy val dtFormat = DtFormat.TS_DASH_MICROSEC

  // data_increment_heartbeat
  def dataIncrementHeartbeat(sourceNamespace: String, heartbeatTimestamp: DateTime) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.DATA_INCREMENT_HEARTBEAT),
    schema = UmsSchema(sourceNamespace, Some(Seq(UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME)))),
    payload = Some(Seq(UmsTuple(Seq(dt2string(heartbeatTimestamp, dtFormat)))))))

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
    payload = Some(Seq(UmsTuple(Seq(dt2string(terminationTimestamp, dtFormat)))))))

  private def dataToJson(umsProtocolType: UmsProtocolType, sourceNamespace: String, fields: Seq[UmsField], payload: Seq[UmsTuple]) = toJsonCompact(Ums(
    protocol = UmsProtocol(umsProtocolType),
    schema = UmsSchema(sourceNamespace, Some(fields)),
    payload = Some(payload)))

  private def incrementTermination(umsProtocolType: UmsProtocolType, sourceNamespace: String, terminationTimestamp: DateTime) = toJsonCompact(Ums(
    protocol = UmsProtocol(umsProtocolType),
    schema = UmsSchema(sourceNamespace, Some(Seq(UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME)))),
    payload = Some(Seq(UmsTuple(Seq(dt2string(terminationTimestamp, dtFormat)))))))

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
      dt2string(timeNow, dtFormat),
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
      dt2string(timeNow, dtFormat),
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
      dt2string(timeNow, dtFormat),
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
      dt2string(timeNow, dtFormat),
      topicName))))))

  // feedback_data_increment_termination
  def feedbackDataIncrementTermination(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_INCREMENT_TERMINATION),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  // feedback_data_increment_heartbeat
  def feedbackDataIncrementHeartbeat(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_INCREMENT_HEARTBEAT),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  // feedback_data_batch_termination
  def feedbackDataBatchTermination(sourceNamespace: String, time: String, streamID: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DATA_BATCH_TERMINATION),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG)))),
    payload = Some(Seq(UmsTuple(Seq(
      time,
      streamID.toString))))))

  //  feedback_directive
  def feedbackDirective(timeNow: DateTime,
                        directiveId: Long,
                        status: UmsFeedbackStatus,
                        streamId:Long,
                        resultDesc:String): String = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_DIRECTIVE),
    schema = UmsSchema("", Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("directive_id", UmsFieldType.LONG),
      UmsField("status", UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField("result_desc", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      dt2string(timeNow, dtFormat),
      directiveId.toString, status.toString,streamId.toString,resultDesc))))))

  // feedback_flow_error
  def feedbackFlowError(sourceNamespace: String,
                        streamId:Long,
                        timeNow: DateTime,
                        sinkNamespace: String,
                        maxWatermark: UmsWatermark,
                        minWatermark: UmsWatermark,
                        errorCount: Int,
                        errorInfo: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_ERROR),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("sink_namespace", UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField("error_max_watermark_ts", UmsFieldType.STRING),
      UmsField("error_min_watermark_ts", UmsFieldType.STRING),
      UmsField("error_count", UmsFieldType.INT),
      UmsField("error_info", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      dt2string(timeNow, dtFormat),
      sinkNamespace,
      streamId.toString,
      dt2string(maxWatermark.ts, dtFormat),
      dt2string(minWatermark.ts, dtFormat),
      errorCount.toString,
      errorInfo))))))

  // feedback_flow_stats
  def feedbackFlowStats(sourceNamespace: String,
                        dataType: String,
                        timestamp: DateTime,
                        streamId: Long,
                        statsId: String,
                        sinkNamespace: String,
                        rddCount: Int,
                        cdcTs: Long,
                        rddTs: Long,
                        directiveTs: Long,
                        mainDataTs: Long,
                        swiftsTs: Long,
                        sinkTs: Long,
                        doneTs: Long) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_STATS),
    schema = UmsSchema(sourceNamespace, Some(Seq(
      UmsField("data_type", UmsFieldType.STRING),
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("stream_id", UmsFieldType.STRING),
      UmsField("stats_id", UmsFieldType.STRING),
      UmsField("sink_namespace", UmsFieldType.STRING),
      UmsField("rdd_count", UmsFieldType.INT),
      UmsField("data_genereated_ts", UmsFieldType.LONG),
      UmsField("rdd_generated_ts", UmsFieldType.LONG),
      UmsField("directive_process_start_ts", UmsFieldType.LONG),
      UmsField("data_process_start_ts", UmsFieldType.LONG),
      UmsField("swifts_start_ts", UmsFieldType.LONG),
      UmsField("sink_start_ts", UmsFieldType.LONG),
      UmsField("done_ts", UmsFieldType.LONG)
    ))),
    payload = Some(Seq(UmsTuple(Seq(
      dataType,
      dt2string(timestamp, dtFormat),
      streamId.toString,
      statsId,
      sinkNamespace,
      rddCount.toString,
      cdcTs.toString,
      rddTs.toString,
      directiveTs.toString,
      mainDataTs.toString,
      swiftsTs.toString,
      sinkTs.toString,
      doneTs.toString
    ))))))

  // feedback_stream_batch_error
  def feedbackStreamBatchError(streamID: Long, timeNow: DateTime, status: UmsFeedbackStatus, resultDesc: String) = toJsonCompact(Ums(
    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAM_BATCH_ERROR),
    schema = UmsSchema("", Some(Seq(
      UmsField("stream_id", UmsFieldType.LONG),
      UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
      UmsField("status", UmsFieldType.STRING),
      UmsField("result_desc", UmsFieldType.STRING)))),
    payload = Some(Seq(UmsTuple(Seq(
      streamID.toString,
      dt2string(timeNow, dtFormat),
      status.toString,
      resultDesc.toString))))))

  // feedback_stream_topic_offset
  def feedbackStreamTopicOffset(timeNow: DateTime, streamID: Long,tp:Map[String, String]) = {
    toJsonCompact(Ums(
      protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET),
      schema = UmsSchema(empty, Some(Seq(
        UmsField(UmsSysField.TS.toString, UmsFieldType.STRING),
        UmsField("stream_id", UmsFieldType.INT),
        UmsField("topic_name", UmsFieldType.STRING),
        UmsField("partition_offsets", UmsFieldType.STRING)
      ))),
      payload = Some(tp.map{case (topicName, partitionOffsets) => UmsTuple(Seq(dt2string(timeNow, dtFormat), streamID.toString, topicName, partitionOffsets))}.toSeq)))
  }


//  Seq(
//    dt2string(timeNow, dtFormat),
//    streamID.toString,
//    topicName,
//    partitionNum.toString,
//    partitionOffset.toString)


//  def feedbackError(sourceNamespace: String,
//                    timeNow: DateTime,
//                    sinkNamespace: String,
//                    maxWatermark: UmsWatermark,
//                    minWatermark: UmsWatermark,
//                    errorCount: Int,
//                    errorInfo: String) = toJsonCompact(Ums(
//    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_ERROR),
//    schema = UmsSchema(sourceNamespace, Some(Seq(
//      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
//      UmsField("sink_namespace", UmsFieldType.STRING),
//      //      UmsField("error_max_watermark_id", UmsFieldType.LONG),
//      UmsField("error_max_watermark_ts", UmsFieldType.DATETIME),
//      //      UmsField("error_min_watermark_id", UmsFieldType.LONG),
//      UmsField("error_min_watermark_ts", UmsFieldType.DATETIME),
//      UmsField("error_count", UmsFieldType.INT),
//      UmsField("error_info", UmsFieldType.STRING)))),
//    payload = Some(Seq(UmsTuple(Seq(
//      dt2string(timeNow, dtFormat),
//      sinkNamespace,
//      //      maxWatermark.id.toString,
//      dt2string(maxWatermark.ts, dtFormat),
//      //      minWatermark.id.toString,
//      dt2string(minWatermark.ts, dtFormat),
//      errorCount.toString,
//      errorInfo))))))
//
//
//  def feedbackTopicPartitionOffset(timeNow: DateTime,
//                                   topic_name: String,
//                                   partition_num: Int,
//                                   partition_offset: Long,
//                                   stream_id: Int) = toJsonCompact(Ums(
//    protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET),
//    schema = UmsSchema(empty, Some(Seq(
//      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
//      UmsField("stream_id", UmsFieldType.INT),
//      UmsField("topic_name", UmsFieldType.STRING),
//      UmsField("partition_num", UmsFieldType.INT),
//      UmsField("partition_offset", UmsFieldType.LONG)))),
//    payload = Some(Seq(UmsTuple(Seq(
//      dt2string(timeNow, dtFormat),
//      stream_id.toString,
//      topic_name,
//      partition_num.toString,
//      partition_offset.toString))))))
//
//  def feedbackDataHeartbeatOrTermination(timeNow: String,
//                                         namespace: String,
//                                         stream_id: Int,
//                                         protocol: UmsProtocolType) = toJsonCompact(Ums(
//    protocol = UmsProtocol(protocol),
//    schema = UmsSchema(namespace, Some(Seq(
//      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
//      UmsField("stream_id", UmsFieldType.INT)))),
//    payload = Some(Seq(UmsTuple(Seq(
//      timeNow,
//      stream_id.toString))))))


  /*
    def feedbackWatermark(sourceNamespace: String,
                          timeNow: DateTime,
                          sinkNamespace: String,
                          watermark: UmsWatermark) = toJsonCompact(Ums(
      protocol = UmsProtocol(UmsProtocolType.FEEDBACK_WATERMARK),
      schema = UmsSchema(sourceNamespace, Some(Seq(
        UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
        UmsField("sink_namespace", UmsFieldType.STRING),
        //      UmsField("watermark_id", UmsFieldType.LONG),
        UmsField("watermark_ts", UmsFieldType.DATETIME)))),
      payload = Some(Seq(UmsTuple(Seq(
        dt2string(timeNow, dtFormat),
        sinkNamespace,
        //      watermark.id.toString,
        dt2string(watermark.ts, dtFormat)))))))
  */
  // feedback_flow_status
//  def feedbackStats(protocolType: UmsProtocolType,
//                    dataType: String,
//                    sourceNamespace: String,
//                    sinkNamespace: String,
//                    timestamp: DateTime,
//                    streamId: Long,
//                    statsId: String,
//                    rddCount: Int,
//                    cdcTs: Long,
//                    rddTs: Long,
//                    streamMergeTs: Long,
//                    mainDataTs: Long,
//                    swiftsTs: Long,
//                    sinkTs: Long,
//                    doneTs: Long) = toJsonCompact(Ums(
//    protocol = UmsProtocol(protocolType),
//    schema = UmsSchema(sourceNamespace, Some(Seq(
//      UmsField(UmsSysField.TS.toString, UmsFieldType.DATETIME),
//      UmsField("data_type", UmsFieldType.STRING),
//      UmsField("stream_id", UmsFieldType.STRING),
//      UmsField("stats_id", UmsFieldType.STRING),
//      UmsField("sink_namespace", UmsFieldType.STRING),
//      UmsField("rdd_count", UmsFieldType.INT),
//      UmsField("cdc_ts", UmsFieldType.LONG),
//      UmsField("rdd_ts", UmsFieldType.LONG),
//      UmsField("stream_merge_ts", UmsFieldType.LONG),
//      UmsField("main_data_ts", UmsFieldType.LONG),
//      UmsField("swifts_ts", UmsFieldType.LONG),
//      UmsField("sink_ts", UmsFieldType.LONG),
//      UmsField("done_ts", UmsFieldType.LONG)
//    ))),
//    payload = Some(Seq(UmsTuple(Seq(
//      dt2string(timestamp, dtFormat),
//      dataType,
//      streamId.toString,
//      statsId,
//      sinkNamespace,
//      rddCount.toString,
//      cdcTs.toString,
//      rddTs.toString,
//      streamMergeTs.toString,
//      mainDataTs.toString,
//      swiftsTs.toString,
//      sinkTs.toString,
//      doneTs.toString
//    ))))))

  /*
    // feedback_streaming_status
    def feedbackStreamingStats(timeNow: DateTime,
                               stream_id: String,
                               stream_stats: String) = toJsonCompact(Ums(
      protocol = UmsProtocol(UmsProtocolType.FEEDBACK_STREAMING_STATS),
      schema = UmsSchema("", Some(Seq(
        UmsField("stream_id", UmsFieldType.STRING),
        UmsField(UmsSysField.TS.toString, UmsFieldType.LONG),
        UmsField("stream_status", UmsFieldType.STRING)
      ))),
      payload = Some(Seq(UmsTuple(Seq(
        stream_id,
        dt2string(timeNow, dtFormat),
        stream_stats))))))
        */
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
