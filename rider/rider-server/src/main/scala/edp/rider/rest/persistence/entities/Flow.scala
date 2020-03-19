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


package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.lifted.{Rep, Tag}
import slick.jdbc.MySQLProfile.api._


case class Flow(id: Long, flowName: String, projectId: Long, streamId: Long, priorityId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String], tranConfig: Option[String], tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], logPath: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class FlowStream(id: Long, flowName: String, projectId: Long, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String] = None, tranConfig: Option[String] = None, tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], logPath: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long, streamName: String, streamAppId: Option[String], streamStatus: String, streamType: String, functionType: String, disableActions: String, hideActions: String, topicInfo: Option[GetTopicsResponse], currentUdf: Seq[FlowUdfResponse], msg: String)

case class FlowStreamInfo(id: Long, flowName: String, projectId: Long, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String] = None, tranConfig: Option[String] = None, tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long, streamName: String, streamStatus: String, streamType: String, functionType: String, disableActions: String, kafka: String, topics: String)

case class FlowStreamAdmin(id: Long, flowName: String, projectId: Long, projectName: String, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String] = None, tranConfig: Option[String] = None, tableKeys: Option[String], desc: Option[String] = None, startedTime: Option[String], stoppedTime: Option[String], status: String, active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long, streamName: String, streamStatus: String, streamType: String, functionType: String, disableActions: String, msg: String)

case class FlowStreamAdminInfo(id: Long, flowName: String, projectId: Long, projectName: String, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String] = None, tranConfig: Option[String] = None, tableKeys: Option[String], desc: Option[String] = None, startedTime: Option[String], stoppedTime: Option[String], status: String, active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long, streamName: String, streamStatus: String, streamType: String, functionType: String, disableActions: String, hideActions: String, msg: String)

case class FlowUpdateInfo(id: Long, flowName: String, projectId: Long, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String], tranConfig: Option[String], tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], logPath: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class FlowAdminAllInfo(id: Long, flowName: String, projectId: Long, projectName: String, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String] = None, tranConfig: Option[String] = None, tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long, streamName: String, streamStatus: String, streamType: String, functionType: String, disableActions: String, hideActions: String, topicInfo: GetTopicsResponse, currentUdf: Seq[FlowUdfResponse], msg: String)

case class SimpleFlow(flowName: String, projectId: Long, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String], tranConfig: Option[String], tableKeys: Option[String], desc: Option[String] = None) extends SimpleBaseEntity


case class FlowInfo(id: Long,
                    flowStatus: String,
                    disableActions: String,
                    startTime: Option[String],
                    stopTime: Option[String],
                    msg: String,
                    update: Boolean = true)

case class FlowCacheMap(flowName: String, flowId: Long)

case class FlowHealth(flowStatus: String,
                      latestSinkWaterMark: String,
                      latesthdfslogWaterMark: String,
                      sinkErrorMinWatermark: String,
                      sinkErrorMaxWatermark: String,
                      sinkErrorCount: Long)

case class Sql(sql: String)

case class RechargeType(protocolType: String)

case class FlowPriority(id: Long, flowName: String, priorityId: Long)

case class FlowPriorities(flowPrioritySeq: Seq[FlowPriority])

case class DeleteTopic(ids: Seq[Long], topics: Seq[String])

case class FlowDirective(udfInfo: Seq[Long], topicInfo: PutFlowTopic)

case class PutFlowTopic(autoRegisteredTopics: Seq[PutFlowTopicDirective], userDefinedTopics: Seq[PutFlowTopicDirective])

case class PutFlowTopicDirective(name: String, partitionOffsets: String)


case class SimpleFlowTopicAllOffsets(name: String,
                                     rate: Int,
                                     consumedLatestOffset: String,
                                     kafkaEarliestOffset: String,
                                     kafkaLatestOffset: String)

case class GetFlowTopicsOffsetResponse(autoRegisteredTopics: Seq[SimpleFlowTopicAllOffsets], userDefinedTopics: Seq[SimpleFlowTopicAllOffsets])

case class FlowIdKafkaUrl(flowId: Long, kafkaUrl: String)

case class FlowUdfResponse(id: Long, functionName: String, fullClassName: String, jarName: String, mapOrAgg: String)

case class StartFlinkFlowResponse(id: Long,
                                  status: String,
                                  disableActions: String,
                                  hiddenActions: String,
                                  startedTime: Option[String] = None,
                                  stoppedTime: Option[String] = None)

case class FlinkJobStatus(name: String, jobId: String, state: String, startTime: String, stopTime: String)

case class FlinkFlowStatus(status: String, startTime: Option[String], stopTime: Option[String])

case class DriftFlowRequest(streamId: Long)

case class DriftFlowResponse(id: Long,
                             status: String,
                             streamId: Long,
                             streamStatus: String,
                             disableActions: String,
                             hiddenActions: String,
                             startedTime: Option[String] = None,
                             stoppedTime: Option[String] = None,
                             msg: String)

class FlowTable(_tableTag: Tag) extends BaseTable[Flow](_tableTag, "flow") {
  def * = (id, flowName, projectId, streamId, priorityId, sourceNs, sinkNs, config, consumedProtocol, sinkConfig, tranConfig, tableKeys, desc, status, startedTime, stoppedTime, logPath, active, createTime, createBy, updateTime, updateBy) <> (Flow.tupled, Flow.unapply)

  val flowName: Rep[String] = column[String]("flow_name", O.Length(200, varying = true))
  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
  /** Database column priority_id SqlType(BIGINT) */
  val priorityId: Rep[Long] = column[Long]("priority_id")
  /** Database column source_ns SqlType(VARCHAR), Length(200,true) */
  val sourceNs: Rep[String] = column[String]("source_ns", O.Length(200, varying = true))
  /** Database column sink_ns SqlType(VARCHAR), Length(200,true) */
  val sinkNs: Rep[String] = column[String]("sink_ns", O.Length(200, varying = true))

  val config: Rep[Option[String]] = column[Option[String]]("config")
  /** Database column consumed_protocol SqlType(VARCHAR), Length(20,true) */
  val consumedProtocol: Rep[String] = column[String]("consumed_protocol", O.Length(20, varying = true))
  /** Database column tran_config SqlType(VARCHAR), Length(5000,true) */
  val tranConfig: Rep[Option[String]] = column[Option[String]]("tran_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column table_keys SqlType(VARCHAR), Length(1000,true), Default(None) */
  val tableKeys: Rep[Option[String]] = column[Option[String]]("table_keys", O.Length(1000, varying = true), O.Default(None))
  /** Database column sink_config SqlType(VARCHAR), Length(5000,true) */
  val sinkConfig: Rep[Option[String]] = column[Option[String]]("sink_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column desc SqlType(VARCHAR), Length(1000,true), Default(None) */
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column status SqlType(VARCHAR), Length(200,true) */
  val status: Rep[String] = column[String]("status", O.Length(200, varying = true))
  /** Database column update_time SqlType(TIMESTAMP) */
  val startedTime: Rep[Option[String]] = column[Option[String]]("started_time", O.Length(1000, varying = true), O.Default(None))
  /** Database column update_time SqlType(TIMESTAMP) */
  val stoppedTime: Rep[Option[String]] = column[Option[String]]("stopped_time", O.Length(1000, varying = true), O.Default(None))
  val logPath: Rep[Option[String]] = column[Option[String]]("log_path", O.Length(2000, varying = true), O.Default(None))

  /** Uniqueness Index over (sourceNsId,sinkNsId,streamId,projectId) (database name flow_UNIQUE) */
  val index1 = index("flow_UNIQUE", (sourceNs, sinkNs), unique = true)
}

