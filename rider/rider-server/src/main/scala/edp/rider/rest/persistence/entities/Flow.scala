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


case class Flow(id: Long,
                projectId: Long,
                streamId: Long,
                sourceNs: String,
                sinkNs: String,
                consumedProtocol: String,
                sinkConfig: Option[String],
                tranConfig: Option[String],
                status: String,
                startedTime: Option[String],
                stoppedTime: Option[String],
                active: Boolean,
                createTime: String,
                createBy: Long,
                updateTime: String,
                updateBy: Long) extends BaseEntity

case class FlowStream(id: Long,
                      projectId: Long,
                      streamId: Long,
                      sourceNs: String,
                      sinkNs: String,
                      consumedProtocol: String,
                      sinkConfig: Option[String] = None,
                      tranConfig: Option[String] = None,
                      status: String,
                      startedTime: Option[String],
                      stoppedTime: Option[String],
                      active: Boolean,
                      createTime: String,
                      createBy: Long,
                      updateTime: String,
                      updateBy: Long,
                      streamName: String,
                      streamStatus: String,
                      streamType: String,
                      disableActions: String,
                      msg: String)

case class FlowStreamInfo(id: Long,
                          projectId: Long,
                          streamId: Long,
                          sourceNs: String,
                          sinkNs: String,
                          consumedProtocol: String,
                          sinkConfig: Option[String] = None,
                          tranConfig: Option[String] = None,
                          status: String,
                          startedTime: Option[String],
                          stoppedTime: Option[String],
                          active: Boolean,
                          createTime: String,
                          createBy: Long,
                          updateTime: String,
                          updateBy: Long,
                          streamName: String,
                          streamStatus: String,
                          streamType: String,
                          disableActions: String,
                          kafka: String,
                          topics: String)

case class FlowStreamAdmin(id: Long,
                           projectId: Long,
                           projectName: String,
                           streamId: Long,
                           sourceNs: String,
                           sinkNs: String,
                           consumedProtocol: String,
                           sinkConfig: Option[String] = None,
                           tranConfig: Option[String] = None,
                           startedTime: Option[String],
                           stoppedTime: Option[String],
                           status: String,
                           active: Boolean,
                           createTime: String,
                           createBy: Long,
                           updateTime: String,
                           updateBy: Long,
                           streamName: String,
                           streamStatus: String,
                           streamType: String,
                           disableActions: String,
                           msg: String)

case class SimpleFlow(projectId: Long,
                      streamId: Long,
                      sourceNs: String,
                      sinkNs: String,
                      consumedProtocol: String,
                      sinkConfig: Option[String],
                      tranConfig: Option[String]) extends SimpleBaseEntity

case class FlowInfo(id: Long,
                    flowStatus: String,
                    disableActions: String,
                    msg: String)

case class FlowCacheMap(flowName: String, flowId: Long)

case class FlowHealth(flowStatus: String,
                      latestSinkWaterMark: String,
                      latesthdfslogWaterMark: String,
                      sinkErrorMinWatermark: String,
                      sinkErrorMaxWatermark: String,
                      sinkErrorCount: Long)

case class Sql(sql: String)

case class DeleteTopic(ids: Seq[Long], topics: Seq[String])

class FlowTable(_tableTag: Tag) extends BaseTable[Flow](_tableTag, "flow") {
  def * = (id, projectId, streamId, sourceNs, sinkNs, consumedProtocol, sinkConfig, tranConfig, status, startedTime, stoppedTime, active, createTime, createBy, updateTime, updateBy) <> (Flow.tupled, Flow.unapply)

  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
  /** Database column source_ns SqlType(VARCHAR), Length(200,true) */
  val sourceNs: Rep[String] = column[String]("source_ns", O.Length(200, varying = true))
  /** Database column sink_ns SqlType(VARCHAR), Length(200,true) */
  val sinkNs: Rep[String] = column[String]("sink_ns", O.Length(200, varying = true))
  /** Database column consumed_protocol SqlType(VARCHAR), Length(20,true) */
  val consumedProtocol: Rep[String] = column[String]("consumed_protocol", O.Length(20, varying = true))
  /** Database column tran_config SqlType(VARCHAR), Length(5000,true) */
  val tranConfig: Rep[Option[String]] = column[Option[String]]("tran_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column sink_config SqlType(VARCHAR), Length(5000,true) */
  val sinkConfig: Rep[Option[String]] = column[Option[String]]("sink_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column status SqlType(VARCHAR), Length(200,true) */
  val status: Rep[String] = column[String]("status", O.Length(200, varying = true))
  /** Database column update_time SqlType(TIMESTAMP) */
  val startedTime: Rep[Option[String]] = column[Option[String]]("started_time", O.Length(1000, varying = true), O.Default(None))
  /** Database column update_time SqlType(TIMESTAMP) */
  val stoppedTime: Rep[Option[String]] = column[Option[String]]("stopped_time", O.Length(1000, varying = true), O.Default(None))

  /** Uniqueness Index over (sourceNsId,sinkNsId,streamId,projectId) (database name flow_UNIQUE) */
  val index1 = index("flow_UNIQUE", (sourceNs, sinkNs), unique = true)
}

