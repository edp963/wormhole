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


case class FlowHistory(id: Long, flowId: Long, flowName: String, projectId: Long, streamId: Long, sourceNs: String, sinkNs: String, config: Option[String], consumedProtocol: String, sinkConfig: Option[String], tranConfig: Option[String], tableKeys: Option[String], desc: Option[String] = None, status: String, startedTime: Option[String], stoppedTime: Option[String], logPath: Option[String], active: Boolean, createTime: String, createBy: Long, updateTime: String, updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


class FlowHistoryTable(_tableTag: Tag) extends BaseTable[FlowHistory](_tableTag, "flow_history") {
  def * = (id, flowId, flowName, projectId, streamId, sourceNs, sinkNs, config, consumedProtocol, sinkConfig, tranConfig, tableKeys, desc, status, startedTime, stoppedTime, logPath, active, createTime, createBy, updateTime, updateBy) <> (FlowHistory.tupled, FlowHistory.unapply)

  val flowId: Rep[Long] = column[Long]("flow_id")
  val flowName: Rep[String] = column[String]("flow_name", O.Length(200, varying = true))
  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
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
  //val index1 = index("flow_UNIQUE", (sourceNs, sinkNs), unique = true)

  //val action: Rep[String] = column[String]("action", O.Length(20, varying = true))
}

