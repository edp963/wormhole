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


package edp.rider.rest.persistence.dal

import edp.rider.common.{FlowStatus, RiderLogger, StreamStatus, StreamType}
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils
import edp.rider.rest.util.CommonUtils._
import slick.lifted.{CanBeQueryCondition, TableQuery}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class FlowHistoryDal(flowHistoryTable: TableQuery[FlowHistoryTable])
  extends BaseDalImpl[FlowHistoryTable, FlowHistory](flowHistoryTable) with RiderLogger {

  def getAll[C: CanBeQueryCondition](f: FlowHistoryTable => C): Future[Seq[FlowHistory]] = {
    try {
      Future(Await.result(super.findByFilter(f), minTimeOut))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Failed to get flowHistory", ex)
        throw ex
    }
  }

  def insert(flows: Seq[Flow], action: String, userId: Long): Unit = {
    try {
      val flowHistories = flows.map(flow => {
        new FlowHistory(0, flow.id, flow.flowName, flow.projectId, flow.streamId, flow.sourceNs, flow.sinkNs, flow.config, flow.consumedProtocol, flow.sinkConfig, flow.tranConfig, flow.tableKeys, flow.desc, flow.status, flow.startedTime, flow.stoppedTime, flow.logPath, flow.active, flow.createTime, flow.createBy, currentSec, userId)
      })
      Await.result(insert(flowHistories), CommonUtils.minTimeOut)
    } catch {
      case ex: Exception =>
        val flowIds = flows.map(flow => flow.id).mkString(",")
        riderLogger.error(s"flows $flowIds insert into flowHistory failed", ex)
    }
  }
}
