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

import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.{maxTimeOut, minTimeOut}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class FeedbackFlowErrDal(feedbackFlowErrTable: TableQuery[FeedbackFlowErrTable],
                         streamDal: StreamDal,
                         flowDal: FlowDal) extends BaseDalImpl[FeedbackFlowErrTable, FeedbackFlowErr](feedbackFlowErrTable) {

  def getSinkErrorMaxWatermark(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[String]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[String]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorMaxWaterMarkTs).max))
  }

  def getSinkErrorMinWatermark(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[String]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[String]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorMinWaterMarkTs).min))
  }

  def getSinkErrorCount(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[Long]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[Long]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorCount).sum))
  }

  def deleteHistory(pastNdays: String) = {
    val ignoreIds = new ListBuffer[Long]
    val existSeq = Await.result(super.findAll, maxTimeOut).map(
      flowError => StreamSourceSink(flowError.streamId, flowError.sourceNamespace, flowError.sinkNamespace)
    ).distinct
    val streamIds = Await.result(streamDal.findAll, maxTimeOut).map(_.id)
    val sourceSinks = Await.result(flowDal.findAll, maxTimeOut).map(flow => flow.sourceNs + "#" + flow.sinkNs)
    existSeq.filter(flowError => streamIds.contains(flowError.streamId))
      .filter(flowError => sourceSinks.contains(flowError.sourceNs + "#" + flowError.sinkNs))
      .map(flowError => {
        val maxFlowError = Await.result(
          db.run(feedbackFlowErrTable
            .filter(table => table.streamId === flowError.streamId &&
              table.sourceNamespace === flowError.sourceNs && table.sinkNamespace === flowError.sinkNs)
            .sortBy(_.feedbackTime).take(1).result), minTimeOut)
        if (maxFlowError.nonEmpty) ignoreIds += maxFlowError.head.id
      })
    Await.result(super.deleteByFilter(flowError => flowError.feedbackTime <= pastNdays && !flowError.id.inSet(ignoreIds)), maxTimeOut)
  }
}
