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
import edp.rider.rest.persistence.entities.{FeedbackErr, FeedbackErrTable}
import edp.rider.rest.util.CommonUtils.{maxTimeOut, minTimeOut}
import slick.lifted.TableQuery

import slick.jdbc.MySQLProfile.api._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class FeedbackErrDal(feedbackErrTable: TableQuery[FeedbackErrTable])
  extends BaseDalImpl[FeedbackErrTable, FeedbackErr](feedbackErrTable){
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
    val deleteMaxId = Await.result(
      db.run(feedbackErrTable.withFilter(_.feedbackTime <= pastNdays).map(_.id).max.result).mapTo[Option[Long]], minTimeOut)
    //if (deleteMaxId.nonEmpty) Await.result(super.deleteByFilter(_.id <= deleteMaxId), maxTimeOut)

    if (deleteMaxId.isDefined && deleteMaxId.nonEmpty) {
      val deleteBatchSize = 5000
      val deleteMinId = Await.result(db.run(feedbackErrTable.withFilter(_.id <= deleteMaxId).map(_.id).min.result).mapTo[Option[Long]], minTimeOut)
      var curDeleteId = deleteMinId.get + deleteBatchSize
      while(curDeleteId < deleteMaxId.get) {
        Await.result(super.deleteByFilter(error => {error.id <= curDeleteId && error.id >= (curDeleteId - deleteBatchSize)}), maxTimeOut)
        curDeleteId = curDeleteId + deleteBatchSize
      }
      Await.result(super.deleteByFilter(error => {error.id <= deleteMaxId && error.id >= (curDeleteId - deleteBatchSize)}), maxTimeOut)
    }
  }
}
