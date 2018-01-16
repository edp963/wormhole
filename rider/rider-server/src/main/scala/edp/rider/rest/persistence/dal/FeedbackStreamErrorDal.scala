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

import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

class FeedbackStreamErrorDal(streamErrorTable: TableQuery[FeedbackStreamErrTable], streamDal: StreamDal) extends BaseDalImpl[FeedbackStreamErrTable, FeedbackStreamErr](streamErrorTable) {


  def deleteHistory(pastNdays: String) = {
    val ignoreIds = new ListBuffer[Long]
    val streamErrorIds = Await.result(super.findAll, maxTimeOut).map(_.streamId).distinct
    val streamIds = Await.result(streamDal.findAll, maxTimeOut).map(_.id)
    streamErrorIds.filter(errorStream => streamIds.contains(errorStream)).map(errorStream => {
      val maxStreamError = Await.result(db.run(streamErrorTable.filter(_.streamId === errorStream).sortBy(_.feedbackTime).take(1).result), minTimeOut)
      if (maxStreamError.nonEmpty) ignoreIds += maxStreamError.head.id
    })
    Await.result(super.deleteByFilter(streamError => streamError.feedbackTime <= pastNdays && !streamError.id.inSet(ignoreIds)), maxTimeOut)
  }
}
