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

import scala.concurrent.Await

class FeedbackHeartbeatDal(heartbeatTable: TableQuery[FeedbackHeartbeatTable], streamDal: StreamDal) extends BaseDalImpl[FeedbackHeartbeatTable, FeedbackHeartbeat](heartbeatTable) {

  def deleteHistory(pastNdays: String) = {
    val deleteMaxId = Await.result(
      db.run(heartbeatTable.withFilter(_.feedbackTime <= pastNdays).map(_.id).max.result).mapTo[Option[Long]], minTimeOut)
    if (deleteMaxId.nonEmpty) Await.result(super.deleteByFilter(_.id <= deleteMaxId), maxTimeOut)
  }
}
