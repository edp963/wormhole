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

import edp.rider.common.{DatabaseSearchException, RiderLogger}
import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.duration.Duration.Inf

class RelStreamUdfDal(relStreamUdfTable: TableQuery[RelStreamUdfTable], udfTable: TableQuery[UdfTable]) extends BaseDalImpl[RelStreamUdfTable, RelStreamUdf](relStreamUdfTable) with RiderLogger {

  def getStreamUdf(streamIds: Seq[Long], udfIdsOpt: Option[Seq[Long]] = None): Seq[StreamUdfResponse] = {
    val udfQuery = udfIdsOpt match {
      case Some(udfIds) => udfTable.filter(_.id inSet (udfIds)).filter(_.streamType === "spark")
      case None => udfTable.filter(_.streamType === "spark")
    }
    try {
      Await.result(db.run((relStreamUdfTable.filter(_.streamId inSet streamIds) join udfQuery on (_.udfId === _.id))
        .map {
          case (relStreamUdf, udf) => (relStreamUdf.udfId, relStreamUdf.streamId, udf.functionName, udf.fullClassName, udf.jarName) <> (StreamUdfResponse.tupled, StreamUdfResponse.unapply)
        }.result).mapTo[Seq[StreamUdfResponse]], minTimeOut)
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getStreamUdf(streamId: Long): Seq[StreamUdfResponse] = {
    getStreamUdf(Seq(streamId))
  }

  def getDeleteUdfIds(streamId: Long, udfIds: Seq[Long]): Seq[Long] = {
    val udfs = Await.result(super.findByFilter(udf => udf.streamId === streamId), minTimeOut)
    udfs.filter(udf => !udfIds.contains(udf.udfId)).map(_.udfId)
  }
}
