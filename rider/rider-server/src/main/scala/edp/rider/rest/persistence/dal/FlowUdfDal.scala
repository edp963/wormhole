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

import edp.rider.common.RiderLogger
import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.UdfUtils
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class FlowUdfDal(udfTable: TableQuery[UdfTable], relProjectUdfDal: RelProjectUdfDal, flowUdfTable: TableQuery[FlowUdfTable],
             projectDal: ProjectDal, streamDal: StreamDal) extends BaseDalImpl[FlowUdfTable, FlowUdf](flowUdfTable) with RiderLogger {

  def getFlowUdf(flowIds: Seq[Long], udfIdsOpt: Option[Seq[Long]] = None): Seq[FlowUdfResponse] = {
    val udfQuery = udfIdsOpt match {
      case Some(udfIds) => udfTable.filter(_.id inSet (udfIds)).filter(_.streamType === "flink")
      case None => udfTable.filter(_.streamType === "flink")
    }
    try {
      Await.result(db.run((flowUdfTable.filter(_.flowId inSet flowIds) join udfQuery on (_.udfId === _.id))
        .map {
          case (flowUdf, udf) => (flowUdf.udfId, udf.functionName, udf.fullClassName, udf.jarName, udf.mapOrAgg) <> (FlowUdfResponse.tupled, FlowUdfResponse.unapply)
        }.result).mapTo[Seq[FlowUdfResponse]], minTimeOut)
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getFlowUdf(flowId: Long): Seq[FlowUdfResponse] = {
    getFlowUdf(Seq(flowId))
  }

  def getDeleteUdfIds(flowId: Long, udfIds: Seq[Long]): Seq[Long] = {
    val udfs = Await.result(super.findByFilter(udf => udf.flowId === flowId), minTimeOut)
    udfs.filter(udf => !udfIds.contains(udf.udfId)).map(_.udfId)
  }
}
