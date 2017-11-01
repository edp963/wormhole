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
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class RelProjectUdfDal(udfTable: TableQuery[UdfTable],
                       projectTable: TableQuery[ProjectTable],
                       relProjectUdfTable: TableQuery[RelProjectUdfTable]) extends BaseDalImpl[RelProjectUdfTable, RelProjectUdf](relProjectUdfTable) with RiderLogger {

  def getUdfIdsByProjectId(id: Long): Future[String] = super.findByFilter(_.projectId === id)
    .map[String] {
    relProjectUdfSeq =>
      relProjectUdfSeq.map(_.udfId).mkString(",")
  }

  def getUdfByProjectId(id: Long): Future[Seq[Udf]] = {
    db.run((udfTable.filter(_.public === true) union udfTable.filter(_.public === false) join relProjectUdfTable.filter(_.projectId === id) on (_.id === _.udfId))
      .map {
        case (udf, _) => (udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.public, udf.createTime, udf.createBy, udf.updateTime, udf.updateBy) <> (Udf.tupled, Udf.unapply)
      }.result).mapTo[Seq[Udf]]
  }

  def getNonPublicUdfByProjectId(id: Long): Future[Seq[Udf]] = {
    db.run((udfTable.filter(_.public === false) join relProjectUdfTable.filter(_.projectId === id) on (_.id === _.udfId))
      .map {
        case (udf, _) => (udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.public, udf.createTime, udf.createBy, udf.updateTime, udf.updateBy) <> (Udf.tupled, Udf.unapply)
      }.result).mapTo[Seq[Udf]]
  }


  def getUdfProjectName: Future[mutable.HashMap[Long, ArrayBuffer[String]]] = {
    val udfProjectSeq = db.run((projectTable join relProjectUdfTable on (_.id === _.projectId))
      .map {
        case (project, rel) => (rel.udfId, project.name) <> (UdfProjectName.tupled, UdfProjectName.unapply)
      }.result).mapTo[Seq[UdfProjectName]]
    udfProjectSeq.map[mutable.HashMap[Long, ArrayBuffer[String]]] {
      val udfProjectMap = mutable.HashMap.empty[Long, ArrayBuffer[String]]
      udfProjectSeq =>
        udfProjectSeq.foreach(udfProject => {
          if (udfProjectMap.contains(udfProject.udfId))
            udfProjectMap(udfProject.udfId) = udfProjectMap(udfProject.udfId) += udfProject.name
          else
            udfProjectMap(udfProject.udfId) = ArrayBuffer(udfProject.name)
        })
        udfProjectMap
    }
  }

}
