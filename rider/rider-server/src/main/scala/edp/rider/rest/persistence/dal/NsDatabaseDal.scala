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
import edp.rider.rest.persistence.entities.{NsDatabase, _}
import edp.rider.rest.util.CommonUtils.minTimeOut
import edp.rider.rest.util.NamespaceUtils
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import edp.rider.RiderStarter.modules._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class NsDatabaseDal(databaseTable: TableQuery[NsDatabaseTable], instanceTable: TableQuery[InstanceTable]) extends BaseDalImpl[NsDatabaseTable, NsDatabase](databaseTable) with RiderLogger {

  def getDs(visible: Boolean = true, idOpt: Option[Long] = None): Future[Seq[DatabaseInstance]] = {
    val databaseQuery = idOpt match {
      case Some(id) => databaseTable.filter(db => db.id === id)
      case None => if (visible) databaseTable.filter(_.active === visible) else databaseTable
    }
    val instanceQuery = if (visible) instanceTable.filter(_.active === visible) else instanceTable
    val result = db.run((databaseQuery join instanceQuery on (_.nsInstanceId === _.id))
      .map {
        case (database, instance) => (database.id, database.nsDatabase, database.desc, database.nsInstanceId,
          database.user, database.pwd, database.partitions, database.config, instance.nsInstance, instance.nsSys, instance.connUrl,
          database.active, database.createTime, database.createBy, database.updateTime, database.updateBy) <>(DatabaseInstance.tupled, DatabaseInstance.unapply)
      }.result).mapTo[Seq[DatabaseInstance]]
    result.map[Seq[DatabaseInstance]] {
      result =>
        result.sortBy(ds => (ds.nsSys, ds.nsInstance, ds.nsDatabase))
    }
  }

  def getDbusDs: Future[Seq[NsDatabaseInstance]] = {
    db.run((databaseTable join instanceTable.filter(_.nsSys endsWith "kafka") on (_.nsInstanceId === _.id))
      .map {
        case (database, instance) => (database.id, database.nsDatabase, instance.id, instance.nsInstance, instance.connUrl, instance.nsSys) <>(NsDatabaseInstance.tupled, NsDatabaseInstance.unapply)
      }.result).mapTo[Seq[NsDatabaseInstance]]
  }

  def delete(id: Long): (Boolean, String) = {
    try {
      val nsSeq = Await.result(namespaceDal.findByFilter(_.nsDatabaseId === id), minTimeOut).map(ns => NamespaceUtils.generateStandardNs(ns))
      if (nsSeq.nonEmpty) {
        riderLogger.info(s"database $id still has namespace ${nsSeq.mkString(",")}, can't delete it")
        (false, s"please delete namespace ${nsSeq.mkString(",")} first")
      } else {
        Await.result(super.deleteById(id), minTimeOut)
        (true, "success")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete database $id failed", ex)
        throw new Exception(s"delete database $id failed", ex)
    }
  }
}
