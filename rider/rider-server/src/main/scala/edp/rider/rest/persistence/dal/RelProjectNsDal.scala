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
import edp.rider.rest.util.NamespaceUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{CanBeQueryCondition, TableQuery}
import edp.rider.common.DbPermission._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import edp.rider.rest.util.NsDatabaseUtils._

class RelProjectNsDal(namespaceTable: TableQuery[NamespaceTable],
                      databaseTable: TableQuery[NsDatabaseTable],
                      instanceTable: TableQuery[InstanceTable],
                      projectTable: TableQuery[ProjectTable],
                      relProjectNsTable: TableQuery[RelProjectNsTable],
                      streamTable: TableQuery[StreamTable]) extends BaseDalImpl[RelProjectNsTable, RelProjectNs](relProjectNsTable) with RiderLogger {

  def getNsProjectName: Future[mutable.HashMap[Long, ArrayBuffer[String]]] = {
    val nsProjectSeq = db.run((projectTable join relProjectNsTable on (_.id === _.projectId))
      .map {
        case (project, rel) => (rel.nsId, project.name) <> (NamespaceProjectName.tupled, NamespaceProjectName.unapply)
      }.result).mapTo[Seq[NamespaceProjectName]]
    nsProjectSeq.map[mutable.HashMap[Long, ArrayBuffer[String]]] {
      val nsProjectMap = mutable.HashMap.empty[Long, ArrayBuffer[String]]
      nsProjectSeq =>
        nsProjectSeq.foreach(nsProject => {
          if (nsProjectMap.contains(nsProject.nsId))
            nsProjectMap(nsProject.nsId) = nsProjectMap(nsProject.nsId) += nsProject.name
          else
            nsProjectMap(nsProject.nsId) = ArrayBuffer(nsProject.name)
        })
        nsProjectMap
    }
  }

  def getNsIdsByProjectId(id: Long): Future[String] = super.findByFilter(_.projectId === id)
    .map[String] {
    relProjectNsSeq =>
      relProjectNsSeq.map(_.nsId).mkString(",")
  }

  def getNsByProjectId(projectIdOpt: Option[Long] = None, nsIdOpt: Option[Long] = None): Future[Seq[NamespaceTopic]] = {
    val relProjectNsQuery = projectIdOpt match {
      case Some(id) => relProjectNsTable.filter(_.projectId === id)
      case None => relProjectNsTable
    }
    val nsQuery = nsIdOpt match {
      case Some(id) => namespaceTable.filter(_.id === id)
      case None => namespaceTable
    }
    db.run(((nsQuery.filter(_.active === true) join relProjectNsQuery on (_.id === _.nsId))
      join databaseTable on (_._1.nsDatabaseId === _.id))
      .map {
        case ((ns, rel), database) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, database.nsDatabase) <> (NamespaceTopic.tupled, NamespaceTopic.unapply)
      }.distinct.result).map[Seq[NamespaceTopic]] {
      nsSeq =>
        nsSeq.map {
          ns =>
            val topic = if (ns.nsSys != "kafka" && ns.topic == ns.nsDatabase) "" else ns.topic
            NamespaceTopic(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
              ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, topic)
        }
    }
  }

//  def getFlowSourceNamespaceByProjectId(projectId: Long, streamId: Long, nsSys: String) =
//    db.run(((namespaceTable.filter(ns => ns.nsSys.startsWith(nsSys) && ns.active === true) join
//      relProjectNsTable.filter(rel => rel.projectId === projectId && rel.active === true) on (_.id === _.nsId))
//      join streamTable.filter(_.id === streamId) on (_._1.nsInstanceId === _.instanceId))
//      .map {
//        case ((ns, _), _) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
//          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy) <> (NamespaceInfo.tupled, NamespaceInfo.unapply)
//      }.result).mapTo[Seq[NamespaceInfo]]

  def getFlowSourceNamespaceByProjectId(projectId: Long, streamId: Long, nsSys: String) =
    db.run(((((streamTable.filter(_.id === streamId) join instanceTable on (_.instanceId === _.id))
      join instanceTable on (_._2.connUrl === _.connUrl))
      join namespaceTable.filter(_.nsSys.startsWith(nsSys)) on (_._2.id === _.nsInstanceId))
      join relProjectNsTable.filter(_.projectId === projectId) on (_._2.id === _.nsId))
      .map {
        case (((_, ns)), _) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy) <> (NamespaceInfo.tupled, NamespaceInfo.unapply)
      }.result).mapTo[Seq[NamespaceInfo]]

  def getFlowInstanceNamespaceByProjectId(projectId: Long, streamId: Long, nsSys: String) =
    db.run(((((streamTable.filter(_.id === streamId) join instanceTable on (_.instanceId === _.id))
      join instanceTable.filter(_.nsSys.startsWith(nsSys)) on (_._2.connUrl === _.connUrl))
      join namespaceTable on (_._2.id === _.nsInstanceId))
      join relProjectNsTable.filter(_.projectId === projectId) on (_._2.id === _.nsId))
      .map {
        case (((_, ns)), _) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy) <> (NamespaceInfo.tupled, NamespaceInfo.unapply)
      }.result).mapTo[Seq[NamespaceInfo]]

  def getJobSourceNamespaceByProjectId(projectId: Long, nsSys: String) =
    db.run((namespaceTable.filter(ns => ns.nsSys.startsWith(nsSys) && ns.active === true) join
      relProjectNsTable.filter(rel => rel.projectId === projectId && rel.active === true) on (_.id === _.nsId))
      .map {
        case (ns, _) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy) <> (NamespaceInfo.tupled, NamespaceInfo.unapply)
      }.result).mapTo[Seq[NamespaceInfo]]

  def getSinkNamespaceByProjectId(id: Long, nsSys: String) =
    db.run(((namespaceTable.filter(ns => ns.nsSys === nsSys && ns.active === true) join
      relProjectNsTable.filter(rel => rel.projectId === id && rel.active === true) on (_.id === _.nsId)) join instanceTable on (_._1.nsInstanceId === _.id))
      .map {
        case ((ns, rel), instance) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, instance.nsSys) <> (NamespaceTemp.tupled, NamespaceTemp.unapply)
      }.result).map[Seq[NamespaceInfo]] {
      nsSeq =>
        nsSeq.filter(ns => ns.nsSys == ns.nsInstanceSys)
          .map(ns => NamespaceInfo(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
            ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy))
    }

  def getSinkNamespaceByUserId(id: Long): Future[Seq[NamespaceInfo]] =
    db.run((namespaceTable.filter(_.createBy === id) join instanceTable on (_.nsInstanceId === _.id))
      .map {
        case (ns, instance) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
          ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, instance.nsSys) <> (NamespaceTemp.tupled, NamespaceTemp.unapply)
      }.result).map[Seq[NamespaceInfo]] {
      nsSeq =>
        nsSeq.filter(ns => ns.nsSys == ns.nsInstanceSys)
          .map(ns => NamespaceInfo(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
            ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy))
    }

  def getTransNamespaceByProjectId(id: Long, nsSys: String) =
    db.run((((namespaceTable.filter(ns => ns.nsSys === nsSys && ns.active === true) join relProjectNsTable.filter(rel => rel.projectId === id && rel.active === true) on (_.id === _.nsId))
      join databaseTable on (_._1.nsDatabaseId === _.id)) join instanceTable on (_._2.nsInstanceId === _.id))
      .map {
        case (((ns, rel), database), instance) => (instance, database, database.config, ns.nsSys) <> (TransNamespaceTemp.tupled, TransNamespaceTemp.unapply)
      }.distinct.result).map[Seq[TransNamespace]] {
      nsSeq =>
        nsSeq.filter(ns => ns.nsSys == ns.instance.nsSys)
          .map(ns => {
            val url = getConnUrl(ns.instance, ns.db)
            TransNamespace(ns.nsSys, ns.instance.nsInstance, ns.db.nsDatabase, url, ns.db.user, ns.db.pwd, getDbConfig(nsSys, ns.dbConfig.getOrElse("")))
          })
    }

  def getTranDbConfig(nsSys: String, nsInstance: String, nsDb: String) = {
    db.run((instanceTable.filter(instance => instance.nsSys === nsSys && instance.nsInstance === nsInstance)
      join databaseTable.filter(_.nsDatabase === nsDb) on (_.id === _.nsInstanceId))
      .map {
        case (instance, database) => (instance, database, database.config, instance.nsSys) <> (TransNamespaceTemp.tupled, TransNamespaceTemp.unapply)
      }.result).mapTo[Seq[TransNamespaceTemp]]
  }

  def getInstanceByProjectId(projectId: Long, nsSys: String): Future[Seq[Instance]] = {
    db.run((relProjectNsTable.filter(rel => rel.projectId === projectId && rel.active === true) join namespaceTable on (_.nsId === _.id) join instanceTable.filter(_.nsSys === "kafka") on (_._2.nsInstanceId === _.id)).map {
      case ((_, _), instance) => instance
    }.distinct.result).mapTo[Seq[Instance]]
  }

  def getNamespaceAdmin[C: CanBeQueryCondition](f: (NamespaceTable) => C): Future[Seq[NamespaceAdmin]] = {
    try {
      val nsProjectMap = Await.result(getNsProjectName, minTimeOut)
      db.run((namespaceTable.withFilter(f) join databaseTable on (_.nsDatabaseId === _.id))
        .map {
          case (ns, database) => (ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar, ns.keys,
            ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, database.nsDatabase) <> (NamespaceTopic.tupled, NamespaceTopic.unapply)
        }.distinct.result)
        .map[Seq[NamespaceAdmin]] {
        namespaces =>
          val nsProjectSeq = new ArrayBuffer[NamespaceAdmin]
          namespaces.foreach(ns => {
            val topic = if (ns.nsSys != "kafka" && ns.topic == ns.nsDatabase) "" else ns.topic
            if (nsProjectMap.contains(ns.id))
              nsProjectSeq += NamespaceAdmin(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar,
                ns.keys, ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy,
                nsProjectMap(ns.id).mkString(","), topic)
            else
              nsProjectSeq += NamespaceAdmin(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar,
                ns.keys, ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, ns.updateTime, ns.updateBy, "", topic)
          })
          nsProjectSeq
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"admin refresh namespaces failed", ex)
        throw ex
    }

  }

  def isAvailable(projectId: Long, nsId: Long): Boolean = {
    try {
      val rel = Await.result(super.findByFilter(rel => rel.nsId === nsId && rel.projectId === projectId), minTimeOut)
      if (rel.isEmpty) false else true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"check project id $projectId, namespace id $nsId permission failed", ex)
        throw ex
    }
  }

  def getNsByProjectId(projectId: Long): Seq[String] = {
    val nsSeq = Await.result(db.run((relProjectNsTable.filter(_.projectId === projectId) join namespaceTable on (_.nsId === _.id))
      .map {
        case (_, ns) => ns
      }.result).mapTo[Seq[Namespace]], minTimeOut)
    nsSeq.map(ns => generateStandardNs(ns))
  }

}
