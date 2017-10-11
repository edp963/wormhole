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


package edp.rider.rest.router.admin.api

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.dal.{NamespaceDal, NsDatabaseDal, RelProjectNsDal}
import edp.rider.rest.persistence.entities.{Namespace, _}
import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class NamespaceAdminApi(namespaceDal: NamespaceDal, databaseDal: NsDatabaseDal, relProjectNsDal: RelProjectNsDal) extends BaseAdminApiImpl(namespaceDal) with RiderLogger {

  override def getByIdRoute(route: String): Route = path(route / LongNumber) {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(relProjectNsDal.getNamespaceAdmin(_.id === id).mapTo[Seq[NamespaceAdmin]]) {
                case Success(nsSeq) =>
                  if (nsSeq.nonEmpty)
                    complete(OK, ResponseJson[NamespaceAdmin](getHeader(200, session), nsSeq.head))
                  else
                    complete(OK, ResponseJson[String](getHeader(200, session), ""))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select namespace by $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  override def getByAllRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?, 'instanceId.as[Long].?, 'databaseId.as[Long].?, 'tableNames.as[String].?) {
        (visible, instanceIdOpt, databaseIdOpt, tableNamesOpt) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                (visible, instanceIdOpt, databaseIdOpt, tableNamesOpt) match {
                  case (_, Some(instanceId), Some(databaseId), Some(tableNames)) =>
                    onComplete(namespaceDal.findByFilter(ns => ns.nsInstanceId === instanceId && ns.nsDatabaseId === databaseId).mapTo[Seq[Namespace]]) {
                      case Success(nsSeq) =>
                        val tableNameSeq = tableNames.split(",")
                        val tables = nsSeq.map(ns => ns.nsTable)
                        val OKTables = tables.filter(tableNameSeq.contains(_))
                        if (OKTables.isEmpty) {
                          riderLogger.info(s"user ${session.userId} check table name $tableNames doesn't exist success.")
                          complete(OK, getHeader(200, session))
                        }
                        else {
                          riderLogger.error(s"user ${session.userId} check table name $OKTables already exists success.")
                          complete(OK, getHeader(409, s"${OKTables.mkString(",")} already exists", session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} check table name $tableNames does exist failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (_, None, None, None) => synchronizeNs(session, visible.getOrElse(true))
                  case (_, _, _, _) =>
                    riderLogger.error(s"user ${session.userId} request url is not supported.")
                    complete(OK, getHeader(501, session))
                }
              }
          }
      }
    }
  }

  def postRoute(route: String): Route = path(route) {
    post {
      entity(as[SimpleNamespace]) {
        simple =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                onComplete(databaseDal.findById(simple.nsDatabaseId).mapTo[Option[NsDatabase]]) {
                  case Success(dbOpt) =>
                    riderLogger.info(s"user ${session.userId} select database where id is ${simple.nsDatabaseId} success.")
                    dbOpt match {
                      case Some(db) =>
                        val permission = db.permission
                        val nsSeq = new ArrayBuffer[Namespace]
                        simple.nsTables.map(nsTable => {
                          nsSeq += Namespace(0, simple.nsSys, simple.nsInstance, simple.nsDatabase, nsTable.table, "*", "*", "*", permission, nsTable.key,
                            simple.nsDatabaseId, simple.nsInstanceId, active = true, currentSec, session.userId, currentSec, session.userId)
                        })
                        onComplete(namespaceDal.insert(nsSeq).mapTo[Seq[Namespace]]) {
                          case Success(seq) =>
                            riderLogger.info(s"user ${session.userId} inserted $route $seq success.")
                            val ids = new ArrayBuffer[Long]
                            seq.foreach(ns => ids += ns.id)
                            onComplete(relProjectNsDal.getNamespaceAdmin(_.id inSet ids).mapTo[Seq[NamespaceAdmin]]) {
                              case Success(nsProject) =>
                                riderLogger.info(s"user ${session.userId} select $route where id in $ids success.")
                                complete(OK, ResponseSeqJson[NamespaceAdmin](getHeader(200, session), nsProject))
                              case Failure(ex) =>
                                riderLogger.error(s"user ${session.userId} select $route where id in $ids failed", ex)
                                complete(OK, getHeader(451, ex.getMessage, session))
                            }
                          case Failure(ex) =>
                            if (ex.getMessage.contains("Duplicate entry")) {
                              onComplete(namespaceDal.findByFilter(ns => ns.nsInstanceId === simple.nsInstanceId && ns.nsDatabaseId === simple.nsDatabaseId).mapTo[Seq[Namespace]]) {
                                case Success(rows) =>
                                  val tables = rows.map(ns => ns.nsTable)
                                  val OKTables = tables.filter(simple.nsTables.contains(_))
                                  riderLogger.error(s"user ${session.userId} inser namespace ${OKTables.mkString(",")} table failed", ex)
                                  complete(OK, getHeader(409, s"${OKTables.mkString(",")} already exists", session))
                                case Failure(e) =>
                                  riderLogger.error(s"user ${session.userId} insert namespace failed", e)
                                  complete(OK, getHeader(451, e.getMessage, session))
                              }
                            }
                            else {
                              riderLogger.error(s"user ${session.userId} insert namespace failed", ex)
                              complete(OK, getHeader(451, ex.getMessage, session))
                            }
                        }
                      case None =>
                        riderLogger.info(s"user ${session.userId} select databases where id is ${simple.nsDatabaseId} failed, it doesn't exist.")
                        complete(OK, getHeader(451, s"${simple.nsDatabaseId} doesn't exist, please reselect", session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select database where id is ${simple.nsDatabaseId} failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }

  def putRoute(route: String): Route = path(route) {
    put {
      entity(as[Namespace]) {
        ns =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val namespace = Namespace(ns.id, ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar,
                  ns.permission, ns.keys, ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, currentSec, session.userId)
                onComplete(namespaceDal.update(namespace).mapTo[Int]) {
                  case Success(num) =>
                    riderLogger.info(s"user ${session.userId} update namespace success.")
                    onComplete(relProjectNsDal.getNamespaceAdmin(_.id === ns.id).mapTo[Seq[NamespaceAdmin]]) {
                      case Success(nsProject) =>
                        riderLogger.info(s"user ${session.userId} select $route where id is ${ns.id} success.")
                        complete(OK, ResponseJson[NamespaceAdmin](getHeader(200, session), nsProject.head))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select $route where id is ${ns.id} failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} update namespace failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }

  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "namespaces") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(relProjectNsDal.getNsByProjectId(Some(id)).mapTo[Seq[NamespaceTopic]]) {
                case Success(nsSeq) =>
                  riderLogger.info(s"user ${session.userId} select all namespaces where project id $id success.")
                  complete(OK, ResponseSeqJson[NamespaceTopic](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.permission))))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select all namespaces where project id $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }

  }

  def getNsByProjectRoute(route: String): Route = path(route / "namespaces") {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin")
            complete(OK, getHeader(403, session))
          else {
            getNsRoute(session, true)
          }
      }
    }

  }


  private def getNsRoute(session: SessionClass, visible: Boolean): Route = {
    onComplete(relProjectNsDal.getNamespaceAdmin(_.active === visible).mapTo[Seq[NamespaceAdmin]]) {
      case Success(res) =>
        riderLogger.info(s"user ${session.userId} select all namespaces success.")
        complete(OK, ResponseSeqJson[NamespaceAdmin](getHeader(200, session), res))
      case Failure(ex) =>
        riderLogger.error(s"user ${session.userId} select all namespaces failed", ex)
        complete(OK, getHeader(451, ex.getMessage, session))
    }

  }

  private def synchronizeNs(session: SessionClass, visible: Boolean): Route = {
    onComplete(namespaceDal.dbusInsert(session).mapTo[Seq[Dbus]]) {
      case Success(dbusUpsert) =>
        riderLogger.info(s"user ${session.userId} insertOrUpdate dbus table success.")
        val namespaceDbus = namespaceDal.generateNamespaceSeqByDbus(dbusUpsert, session)
        onComplete(namespaceDal.insertOrUpdate(namespaceDbus).mapTo[Int]) {
          case Success(result) =>
            riderLogger.info(s"user ${session.userId} insertOrUpdate $result dbus namespaces success.")
            getNsRoute(session, visible)
          case Failure(ex) =>
            riderLogger.error(s"user ${session.userId} insertOrUpdate dbus namespaces failed", ex)
            getNsRoute(session, visible)
        }
      case Failure(ex) =>
        riderLogger.error(s"user ${session.userId} insertOrUpdate dbus table failed", ex)
        getNsRoute(session, visible)
    }
  }


}

