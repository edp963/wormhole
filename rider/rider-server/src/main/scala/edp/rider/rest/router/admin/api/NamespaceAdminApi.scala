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
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class NamespaceAdminApi(namespaceDal: NamespaceDal, databaseDal: NsDatabaseDal, relProjectNsDal: RelProjectNsDal) extends BaseAdminApiImpl(namespaceDal) with RiderLogger with JsonSerializer {

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
                case Success(ns) =>
                  complete(OK, ResponseJson[NamespaceAdmin](getHeader(200, session), ns.head))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select namespace by $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  def getUmsInfoByIdRoute(route: String): Route = path(route / LongNumber / "schema" / "source") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(namespaceDal.getUmsInfo(id).mapTo[Option[SourceSchema]]) {
                case Success(umsInfo) =>
                  riderLogger.info(s"user ${session.userId} select namespace source schema by $id success")
                  complete(OK, ResponseJson[Option[SourceSchema]](getHeader(200, session), umsInfo))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select namespace source schema by $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }

  def getSinkInfoByIdRoute(route: String): Route = path(route / LongNumber / "schema" / "sink") {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(namespaceDal.getSinkInfo(id).mapTo[Option[SinkSchema]]) {
                case Success(sinkInfo) =>
                  riderLogger.info(s"user ${session.userId} select namespace sink schema by $id success")
                  complete(OK, ResponseJson[Option[SinkSchema]](getHeader(200, session), sinkInfo))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select namespace sink schema by $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }
  }


  override def getByAllRoute(route: String): Route
  = path(route) {
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
                    var flag = true
                    tableNames.split(",").foreach(table => if (!namePattern.matcher(table).matches()) flag = false)
                    if (flag) {
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
                    } else {
                      riderLogger.info(s"user ${session.userId} check tables $tableNames format is wrong.")
                      complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
                    }
                  case (_, None, None, None) => synchronizeNs(session, visible.getOrElse(true))
                  case (_, _, _, _) =>
                    riderLogger.error(s"user ${session.userId} request url is not supported.")
                    complete(OK, ResponseJson[String](getHeader(403, session), msgMap(403)))
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
              if (session.roleType == "user") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                var flag = true
                simple.nsTables.map(_.table).foreach(table => if (!namePattern.matcher(table).matches()) flag = false)
                if (flag) {
                  onComplete(databaseDal.findById(simple.nsDatabaseId).mapTo[Option[NsDatabase]]) {
                    case Success(dbOpt) =>
                      riderLogger.info(s"user ${session.userId} select database where id is ${simple.nsDatabaseId} success.")
                      dbOpt match {
                        case Some(_) =>
                          val nsSeq = new ArrayBuffer[Namespace]
                          simple.nsTables.map(nsTable => {
                            nsSeq += Namespace(0, simple.nsSys.trim, simple.nsInstance.trim, simple.nsDatabase.trim, nsTable.table.trim, "*", "*", "*", nsTable.key,
                              None, None, simple.nsDatabaseId, simple.nsInstanceId, active = true, currentSec, session.userId, currentSec, session.userId)
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
                                    val existTables = tables.filter(simple.nsTables.contains(_))
                                    riderLogger.error(s"user ${session.userId} insert namespace ${existTables.mkString(",")} table failed", ex)
                                    complete(OK, getHeader(409, s"${existTables.mkString(",")} already exists", session))
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
                } else {
                  riderLogger.info(s"user ${session.userId} check tables ${simple.nsTables.map(_.table).mkString(",")} format is wrong.")
                  complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
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
                if (namePattern.matcher(ns.nsTable).matches()) {
                  val namespace = Namespace(ns.id, ns.nsSys.trim, ns.nsInstance.trim, ns.nsDatabase.trim, ns.nsTable.trim, ns.nsVersion, ns.nsDbpar, ns.nsTablepar,
                    ns.keys, ns.sourceSchema, ns.sinkSchema, ns.nsDatabaseId, ns.nsInstanceId, ns.active, ns.createTime, ns.createBy, currentSec, session.userId)
                  onComplete(namespaceDal.update(namespace).mapTo[Int]) {
                    case Success(_) =>
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
                } else {
                  riderLogger.info(s"user ${session.userId} check tables ${ns.nsTable} format is wrong.")
                  complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
                }
              }
          }
      }
    }

  }

  def putSourceInfoRoute(route: String): Route = path(route / LongNumber / "schema" / "source") {
    id =>
      put {
        entity(as[SourceSchema]) {
          ums =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "admin") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  val umsFinal =
                    if (ums.jsonSample.isEmpty || ums.jsonSample == null || ums.jsonSample.getOrElse("null") == "null")
                      null
                    else
                      ums
                  onComplete(namespaceDal.updateUmsInfo(id, umsFinal, session.userId).mapTo[Int]) {
                    case Success(_) =>
                      riderLogger.info(s"user ${session.userId} update namespace source schema success.")
                      complete(OK, ResponseJson[SourceSchema](getHeader(200, session), ums))
                    case Failure(ex) =>
                      riderLogger.error(s"user ${session.userId} update namespace source schema failed", ex)
                      complete(OK, getHeader(451, ex.getMessage, session))
                  }
                }
            }
        }
      }

  }

  def putSinkInfoRoute(route: String): Route = path(route / LongNumber / "schema" / "sink") {
    id =>
      put {
        entity(as[SinkSchema]) {
          schema =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "admin") {
                  riderLogger.warn(s"${session.userId} has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  val schemaFinal =
                    if (schema.jsonSample.isEmpty || schema.jsonSample == null || schema.jsonSample.getOrElse("null") == "null")
                      null
                    else schema
                  onComplete(namespaceDal.updateSinkInfo(id, schemaFinal, session.userId).mapTo[Int]) {
                    case Success(_) =>
                      riderLogger.info(s"user ${session.userId} update namespace sink schema success.")
                      complete(OK, ResponseJson[SinkSchema](getHeader(200, session), schema))
                    case Failure(ex) =>
                      riderLogger.error(s"user ${session.userId} update namespace sink schema failed", ex)
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
                  complete(OK, ResponseSeqJson[NamespaceTopic](getHeader(200, session), nsSeq.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable))))
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
        complete(OK, ResponseSeqJson[NamespaceAdmin](getHeader(200, session),
          res.sortBy(ns => (ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable))))
      case Failure(ex) =>
        riderLogger.error(s"user ${session.userId} select all namespaces failed", ex)
        complete(OK, getHeader(451, ex.getMessage, session))
    }

  }

  private def synchronizeNs(session: SessionClass, visible: Boolean): Route

  = {
    try {
      onComplete(namespaceDal.dbusInsert(session).mapTo[Seq[Dbus]]) {
        case Success(dbusUpsert) =>
          val (insertSeq, updateSeq) = namespaceDal.generateNamespaceSeqByDbus(dbusUpsert, session)
          onComplete(namespaceDal.insert(insertSeq)) {
            case Success(_) =>
              riderLogger.info(s"user ${session.userId} insert dbus namespaces success.")
              getNsRoute(session, visible)
            //              onComplete(namespaceDal.update(updateSeq)) {
            //                case Success(_) =>
            //                  riderLogger.info(s"user ${session.userId} update dbus namespaces success.")
            //                  getNsRoute(session, visible)
            //                case Failure(ex) =>
            //                  riderLogger.error(s"user ${session.userId} update dbus namespaces failed", ex)
            //                  getNsRoute(session, visible)
            //              }
            case Failure(ex) =>
              riderLogger.error(s"user ${session.userId} insertOrUpdate dbus namespaces failed", ex)
              getNsRoute(session, visible)
          }
        case Failure(ex) =>
          riderLogger.error(s"user ${session.userId} insertOrUpdate dbus table failed", ex)
          getNsRoute(session, visible)
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user ${session.userId} synchronize dbus tables failed", ex)
        getNsRoute(session, visible)
    }

  }


  override def deleteRoute(route: String): Route

  = path(route / LongNumber) {
    id =>
      delete {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${
                session.userId
              } has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              try {
                val result = namespaceDal.delete(id)
                if (result._1) {
                  riderLogger.error(s"user ${
                    session.userId
                  } delete namespace $id success.")
                  complete(OK, getHeader(200, session))
                }
                else complete(OK, getHeader(412, result._2, session))
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${
                    session.userId
                  } delete namespace $id failed", ex)
                  complete(OK, getHeader(451, session))
              }
            }
        }
      }
  }

  def getSinkNsByUser(route: String): Route = path("namespaces" / "sink") {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "app") {
            riderLogger.warn(s"${
              session.userId
            } has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            try {
              onComplete(relProjectNsDal.getSinkNamespaceByUserId(session.userId).mapTo[Seq[NamespaceInfo]]) {
                case Success(nsSeq) =>
                  riderLogger.info(s"user ${
                    session.userId
                  } get sink namespaces success.")
                  complete(OK, ResponseSeqJson[NamespaceInfo](getHeader(200, session), nsSeq))
                case Failure(ex) =>
                  riderLogger.info(s"user ${
                    session.userId
                  } get sink namespaces failed", ex)
                  complete(OK, getHeader(451, session))
              }
            } catch {
              case ex: Exception =>
                riderLogger.error(s"user ${
                  session.userId
                } get sink namespaces failed", ex)
                complete(OK, getHeader(451, session))
            }
          }
      }
    }

  }


}

