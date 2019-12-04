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
import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.base.{BaseDal, BaseEntity, BaseTable, SimpleBaseEntity}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.util.{Failure, Success}

trait BaseRoutesApi extends Directives {

  def getByIdRoute(route: String): Route

  def getByAllRoute(route: String): Route

  def getByAllWithVisibleRoute(route: String): Route

  def postRoute(session: SessionClass, simple: SimpleBaseEntity, tip: String): Route

  def putRoute(session: SessionClass, base: BaseEntity): Route

  def deleteRoute(route: String): Route

}


class BaseAdminApiImpl[T <: BaseTable[A], A <: BaseEntity](baseDal: BaseDal[T, A]) extends BaseRoutesApi with RiderLogger with JsonSerializer {

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
              onComplete(baseDal.findById(id).mapTo[Option[BaseEntity]]) {
                case Success(baseEntityOpt) => baseEntityOpt match {
                  case Some(baseEntity) =>
                    riderLogger.info(s"user ${session.userId} select $route by $id success.")
                    complete(OK, ResponseJson[BaseEntity](getHeader(200, session), baseEntity))
                  case None =>
                    riderLogger.warn(s"user ${session.userId} select $route by $id, it doesn't exist.")
                    complete(OK, ResponseJson[String](getHeader(200, session), ""))
                }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select $route by $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }

  }


  override def getByAllRoute(route: String): Route = path(route) {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin") {
            riderLogger.warn(s"user ${session.userId} has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            onComplete(baseDal.findAll.mapTo[Seq[BaseEntity]]) {
              case Success(baseSeq) =>
                riderLogger.info(s"user ${session.userId} select all $route success.")
                complete(OK, ResponseSeqJson[BaseEntity](getHeader(200, session), baseSeq))
              case Failure(ex) =>
                riderLogger.error(s"user ${session.userId} select all $route failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }
          }
      }
    }

  }


  override def getByAllWithVisibleRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?) {
        visible =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val future = if (visible.getOrElse(true)) baseDal.findByFilter(_.active === visible) else baseDal.findAll
                onComplete(future.mapTo[Seq[BaseEntity]]) {
                  case Success(baseSeq) =>
                    riderLogger.info(s"user ${session.userId} select all $route success where active is ${visible.getOrElse(true)}.")
                    complete(OK, ResponseSeqJson[BaseEntity](getHeader(200, session), baseSeq))
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} select all $route failed where active is ${visible.getOrElse(true)}", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }

  override def postRoute(session: SessionClass, simple: SimpleBaseEntity, tip: String): Route = {
    if (session.roleType != "admin") {
      riderLogger.warn(s"user ${session.userId} has no permission to access it.")
      complete(OK, getHeader(403, session))
    }
    else {
      val entity = generateEntity(simple, session).asInstanceOf[A]
      onComplete(baseDal.insert(entity).mapTo[BaseEntity]) {
        case Success(base) =>
          riderLogger.info(s"user ${session.userId} insert success.")
          complete(OK, ResponseJson[BaseEntity](getHeader(200, session), base))
        case Failure(ex) =>
          if (ex.toString.contains("Duplicate entry")) {
            riderLogger.error(s"user ${session.userId} insert failed", ex)
            complete(OK, getHeader(409, tip, session))
          }
          else {
            riderLogger.error(s"user ${session.userId} insert failed", ex)
            complete(OK, getHeader(451, ex.getMessage, session))
          }
      }
    }

  }


  override def putRoute(session: SessionClass, base: BaseEntity): Route = {
    if (session.roleType != "admin") {
      riderLogger.warn(s"${session.userId} has no permission to access it.")
      complete(OK, getHeader(403, session))
    }
    else {
      val entity = generateEntity(base, session).asInstanceOf[A]
      onComplete(baseDal.update(entity)) {
        case Success(result) =>
          if (result != 0) {
            riderLogger.info(s"user ${session.userId} update success.")
            complete(OK, ResponseJson[BaseEntity](getHeader(200, session), base))
          }
          else {
            riderLogger.warn(s"user ${session.userId} update failed because it doesn't exist.")
            complete(OK, ResponseJson[String](getHeader(404, session), ""))
          }
        case Failure(ex) =>
          riderLogger.error(s"user ${session.userId} update failed", ex)
          complete(OK, getHeader(451, ex.getMessage, session))
      }
    }

  }

  override def deleteRoute(route: String): Route = path(route / LongNumber) {
    id =>
      delete {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"user ${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            } else {
              onComplete(baseDal.deleteById(id).mapTo[Int]) {
                case Success(result) =>
                  riderLogger.info(s"user ${session.userId} delete $route $id success")
                  complete(OK, getHeader(200, session))
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} delete $route $id failed", ex)
                  complete(OK, getHeader(451, session))
              }
            }
        }
      }
  }


  private def generateEntity(simple: SimpleBaseEntity, session: SessionClass): BaseEntity = {
    simple match {
      case user: SimpleUser => User(0, user.email, user.password, user.name, user.roleType, RiderConfig.riderServer.defaultLanguage, active = true, currentSec, session.userId, currentSec, session.userId)
    }
  }

  private def generateEntity(base: BaseEntity, session: SessionClass): BaseEntity = {
    base match {
      case instance: Instance => Instance(instance.id, instance.nsInstance, Some(instance.desc.getOrElse("")), instance.nsSys, instance.connUrl, instance.connConfig, instance.active, instance.createTime, instance.createBy, currentSec, session.userId)
      //      case user: User => User(user.id, user.email, user.password, user.name, user.roleType, user.active, user.createTime, user.createBy, currentSec, session.userId)
    }
  }

}
