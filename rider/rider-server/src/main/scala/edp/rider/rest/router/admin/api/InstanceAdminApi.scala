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
import edp.rider.rest.persistence.base.BaseDal
import edp.rider.rest.persistence.dal.InstanceDal
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.InstanceUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._

import scala.util.{Failure, Success}

class InstanceAdminApi(instanceDal: InstanceDal) extends BaseAdminApiImpl(instanceDal) with RiderLogger with JsonSerializer {

  def getByFilterRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?, 'type.as[String].?, 'conn_url.as[String].?, 'nsInstance.as[String].?) {
        (visible, nsSys, conn_url, nsInstance) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType == "user") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                (visible, nsSys, conn_url, nsInstance) match {
                  case (None, Some(sys), None, None) =>
                    onComplete(instanceDal.findByFilter(instance => instance.nsSys === sys.toLowerCase && instance.active === true && instance.desc =!= "dbus kafka !!!").mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        riderLogger.info(s"user ${session.userId} select $route success where nsSys is $sys.")
                        complete(OK, ResponseSeqJson[Instance](getHeader(200, session), instances.sortBy(_.connUrl)))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select $route failed where nsSys is $sys", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (None, Some(sys), Some(url), None) =>
                    onComplete(instanceDal.findByFilter(_.connUrl === conn_url).mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        if (instances.isEmpty) {
                          if (checkSys(sys) && checkFormat(sys, url)) {
                            riderLogger.info(s"user ${session.userId} check instance url $url doesn't exist, and fits the url format.")
                            complete(OK, ResponseJson[String](getHeader(200, session), url))
                          }
                          else {
                            riderLogger.info(s"user ${session.userId} check instance url $url doesn't exist, but doesn't fit the url format.")
                            complete(OK, getHeader(400, getTip(sys, url), session))
                          }
                        }
                        else {
                          riderLogger.info(s"user ${session.userId} check instance url $url already exists.")
                          complete(OK, getHeader(409, s"$url already exists, are you sure to create it again", session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} check instance url $url does exist failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (None, Some(sys), None, Some(nsInstanceInput)) =>
                    if (namePattern.matcher(nsInstanceInput).matches()) {
                      onComplete(instanceDal.findByFilter(instance => instance.nsInstance === nsInstanceInput && instance.nsSys === sys.toLowerCase).mapTo[Seq[Instance]]) {
                        case Success(instances) =>
                          if (session.roleType == "app") {
                            if(instances.isEmpty)
                              complete(OK, ResponseJson[String](getHeader(404, session), "Not Found"))
                            else
                              complete(OK, ResponseJson[Long](getHeader(200, session), instances.head.id))
                          } else {
                            if (instances.isEmpty) {
                              riderLogger.info(s"user ${session.userId} check nsSys $sys instance nsInstance $nsInstanceInput doesn't exist")
                              complete(OK, getHeader(200, session))
                            }
                            else {
                              riderLogger.info(s"user ${session.userId} check nsSys $sys instance nsInstance $nsInstanceInput already exists.")
                              complete(OK, getHeader(409, s"$nsInstanceInput instance already exists", session))
                            }
                          }
                        case Failure(ex) =>
                          riderLogger.error(s"user ${session.userId} check nsSys $sys instance nsInstance $nsInstanceInput does exist failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    } else {
                      riderLogger.info(s"user ${session.userId} nsSys $sys instance nsInstance $nsInstanceInput format is wrong.")
                      complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
                    }
                  case (_, None, None, None) =>
                    val future = if (visible.getOrElse(true)) instanceDal.findByFilter(_.active === true) else instanceDal.findAll
                    onComplete(future.mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        riderLogger.info(s"user ${session.userId} select all $route where active is ${visible.getOrElse(true)} success.")
                        complete(OK, ResponseSeqJson[Instance](getHeader(200, session), instances.sortBy(instance => (instance.nsSys, instance.nsInstance))))
                      case Failure(ex) =>
                        riderLogger.info(s"user ${session.userId} select all $route where active is ${visible.getOrElse(true)} failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
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
      entity(as[SimpleInstance]) {
        simple =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType == "user") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                if (namePattern.matcher(simple.nsInstance).matches()) {
                  if (checkSys(simple.nsSys) && checkFormat(simple.nsSys, simple.connUrl)) {
                    val instance = Instance(0, simple.nsInstance.trim, simple.desc, simple.nsSys.trim, simple.connUrl.trim, simple.connConfig, active = true, currentSec, session.userId, currentSec, session.userId)
                    onComplete(instanceDal.insert(instance).mapTo[Instance]) {
                      case Success(row) =>
                        riderLogger.info(s"user ${session.userId} inserted instance $row success.")
                        complete(OK, ResponseJson[Instance](getHeader(200, session), row))
                      case Failure(ex) =>
                        if (ex.toString.contains("Duplicate entry")) {
                          riderLogger.error(s"user ${session.userId} insert instance failed", ex)
                          complete(OK, getHeader(409, s"${simple.nsSys} system ${simple.nsInstance} instance already exists", session))
                        }
                        else {
                          riderLogger.error(s"user ${session.userId} insert instance failed", ex)
                          complete(OK, getHeader(451, ex.toString, session))
                        }
                    }
                  }
                  else {
                    riderLogger.error(s"user ${session.userId} insert instance failed, ${simple.connUrl} format is wrong.")
                    complete(OK, getHeader(400, getTip(simple.nsSys, simple.connUrl), session))
                  }
                } else {
                  riderLogger.info(s"user ${session.userId} nsSys ${simple.nsSys} instance nsInstance ${simple.nsInstance} format is wrong.")
                  complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
                }
              }
          }
      }
    }
  }

  def putRoute(route: String): Route = path(route) {
    put {
      entity(as[Instance]) {
        instance =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                if (namePattern.matcher(instance.nsInstance).matches()) {
                  if (checkSys(instance.nsSys) && checkFormat(instance.nsSys, instance.connUrl)) {
                    val instanceUpdate = Instance(instance.id, instance.nsInstance.trim, instance.desc, instance.nsSys.trim, instance.connUrl.trim, instance.connConfig, instance.active, instance.createTime, instance.createBy, currentSec, session.userId)
                    onComplete(instanceDal.update(instanceUpdate).mapTo[Int]) {
                      case Success(_) =>
                        riderLogger.info(s"user ${session.userId} update instance success.")
                        complete(OK, ResponseJson[Instance](getHeader(200, session), instanceUpdate))
                      case Failure(ex) =>
                        if (ex.toString.contains("Duplicate entry")) {
                          riderLogger.error(s"user ${session.userId} update instance failed", ex)
                          complete(OK, getHeader(409, s"${instance.nsSys} system ${instance.connUrl} instance already exists", session))
                        }
                        else {
                          riderLogger.error(s"user ${session.userId} update instance failed", ex)
                          complete(OK, getHeader(451, ex.toString, session))
                        }
                    }
                  }
                  else {
                    riderLogger.error(s"user ${session.userId} updated instance failed, ${instance.connUrl} format is wrong.")
                    complete(OK, getHeader(400, getTip(instance.nsSys, instance.connUrl), session))
                  }
                } else {
                  riderLogger.info(s"user ${session.userId} nsSys ${instance.nsSys} instance nsInstance ${instance.nsInstance} format is wrong.")
                  complete(OK, getHeader(402, s"begin with a letter, certain special characters as '_', '-', end with a letter or number", session))
                }
              }
          }
      }
    }

  }

  override def deleteRoute(route: String): Route = path(route / LongNumber) {
    id =>
      delete {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              try {
                val result = instanceDal.delete(id)
                if (result._1) {
                  riderLogger.error(s"user ${session.userId} delete instance $id success.")
                  complete(OK, getHeader(200, session))
                }
                else complete(OK, getHeader(412, result._2, session))
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} delete instance $id failed", ex)
                  complete(OK, getHeader(451, session))
              }
            }
        }
      }
  }

}
