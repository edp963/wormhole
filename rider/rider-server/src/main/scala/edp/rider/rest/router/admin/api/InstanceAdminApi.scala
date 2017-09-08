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
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.JsonProtocol._
import edp.rider.rest.router.{ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import slick.jdbc.MySQLProfile.api._
import edp.rider.rest.util.InstanceUtils._
import scala.util.{Failure, Success}

class InstanceAdminApi(instanceDal: BaseDal[InstanceTable, Instance]) extends BaseAdminApiImpl(instanceDal) with RiderLogger {

  def getByFilterRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?, 'type.as[String].?, 'conn_url.as[String].?) {
        (visible, nsSys, conn_url) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(Forbidden, getHeader(403, session))
              }
              else {
                (visible, nsSys, conn_url) match {
                  case (None, Some(sys), None) =>
                    onComplete(instanceDal.findByFilter(instance => instance.nsSys === sys.toLowerCase && instance.active === true).mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        riderLogger.info(s"user ${session.userId} select $route success where nsSys is $sys.")
                        complete(OK, ResponseSeqJson[Instance](getHeader(200, session), instances.sortBy(_.connUrl)))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select $route failed where nsSys is $sys", ex)
                        complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
                    }
                  case (None, Some(sys), Some(url)) =>
                    val nsInstance = generateNsInstance(url)
                    onComplete(instanceDal.findByFilter(_.nsInstance === nsInstance).mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        if (instances.isEmpty) {
                          if (checkFormat(sys, url)) {
                            riderLogger.info(s"user ${session.userId} check instance url $url doesn't exist, and fits the url format.")
                            complete(OK, ResponseJson[String](getHeader(200, session), nsInstance))
                          }
                          else {
                            riderLogger.info(s"user ${session.userId} check instance url $url doesn't exist, but doesn't fit the url format.")
                            complete(BadRequest, getHeader(400, getTip(sys, url), session))
                          }
                        }
                        else {
                          riderLogger.info(s"user ${session.userId} check instance url $url already exists.")
                          complete(Conflict, getHeader(409, s"$url instance already exists", session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} check instance url $url does exist failed", ex)
                        complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
                    }
                  case (_, None, None) =>
                    val future = if (visible.getOrElse(true)) instanceDal.findByFilter(_.active === true) else instanceDal.findAll
                    onComplete(future.mapTo[Seq[Instance]]) {
                      case Success(instances) =>
                        riderLogger.info(s"user ${session.userId} select all $route where active is ${visible.getOrElse(true)} success.")
                        complete(OK, ResponseSeqJson[Instance](getHeader(200, session), instances.sortBy(instance => (instance.nsSys, instance.nsInstance))))
                      case Failure(ex) =>
                        riderLogger.info(s"user ${session.userId} select all $route where active is ${visible.getOrElse(true)} failed", ex)
                        complete(UnavailableForLegalReasons, getHeader(451, ex.getMessage, session))
                    }
                  case (_, _, _) =>
                    riderLogger.error(s"user ${session.userId} request url is not supported.")
                    complete(NotImplemented, getHeader(501, session))
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
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(Forbidden, getHeader(403, session))
              }
              else {
                if (checkFormat(simple.nsSys, simple.connUrl)) {
                  val instance = Instance(0, generateNsInstance(simple.connUrl), simple.desc, simple.nsSys, simple.connUrl, active = true, currentSec, session.userId, currentSec, session.userId)
                  onComplete(instanceDal.insert(instance).mapTo[Instance]) {
                    case Success(row) =>
                      riderLogger.info(s"user ${session.userId} inserted instance $row success.")
                      complete(OK, ResponseJson[Instance](getHeader(200, session), row))
                    case Failure(ex) =>
                      if (ex.toString.contains("Duplicate entry")) {
                        riderLogger.error(s"user ${session.userId} inserted instance $instance failed", ex)
                        complete(Conflict, getHeader(409, s"${simple.nsSys} system ${simple.connUrl} instance already exists", session))
                      }
                      else {
                        riderLogger.error(s"user ${session.userId} inserted instance $instance failed", ex)
                        complete(UnavailableForLegalReasons, getHeader(451, ex.toString, session))
                      }
                  }
                }
                else {
                  riderLogger.error(s"user ${session.userId} inserted new instance failed, ${simple.connUrl} format is wrong.")
                  complete(BadRequest, getHeader(400, getTip(simple.nsSys, simple.connUrl), session))
                }
              }
          }
      }
    }

  }

}
