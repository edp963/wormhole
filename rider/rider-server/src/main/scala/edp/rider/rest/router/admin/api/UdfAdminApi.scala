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

import scala.language.postfixOps
import edp.rider.rest.persistence.dal.{RelProjectUdfDal, UdfDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.StreamUtils
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.UdfUtils._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}


class UdfAdminApi(udfDal: UdfDal, relProjectUdfDal: RelProjectUdfDal) extends BaseAdminApiImpl(udfDal) with RiderLogger with JsonSerializer {

  override def getByAllRoute(route: String): Route = path(route) {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin") {
            riderLogger.warn(s"${session.userId} has no permission to access it.")
            complete(OK, getHeader(403, session))
          }
          else {
            onComplete(udfDal.getUdfProject.mapTo[Seq[UdfProject]]) {
              case Success(udfProjects) =>
                riderLogger.info(s"user ${session.userId} select all $route success.")
                complete(OK, ResponseSeqJson[UdfProject](getHeader(200, session), udfProjects.sortBy(_.functionName)))
              case Failure(ex) =>
                riderLogger.error(s"user ${session.userId} select all $route failed", ex)
                complete(OK, getHeader(451, ex.getMessage, session))
            }

          }
      }
    }

  }

  def getById(route: String): Route = path(route / LongNumber) {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              udfDal.getUdfProjectById(id) match {
                case Some(udfProject) =>
                  riderLogger.info(s"user ${session.userId} select udf $id success.")
                  complete(OK, ResponseJson[UdfProject](getHeader(200, session), udfProject))
                case None =>
                  riderLogger.info(s"user ${session.userId} select udf $id success, but it doesn't exist.")
                  complete(OK, ResponseJson[String](getHeader(200, session), ""))
              }
            }
        }
      }
  }

  def postRoute(route: String): Route = path(route) {
    post {
      entity(as[SimpleUdf]) {
        simpleUdf =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${
                  session.userId
                } has no permission to access it.")
                complete(OK, getHeader(403, session))
              } else {
                postResponse(simpleUdf, session)
              }
          }
      }
    }
  }

  def putRoute(route: String): Route = path(route) {
    put {
      entity(as[Udf]) {
        udf =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"user ${
                  session.userId
                } has no permission to access it.")
                complete(OK, getHeader(403, session))
              } else {
                putResponse(udf, session)
              }
          }
      }
    }
  }

  override def deleteRoute(route: String): Route = path(route / LongNumber) {
    id =>
      delete {
        authenticateOAuth2Async("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.error(s"user ${
                session.userId
              } has no permission to access it")
              complete(OK, getHeader(403, session))
            } else {
              deleteResponse(id, session)
            }
        }
      }
  }

  def getNonPublicUdfRoute(route: String): Route = path(route / "udfs") {
    get {
      authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
        session =>
          if (session.roleType != "admin")
            complete(OK, getHeader(403, session))
          else {
            getNonPublicUdfResponse(session)
          }
      }
    }

  }

  private def postResponse(simpleUdf: SimpleUdf, session: SessionClass): Route

  = {
    if (simpleUdf.streamType == "spark" && (!checkHdfsPathExist(simpleUdf.jarName))) {
      riderLogger.warn(s"user ${
        session.userId
      } insert udf, but jar ${
        simpleUdf.jarName
      } doesn't exist in hdfs")
      complete(OK, getHeader(412, s"jar ${
        simpleUdf.jarName
      } doesn't exist in hdfs", session))
    } else {
      val function = Await.result(udfDal.findByFilter(_.functionName === simpleUdf.functionName).mapTo[Seq[Udf]], minTimeOut)

      val msg =
        if (function.nonEmpty)
          s"function_name ${
            simpleUdf.functionName
          } already exists"
        else ""
      if (msg != "") {
        riderLogger.warn(msg)
        complete(OK, getHeader(409, msg, session))
      } else {
        val udfInsert = Udf(0, simpleUdf.functionName.trim, simpleUdf.fullClassName.trim, simpleUdf.jarName.trim, simpleUdf.desc, simpleUdf.public, simpleUdf.streamType, simpleUdf.mapOrAgg, currentSec, session.userId, currentSec, session.userId)
        onComplete(udfDal.insert(udfInsert).mapTo[Udf]) {
          case Success(udf) =>
            riderLogger.info(s"user ${
              session.userId
            } insert udf success")
            complete(OK, ResponseJson[Udf](getHeader(200, session), udf))
          case Failure(ex) =>
            riderLogger.error(s"user ${
              session.userId
            } insert udf failed", ex)
            if (ex.toString.contains("Duplicate entry"))
              complete(OK, getHeader(409, s"udf already exists", session))
            else
              complete(OK, getHeader(451, ex.toString, session))
        }
      }
    }
  }

  private def putResponse(udf: Udf, session: SessionClass): Route

  = {
    if (udf.streamType == "spark" && (!checkHdfsPathExist(udf.jarName))) {
    //if (!checkHdfsPathExist(udf.jarName)) {
      riderLogger.warn(s"user ${
        session.userId
      } update udf, but jar ${
        udf.jarName
      } doesn't exist in hdfs")
      complete(OK, getHeader(412, s"jar ${
        udf.jarName
      } doesn't exist in hdfs", session))
    } else {
      val udfSearch = Await.result(udfDal.findById(udf.id), minTimeOut)
      val updateUdf = Udf(udf.id, udf.functionName.trim, udf.fullClassName.trim, udf.jarName.trim, udf.desc, udf.pubic, udf.streamType, udf.mapOrAgg, udf.createTime, udf.createBy, currentSec, session.userId)
      onComplete(udfDal.update(updateUdf).mapTo[Int]) {
        case Success(_) =>
          riderLogger.info(s"user ${
            session.userId
          } update udf success")
          if (udfSearch.nonEmpty) {
            if (udfSearch.get.pubic != udf.pubic) {
              if (udf.pubic) {
                Await.result(relProjectUdfDal.deleteByFilter(_.udfId === udf.id), minTimeOut)
              } else {
                val projectIds = StreamUtils.getProjectIdsByUdf(udf.id)
                val projectUdfs = projectIds.map(projectId => RelProjectUdf(0, projectId, udf.id, currentSec, session.userId, currentSec, session.userId))
                Await.result(relProjectUdfDal.insert(projectUdfs), minTimeOut)
              }
            }
          }
          complete(OK, ResponseJson[Udf](getHeader(200, session), updateUdf))
        case Failure(ex) =>
          riderLogger.error(s"user ${
            session.userId
          } update udf failed")
          if (ex.toString.contains("Duplicate entry")) {
            if (ex.toString.contains("function_name_UNIQUE"))
              complete(OK, getHeader(409, s"function_name ${
                udf.functionName
              } already exists", session))
            else
              complete(OK, getHeader(409, s"full_class_name ${
                udf.functionName
              } already exists", session))
          }
          else
            complete(OK, getHeader(451, ex.toString, session))
      }
    }
  }

  private def deleteResponse(id: Long, session: SessionClass): Route = {
    try {
      val result = udfDal.delete(id)
      if (result._1) {
        riderLogger.error(s"user ${session.userId} delete udf $id success.")
        complete(OK, getHeader(200, session))
      }
      else complete(OK, getHeader(412, result._2, session))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user ${session.userId} delete udf $id failed", ex)
        complete(OK, getHeader(451, session))
    }
  }

  private def getNonPublicUdfResponse(session: SessionClass): Route

  = {
    onComplete(udfDal.findByFilter(_.public === false).mapTo[Seq[Udf]]) {
      case Success(udfs) =>
        riderLogger.info(s"user ${
          session.userId
        } find all non public udfs success")
        complete(OK, ResponseSeqJson(getHeader(200, session), udfs.sortBy(_.functionName)))
      case Failure(ex) =>
        riderLogger.error(s"user ${
          session.userId
        } find all non public udfs failed", ex)
        complete(OK, getHeader(451, session))
    }
  }

  def getByProjectIdRoute(route: String): Route = path(route / LongNumber / "udfs") {
    id =>
      get {
        parameter('public.as[Boolean] ?) {
          publicOpt =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "admin") {
                  riderLogger.warn(s"${
                    session.userId
                  } has no permission to access it.")
                  complete(OK, getHeader(403, session))
                }
                else {
                  publicOpt match {
                    case Some(false) =>
                      onComplete(relProjectUdfDal.getNonPublicUdfByProjectId(id).mapTo[Seq[Udf]]) {
                        case Success(udfs) =>
                          riderLogger.info(s"user ${
                            session.userId
                          } select all udfs where project id is $id success.")
                          complete(OK, ResponseSeqJson[Udf](getHeader(200, session), udfs.sortBy(_.functionName)))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${
                            session.userId
                          } select all udfs where project id is $id failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    case Some(true) =>
                      onComplete(udfDal.findByFilter(_.public === true).mapTo[Seq[Udf]]) {
                        case Success(udfs) =>
                          riderLogger.info(s"user ${
                            session.userId
                          } select all udfs where project id is $id success.")
                          complete(OK, ResponseSeqJson[Udf](getHeader(200, session), udfs.sortBy(_.functionName)))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${
                            session.userId
                          } select all udfs where project id is $id failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                    case None =>
                      onComplete(relProjectUdfDal.getUdfByProjectId(id).mapTo[Seq[Udf]]) {
                        case Success(udfs) =>
                          riderLogger.info(s"user ${
                            session.userId
                          } select all udfs where project id is $id success.")
                          complete(OK, ResponseSeqJson[Udf](getHeader(200, session), udfs.sortBy(_.functionName)))
                        case Failure(ex) =>
                          riderLogger.error(s"user ${
                            session.userId
                          } select all udfs where project id is $id failed", ex)
                          complete(OK, getHeader(451, ex.getMessage, session))
                      }
                  }
                }
            }
        }
      }
  }

}
