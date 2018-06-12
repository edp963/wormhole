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
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.monitor.Dashboard
import edp.rider.rest.persistence.dal._
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, ResponseSeqJson, SessionClass}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.ResponseUtils._
import edp.rider.rest.util.{AuthorizationProvider, FlowUtils, StreamUtils}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}

class ProjectAdminApi(projectDal: ProjectDal,
                      relProjectNsDal: RelProjectNsDal,
                      relProjectUserDal: RelProjectUserDal,
                      relProjectUdfDal: RelProjectUdfDal,
                      flowDal: FlowDal) extends BaseAdminApiImpl(projectDal) with RiderLogger with JsonSerializer {

  override def getByIdRoute(route: String): Route = path(route / LongNumber) {
    id =>
      get {
        authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
          session =>
            if (session.roleType != "admin") {
              riderLogger.warn(s"${session.userId} has no permission to access it.")
              complete(OK, getHeader(403, session))
            }
            else {
              onComplete(projectDal.getById(id).mapTo[Option[ProjectUserNsUdf]]) {
                case Success(projectOpt) => projectOpt match {
                  case Some(project) =>
                    riderLogger.info(s"user ${session.userId} select project where id is $id success.")
                    complete(OK, ResponseJson[ProjectUserNsUdf](getHeader(200, session), project))
                  case None =>
                    riderLogger.warn(s"user ${session.userId} select project where id is $id success, but it doesn't exist.")
                    complete(OK, ResponseJson[String](getHeader(200, session), ""))
                }
                case Failure(ex) =>
                  riderLogger.error(s"user ${session.userId} select project where id is $id failed", ex)
                  complete(OK, getHeader(451, ex.getMessage, session))
              }
            }
        }
      }

  }

  def getByFilterRoute(route: String): Route = path(route) {
    get {
      parameter('visible.as[Boolean].?, 'name.as[String].?) {
        (visible, name) =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                (visible, name) match {
                  case (None, Some(projectName)) =>
                    onComplete(projectDal.findByFilter(_.name === projectName).mapTo[Seq[Project]]) {
                      case Success(projects) =>
                        if (projects.isEmpty) {
                          riderLogger.info(s"user ${session.userId} check project name $projectName doesn't exist.")
                          complete(OK, getHeader(200, session))
                        }
                        else {
                          riderLogger.warn(s"user ${session.userId} check project name $projectName already exists.")
                          complete(OK, getHeader(409, s"$projectName project already exists", session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} check project name $projectName does exist failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (_, None) =>
                    onComplete(projectDal.findAll.mapTo[Seq[Project]]) {
                      case Success(projects) =>
                        riderLogger.info(s"user ${session.userId} select all $route success.")
                        complete(OK, ResponseSeqJson[Project](getHeader(200, session), projects.sortBy(project => (project.active, project.createTime)).reverse))
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select all $route failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case (_, _) =>
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
      entity(as[SimpleProjectRel]) {
        simple =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val projectEntity = Project(0, simple.name.trim, simple.desc, simple.pic, simple.resCores, simple.resMemoryG, active = true, currentSec, session.userId, currentSec, session.userId)
                onComplete(projectDal.insert(projectEntity).mapTo[Project]) {
                  case Success(project) =>
                    if (RiderConfig.grafana != null)
                      Dashboard.createDashboard(project.id, simple.name, RiderConfig.es.wormholeIndex)
                    riderLogger.info(s"user ${session.userId} insert project success.")
                    val relNsEntity = simple.nsId.split(",").map(nsId => RelProjectNs(0, project.id, nsId.toLong, active = true, currentSec, session.userId, currentSec, session.userId)).toSeq
                    val relUserEntity = simple.userId.split(",").map(userId => RelProjectUser(0, project.id, userId.toLong, active = true, currentSec, session.userId, currentSec, session.userId)).toSeq
                    val relUdfEntity =
                      if (simple.udfId != "" && simple.udfId != null)
                        simple.udfId.split(",").map(udfId => RelProjectUdf(0, project.id, udfId.toLong, currentSec, session.userId, currentSec, session.userId)).toSeq
                      else null
                    onComplete(relProjectNsDal.insert(relNsEntity).mapTo[Seq[RelProjectNs]]) {
                      case Success(_) =>
                        riderLogger.info(s"user ${session.userId} insert relProjectNs success.")
                        onComplete(relProjectUserDal.insert(relUserEntity).mapTo[Seq[RelProjectUser]]) {
                          case Success(_) =>
                            riderLogger.info(s"user ${session.userId} insert relProjectUser success.")
                            if (relUdfEntity != null) {
                              onComplete(relProjectUdfDal.insert(relUdfEntity).mapTo[Seq[RelProjectUdf]]) {
                                case Success(_) =>
                                  riderLogger.info(s"user ${session.userId} insert relProjectUdf success.")
                                  complete(OK, ResponseJson[Project](getHeader(200, session), project))
                                case Failure(ex) =>
                                  riderLogger.error(s"user ${session.userId} insert relProjectUdf failed", ex)
                                  complete(OK, getHeader(451, ex.getMessage, session))
                              }
                            } else complete(OK, ResponseJson[Project](getHeader(200, session), project))
                          case Failure(ex) =>
                            riderLogger.error(s"user ${session.userId} insert relProjectUdf failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} insert relProjectNs failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} insert project failed", ex)
                    if (ex.toString.contains("Duplicate entry"))
                      complete(OK, getHeader(409, s"${simple.name} project already exists", session))
                    else
                      complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }

  }


  def putRoute(route: String): Route = path(route) {
    put {
      entity(as[ProjectUserNsUdf]) {
        entity =>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "admin") {
                riderLogger.warn(s"${session.userId} has no permission to access it.")
                complete(OK, getHeader(403, session))
              }
              else {
                val projectEntity = Project(entity.id, entity.name.trim, entity.desc, entity.pic, entity.resCores, entity.resMemoryG, entity.active, entity.createTime, entity.createBy, currentSec, session.userId)
                onComplete(projectDal.update(projectEntity).mapTo[Int]) {
                  case Success(_) =>
                    riderLogger.info(s"user ${session.userId} updated project success.")
                    val relNsEntity = entity.nsId.split(",").map(nsId => RelProjectNs(0, entity.id, nsId.toLong, active = true, currentSec, session.userId, currentSec, session.userId)).toSeq
                    val relUserEntity = entity.userId.split(",").map(userId => RelProjectUser(0, entity.id, userId.toLong, active = true, currentSec, session.userId, currentSec, session.userId)).toSeq
                    val relUdfEntity =
                      if (entity.udfId != "")
                        entity.udfId.split(",").map(udfId => RelProjectUdf(0, entity.id, udfId.toLong, currentSec, session.userId, currentSec, session.userId)).toSeq
                      else Seq[RelProjectUdf]()
                    onComplete(relProjectNsDal.findByFilter(_.projectId === entity.id).mapTo[Seq[RelProjectNs]]) {
                      case Success(existRelNsSeq) =>
                        riderLogger.info(s"user ${session.userId} select relProjectNs where project id is ${entity.id} success.")
                        val existRelNsIds = existRelNsSeq.map(_.nsId)
                        val putRelNsIds = relNsEntity.map(_.nsId)
                        val deleteNsIds = existRelNsIds.filter(!putRelNsIds.contains(_))
                        val flowNs = FlowUtils.getFlowsAndJobsByNsIds(projectEntity.id, deleteNsIds, putRelNsIds)
                        val finalDeleteIds = deleteNsIds.filter(!flowNs._3.contains(_))
                        val insertNsSeq = relNsEntity.filter(relNs => !existRelNsIds.contains(relNs.nsId))
                        onComplete(relProjectNsDal.deleteByFilter(relNs => relNs.projectId === entity.id && relNs.nsId.inSet(finalDeleteIds)).mapTo[Int]) {
                          case Success(_) =>
                            riderLogger.info(s"user ${session.userId} delete relProjectNs where project id is ${entity.id} and nsId in $finalDeleteIds success")
                            onComplete(relProjectNsDal.insert(insertNsSeq).mapTo[Seq[RelProjectNs]]) {
                              case Success(_) =>
                                riderLogger.info(s"user ${session.userId} insert relProjectNs success.")
                                onComplete(relProjectUserDal.findByFilter(_.projectId === entity.id).mapTo[Seq[RelProjectUser]]) {
                                  case Success(existRelUserSeq) =>
                                    val existRelUserIds = existRelUserSeq.map(_.userId)
                                    val putRelUserIds = relUserEntity.map(_.userId)
                                    val deleteUserIds = existRelUserIds.filter(!putRelUserIds.contains(_))
                                    val insertUserSeq = relUserEntity.filter(relUser => !existRelUserIds.contains(relUser.userId))
                                    onComplete(relProjectUserDal.deleteByFilter(relUser => relUser.projectId === entity.id && relUser.userId.inSet(deleteUserIds)).mapTo[Int]) {
                                      case Success(_) =>
                                        riderLogger.info(s"user ${session.userId} delete relProjectUser where project id is ${entity.id} and userId in $deleteUserIds success.")
                                        onComplete(relProjectUserDal.insert(insertUserSeq).mapTo[Seq[RelProjectUser]]) {
                                          case Success(_) =>
                                            riderLogger.info(s"user ${session.userId} insert relProjectUser success.")
                                            onComplete(relProjectUdfDal.findByFilter(_.projectId === entity.id).mapTo[Seq[RelProjectUdf]]) {
                                              case Success(existRelUdfSeq) =>
                                                val existRelUdfIds = existRelUdfSeq.map(_.udfId)
                                                val putRelUdfIds = relUdfEntity.map(_.udfId)
                                                val deleteUdfIds = existRelUdfIds.filter(!putRelUdfIds.contains(_))
                                                val streams = StreamUtils.checkAdminRemoveUdfs(projectEntity.id, deleteUdfIds)
                                                val finalDeleteIds = deleteUdfIds.filter(!streams._2.contains(_))
                                                val insertUdfSeq = relUdfEntity.filter(relUdf => !existRelUdfIds.contains(relUdf.udfId))
                                                onComplete(relProjectUdfDal.deleteByFilter(relUdf => relUdf.projectId === entity.id && relUdf.udfId.inSet(finalDeleteIds)).mapTo[Int]) {
                                                  case Success(_) =>
                                                    riderLogger.info(s"user ${session.userId} delete relProjectUdf where project id is ${entity.id} and udfId in $finalDeleteIds success.")
                                                    onComplete(relProjectUdfDal.insert(insertUdfSeq).mapTo[Seq[RelProjectUdf]]) {
                                                      case Success(_) =>
                                                        riderLogger.info(s"user ${session.userId} insert relProjectUdf success.")
                                                        if (flowNs._1.nonEmpty || flowNs._2.nonEmpty || streams._1.nonEmpty) {
                                                          var msg = ""
                                                          if (flowNs._1.nonEmpty)
                                                            flowNs._1.keySet.foreach(flow => msg += s"flow $flow is using namespaces ${flowNs._1(flow).mkString(",")}, ")
                                                          if (flowNs._2.nonEmpty)
                                                            flowNs._2.keySet.foreach(job => msg += s"job $job is using namespaces ${flowNs._2(job).mkString(",")}, ")
                                                          if (streams._1.nonEmpty)
                                                            streams._1.keySet.foreach(stream => msg += s"stream $stream is using udfs ${streams._1(stream).mkString(",")}, ")
                                                          msg += "please stop or delete them first."
                                                          val projectReturn = Await.result(projectDal.getById(projectEntity.id), minTimeOut).get
                                                          complete(OK, ResponseJson[ProjectUserNsUdf](getHeader(406, msg, session), projectReturn))
                                                        } else
                                                          complete(OK, ResponseJson[Project](getHeader(200, session), projectEntity))
                                                      case Failure(ex) =>
                                                        riderLogger.error(s"user ${session.userId} insert relProjectUdf failed", ex)
                                                        complete(OK, getHeader(451, ex.getMessage, session))
                                                    }
                                                  case Failure(ex) =>
                                                    riderLogger.error(s"user ${session.userId} delete relProjectUdf where project id is ${entity.id} and udf in $finalDeleteIds failed", ex)
                                                    complete(OK, getHeader(451, ex.getMessage, session))
                                                }
                                              case Failure(ex) =>
                                                riderLogger.error(s"user ${session.userId} select relProjectUdf where project id is ${entity.id} failed", ex)
                                                complete(OK, getHeader(451, ex.getMessage, session))
                                            }
                                          case Failure(ex) =>
                                            riderLogger.error(s"user ${session.userId} insert relProjectUser failed", ex)
                                            complete(OK, getHeader(451, ex.getMessage, session))
                                        }
                                      case Failure(ex) =>
                                        riderLogger.error(s"user ${session.userId} delete relProjectUser where project id is ${entity.id} and userId in $deleteUserIds failed", ex)
                                        complete(OK, getHeader(451, ex.getMessage, session))
                                    }
                                  case Failure(ex) =>
                                    riderLogger.error(s"user ${session.userId} select relProjectUser where project id is ${entity.id} failed", ex)
                                    complete(OK, getHeader(451, ex.getMessage, session))
                                }
                              case Failure(ex) =>
                                riderLogger.error(s"user ${session.userId} insert relProjectNs failed", ex)
                                complete(OK, getHeader(451, ex.getMessage, session))
                            }
                          //                        }
                          case Failure(ex)
                          =>
                            riderLogger.error(s"user ${session.userId} delete relProjectNs where project id is ${entity.id} and nsId in $finalDeleteIds failed", ex)
                            complete(OK, getHeader(451, ex.getMessage, session))
                        }
                      case Failure(ex) =>
                        riderLogger.error(s"user ${session.userId} select relProjectNs where project id is ${entity.id} failed", ex)
                        complete(OK, getHeader(451, ex.getMessage, session))
                    }
                  case Failure(ex) =>
                    riderLogger.error(s"user ${session.userId} update project failed", ex)
                    complete(OK, getHeader(451, ex.getMessage, session))
                }
              }
          }
      }
    }
  }


  override def deleteRoute(route: String): Route

  = path(route / LongNumber) {
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
                val result = projectDal.delete(id)
                if (result._1)
                  complete(OK, getHeader(200, session))
                else complete(OK, getHeader(412, result._2, session))
              } catch {
                case ex: Exception =>
                  riderLogger.error(s"user ${session.userId} delete project $id success", ex)
                  complete(OK, getHeader(451, session))
              }
            }
        }
      }
  }
}