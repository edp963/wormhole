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

class RelProjectUserDal(userTable: TableQuery[UserTable],
                        projectTable: TableQuery[ProjectTable],
                        relProjectUserTable: TableQuery[RelProjectUserTable]) extends BaseDalImpl[RelProjectUserTable, RelProjectUser](relProjectUserTable) with RiderLogger {

  def getUserIdsByProjectId(id: Long): Future[String] = super.findByFilter(_.projectId === id)
    .map[String] {
    relProjectUserSeq =>
      relProjectUserSeq.map(_.userId).mkString(",")
  }

  def getUserByProjectId(id: Long): Future[Seq[User]] = {
    db.run((userTable.filter(user => user.active === true && user.roleType =!= "admin") join relProjectUserTable.filter(_.projectId === id) on (_.id === _.userId))
      .map {
        case (user, _) => (user.id, user.email, "", user.name, user.roleType, user.preferredLanguage, user.active, user.createTime, user.createBy,
          user.updateTime, user.updateBy) <> (User.tupled, User.unapply)
      }.result).mapTo[Seq[User]]
  }

  def getUserProjectName: Future[mutable.HashMap[Long, ArrayBuffer[String]]] = {
    val userProjectSeq = db.run((projectTable join relProjectUserTable on (_.id === _.projectId))
      .map {
        case (project, rel) => (rel.userId, project.name) <> (UserProjectName.tupled, UserProjectName.unapply)
      }.result).mapTo[Seq[UserProjectName]]
    userProjectSeq.map[mutable.HashMap[Long, ArrayBuffer[String]]] {
      val userProjectMap = mutable.HashMap.empty[Long, ArrayBuffer[String]]
      userProjectSeq =>
        userProjectSeq.foreach(userProject => {
          if (userProjectMap.contains(userProject.userId))
            userProjectMap(userProject.userId) = userProjectMap(userProject.userId) += userProject.name
          else
            userProjectMap(userProject.userId) = ArrayBuffer(userProject.name)
        })
        userProjectMap
    }
  }

  def getProjectByUserId(userId: Long): Future[Seq[Project]] = {
    db.run((projectTable.filter(_.active === true).sortBy(_.name) join relProjectUserTable.filter(rel => rel.userId === userId && rel.active === true) on (_.id === _.projectId))
      .map {
        case (project, rel) => project
      }.result).mapTo[Seq[Project]]
  }

  def isAvailable(projectId: Long, userId: Long): Boolean = {
    try {
      val rel = Await.result(super.findByFilter(rel => rel.userId === userId && rel.projectId === projectId), minTimeOut)
      if (rel.isEmpty) false else true
    } catch {
      case ex: Exception =>
        riderLogger.error(s"check project id $projectId, user id $userId permission failed", ex)
        throw ex
    }
  }
}
