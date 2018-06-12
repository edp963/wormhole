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

import edp.rider.module.DbModule._
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.lifted.{CanBeQueryCondition, TableQuery}
import slick.jdbc.MySQLProfile.api._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UserDal(userTable: TableQuery[UserTable], relProjectUserDal: RelProjectUserDal, projectDal: ProjectDal) extends BaseDalImpl[UserTable, User](userTable) with RiderLogger {

  def getUserProject[C: CanBeQueryCondition](f: (UserTable) => C): Future[Seq[UserProject]] = {
    try {
      val userProjectMap = Await.result(relProjectUserDal.getUserProjectName, minTimeOut)
      val users = super.findByFilter(f)
      users.map[Seq[UserProject]] {
        val userProjectSeq = new ArrayBuffer[UserProject]
        users =>
          users.foreach(
            user =>
              if (userProjectMap.contains(user.id))
                userProjectSeq += UserProject(user.id, user.email, "", user.name, user.roleType, user.preferredLanguage, user.active,
                  user.createTime, user.createBy, user.updateTime, user.updateBy, userProjectMap(user.id).sorted.mkString(","))
              else
                userProjectSeq += UserProject(user.id, user.email, "", user.name, user.roleType, user.preferredLanguage, user.active,
                  user.createTime, user.createBy, user.updateTime, user.updateBy, "")
          )
          userProjectSeq
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"admin refresh users failed", ex)
        throw ex
    }
  }

  def delete(id: Long): (Boolean, String) = {
    try {
      val rel = Await.result(relProjectUserDal.findByFilter(_.userId === id), minTimeOut)
      val projects = Await.result(projectDal.findByFilter(_.id inSet rel.map(_.projectId)), minTimeOut).map(_.name)
      if (projects.nonEmpty) {
        riderLogger.info(s"user $id still has project ${projects.mkString(",")}, can't delete it")
        (false, s"please revoke project ${projects.mkString(",")} and user binding relation first")
      } else {
        Await.result(super.deleteById(id), minTimeOut)
        (true, "success")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete user $id failed", ex)
        throw new Exception(s"delete user $id failed", ex)
    }
  }

  def updateWithoutPwd(user: User): Future[Int] = {
    db.run(userTable.filter(_.id === user.id).map(user => (user.name, user.preferredLanguage, user.roleType, user.updateTime, user.updateBy))
      .update((user.name, user.preferredLanguage, user.roleType, user.updateTime, user.updateBy)))
  }
}
