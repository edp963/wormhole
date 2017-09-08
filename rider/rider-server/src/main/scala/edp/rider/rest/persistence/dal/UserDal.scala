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
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.lifted.{CanBeQueryCondition, TableQuery}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UserDal(userTable: TableQuery[UserTable], relProjectUserDal: RelProjectUserDal) extends BaseDalImpl[UserTable, User](userTable) with RiderLogger {

  def getUserProject[C: CanBeQueryCondition](f: (UserTable) => C): Future[Seq[UserProject]] = {
    try {
      val userProjectMap = Await.result(relProjectUserDal.getUserProjectName, minTimeOut)
      val users = super.findByFilter(f)
      users.map[Seq[UserProject]] {
        val userProjectSeq = new ArrayBuffer[UserProject]
        users => users.foreach(
          user =>
            if (userProjectMap.contains(user.id))
              userProjectSeq += UserProject(user.id, user.email, user.password, user.name, user.roleType, user.active,
                user.createTime, user.createBy, user.updateTime, user.updateBy, userProjectMap(user.id).sorted.mkString(","))
            else
              userProjectSeq += UserProject(user.id, user.email, user.password, user.name, user.roleType, user.active,
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
}
