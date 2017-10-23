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
import slick.lifted.TableQuery
import edp.rider.rest.util.CommonUtils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import slick.jdbc.MySQLProfile.api._

class ProjectDal(projectTable: TableQuery[ProjectTable], relProjectNsDal: RelProjectNsDal, relProjectUserDal: RelProjectUserDal, relProjectUdfDal: RelProjectUdfDal, streamDal: StreamDal) extends BaseDalImpl[ProjectTable, Project](projectTable) with RiderLogger {
  def getById(id: Long): Future[Option[ProjectUserNsUdf]] = {
    val projectOpt = super.findById(id)
    projectOpt.map[Option[ProjectUserNsUdf]] {
      projectOpt => {
        projectOpt match {
          case Some(project) =>
            val userIds = Await.result(relProjectUserDal.getUserIdsByProjectId(id), minTimeOut)
            val nsIds = Await.result(relProjectNsDal.getNsIdsByProjectId(id), minTimeOut)
            val udfIds = Await.result(relProjectUdfDal.getUdfIdsByProjectId(id), minTimeOut)
            Some(ProjectUserNsUdf(project.id, project.name, project.desc, project.pic, project.resCores, project.resMemoryG, project.active,
              project.createTime, project.createBy, project.updateTime, project.updateBy, nsIds, userIds, udfIds))
          case None => None
        }
      }
    }
  }

  def delete(id: Long): (Boolean, String) = {
    try {
      val streamSeq = Await.result(streamDal.findByFilter(_.projectId === id), minTimeOut)
      if (streamSeq.nonEmpty) {
        riderLogger.info(s"project $id still has stream ${streamSeq.map(_.name).mkString(",")}, can't delete it")
        (false, s"please delete stream ${streamSeq.map(_.name).mkString(",")} first")
      } else {
        Await.result(relProjectNsDal.deleteByFilter(_.projectId === id), minTimeOut)
        Await.result(relProjectUserDal.deleteByFilter(_.projectId === id), minTimeOut)
        Await.result(relProjectUdfDal.deleteByFilter(_.projectId === id), minTimeOut)
        Await.result(super.deleteById(id), minTimeOut)
        (true, "success")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete project $id failed", ex)
        throw new Exception(s"delete project $id failed", ex)
    }
  }
}
