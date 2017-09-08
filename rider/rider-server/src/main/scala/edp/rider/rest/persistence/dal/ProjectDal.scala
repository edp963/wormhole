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

import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import slick.lifted.TableQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ProjectDal(projectTable: TableQuery[ProjectTable], relProjectNsDal: RelProjectNsDal, relProjectUserDal: RelProjectUserDal) extends BaseDalImpl[ProjectTable, Project](projectTable) {
  def getById(id: Long): Future[Option[ProjectUserNs]] = {
    val projectOpt = super.findById(id)
    projectOpt.map[Option[ProjectUserNs]] {
      projectOpt => {
        projectOpt match {
          case Some(project) =>
            val userIds = Await.result(relProjectUserDal.getUserIdsByProjectId(id), 5.second)
            val nsIds = Await.result(relProjectNsDal.getNsIdsByProjectId(id), 5.second)
            Some(ProjectUserNs(project.id, project.name, project.desc, project.pic, project.resCores, project.resMemoryG, project.active,
              project.createTime, project.createBy, project.updateTime, project.updateBy, nsIds, userIds))
          case None => None
        }
      }
    }
  }
}
