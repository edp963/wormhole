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


package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.lifted.Tag
import slick.jdbc.MySQLProfile.api._

case class RelProjectUser(override val id: Long,
                          projectId: Long,
                          userId: Long,
                          active: Boolean,
                          createTime: String,
                          createBy: Long,
                          updateTime: String,
                          updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


case class SimpleRelProjectUser(projectId: Long,
                                userId: Long) extends SimpleBaseEntity

class RelProjectUserTable(_tableTag: Tag) extends BaseTable[RelProjectUser](_tableTag, "rel_project_user") {
  def * = (id, projectId, userId, active, createTime, createBy, updateTime, updateBy) <>(RelProjectUser.tupled, RelProjectUser.unapply)

  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column user_id SqlType(BIGINT) */
  val userId: Rep[Long] = column[Long]("user_id")


  /** Uniqueness Index over (userId,projectId) (database name rel_project_user_UNIQUE) */
  val index1 = index("rel_project_user_UNIQUE", (projectId, userId), unique = true)
}
