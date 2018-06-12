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

case class User(id: Long,
                email: String,
                password: String,
                name: String,
                roleType: String,
                preferredLanguage: String,
                active: Boolean,
                createTime: String,
                createBy: Long,
                updateTime: String,
                updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


case class SimpleUser(email: String,
                      password: String,
                      name: String,
                      roleType: String) extends SimpleBaseEntity

case class UserProject(id: Long,
                       email: String,
                       password: String,
                       name: String,
                       roleType: String,
                       preferredLanguage: String,
                       active: Boolean,
                       createTime: String,
                       createBy: Long,
                       updateTime: String,
                       updateBy: Long,
                       projectNames: String)

case class UserProjectName(userId: Long,
                           name: String)

class UserTable(_tableTag: Tag) extends BaseTable[User](_tableTag, "user") {
  def * = (id, email, password, name, roleType, preferredLanguage, active, createTime, createBy, updateTime, updateBy) <>(User.tupled, User.unapply)

  val email: Rep[String] = column[String]("email", O.Length(200, varying = true))
  /** Database column password SqlType(VARCHAR), Length(32,true) */
  val password: Rep[String] = column[String]("password", O.Length(32, varying = true))
  /** Database column name SqlType(VARCHAR), Length(200,true) */
  val name: Rep[String] = column[String]("name", O.Length(200, varying = true))
  /** Database column role_type SqlType(VARCHAR), Length(100,true) */
  val roleType: Rep[String] = column[String]("role_type", O.Length(100, varying = true))

  val preferredLanguage: Rep[String] = column[String]("preferred_language", O.Length(20, varying = true))

  /** Uniqueness Index over (email) (database name email_UNIQUE) */
  val index1 = index("email_UNIQUE", email, unique = true)

}
