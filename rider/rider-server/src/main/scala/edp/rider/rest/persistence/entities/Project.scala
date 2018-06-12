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
import slick.lifted.{Rep, Tag}
import slick.jdbc.MySQLProfile.api._


final case class Project(id: Long,
                         name: String,
                         desc: Option[String] = None,
                         pic: Int,
                         resCores: Int,
                         resMemoryG: Int,
                         active: Boolean,
                         createTime: String,
                         createBy: Long,
                         updateTime: String,
                         updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class ProjectUserNsUdf(id: Long,
                            name: String,
                            desc: Option[String] = None,
                            pic: Int,
                            resCores: Int,
                            resMemoryG: Int,
                            active: Boolean,
                            createTime: String,
                            createBy: Long,
                            updateTime: String,
                            updateBy: Long,
                            nsId: String,
                            userId: String,
                            udfId: String)

case class SimpleProjectRel(name: String,
                            desc: Option[String] = None,
                            pic: Int,
                            resCores: Int,
                            resMemoryG: Int,
                            nsId: String,
                            userId: String,
                            udfId: String)

case class SimpleProject(name: String,
                         desc: Option[String] = None,
                         pic: Int,
                         resCores: Int,
                         resMemoryG: Int) extends SimpleBaseEntity

case class AppResource(name: String,
                       driverCores: Int,
                       driverMemory: Int,
                       executorNums: Int,
                       perExecutorMemory: Int,
                       perExecutorCores: Int)

case class Resource(totalCores: Int,
                    totalMemory: Int,
                    remainCores: Int,
                    remainMemory: Int,
                    stream: Seq[AppResource])


class ProjectTable(_tableTag: Tag) extends BaseTable[Project](_tableTag, "project") {
  def * = (id, name, desc, pic, resCores, resMemoryG, active, createTime, createBy, updateTime, updateBy) <> (Project.tupled, Project.unapply)

  val name: Rep[String] = column[String]("name", O.Length(200, varying = true))
  /** Database column desc SqlType(VARCHAR), Length(1000,true), Default(None) */
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column pic SqlType(VARCHAR), Length(200,true) */
  val pic: Rep[Int] = column[Int]("pic")
  /** Database column res_cores SqlType(INT) */
  val resCores: Rep[Int] = column[Int]("res_cores")
  /** Database column res_memory_m SqlType(INT) */
  val resMemoryG: Rep[Int] = column[Int]("res_memory_g")
  /** Uniqueness Index over (name) (database name name_UNIQUE) */
  val index1 = index("name_UNIQUE", name, unique = true)
}
