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

case class Instance(id: Long,
                    nsInstance: String,
                    desc: Option[String] = None,
                    nsSys: String,
                    connUrl: String,
                    connConfig: Option[String] = None,
                    active: Boolean,
                    createTime: String,
                    createBy: Long,
                    updateTime: String,
                    updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


case class SimpleInstance(desc: Option[String] = None,
                          nsSys: String,
                          nsInstance: String,
                          connUrl: String,
                          connConfig: Option[String] = None) extends SimpleBaseEntity

case class InstanceName(id: Long, nsInstance: String)

class InstanceTable(_tableTag: Tag) extends BaseTable[Instance](_tableTag, "instance") {
  def * = (id, nsInstance, desc, nsSys, connUrl, connConfig, active, createTime, createBy, updateTime, updateBy) <> (Instance.tupled, Instance.unapply)

  /** Database column ns_instance SqlType(VARCHAR), Length(200,true) */
  val nsInstance: Rep[String] = column[String]("ns_instance", O.Length(200, varying = true))
  /** Database column desc SqlType(VARCHAR), Length(1000,true), Default(None) */
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column ns_sys SqlType(VARCHAR), Length(30,true) */
  val nsSys: Rep[String] = column[String]("ns_sys", O.Length(30, varying = true))
  /** Database column conn_url SqlType(VARCHAR), Length(1000,true) */
  val connUrl: Rep[String] = column[String]("conn_url", O.Length(200, varying = true))
  /** Database column conn_config SqlType(VARCHAR), Length(1000,true) */
  val connConfig: Rep[Option[String]] = column[Option[String]]("conn_config", O.Length(1000, varying = true), O.Default(None))
  /** Uniqueness Index over (nsInstance,nsSys) (database name instance_UNIQUE) */
  val index1 = index("instance_UNIQUE", (nsInstance, nsSys), unique = true)
}
