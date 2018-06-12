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
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}


case class NsDatabase(id: Long,
                      nsDatabase: String,
                      desc: Option[String] = None,
                      nsInstanceId: Long,
                      user: Option[String] = None,
                      pwd: Option[String] = None,
                      partitions: Option[Int] = None,
                      config: Option[String] = None,
                      active: Boolean,
                      createTime: String,
                      createBy: Long,
                      updateTime: String,
                      updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }

}

case class SimpleNsDatabase(nsDatabase: String,
                            desc: Option[String] = None,
                            nsInstanceId: Long,
                            user: Option[String] = None,
                            pwd: Option[String] = None,
                            partitions: Option[Int] = None,
                            config: Option[String] = None) extends SimpleBaseEntity


case class DatabaseInstance(id: Long,
                            nsDatabase: String,
                            desc: Option[String] = None,
                            nsInstanceId: Long,
                            user: Option[String] = None,
                            pwd: Option[String] = None,
                            partitions: Option[Int] = None,
                            config: Option[String] = None,
                            nsInstance: String,
                            nsSys: String,
                            connUrl: String,
                            active: Boolean,
                            createTime: String,
                            createBy: Long,
                            updateTime: String,
                            updateBy: Long)

case class DataBaseName(id: Long, nsDatabase: String)


class NsDatabaseTable(_tableTag: Tag) extends BaseTable[NsDatabase](_tableTag, "ns_database") {
  def * = (id, nsDatabase, desc, nsInstanceId, user, pwd, partitions, config, active, createTime, createBy, updateTime, updateBy) <> (NsDatabase.tupled, NsDatabase.unapply)

  /** Database column ns_database SqlType(VARCHAR), Length(200,true) */
  val nsDatabase: Rep[String] = column[String]("ns_database", O.Length(200, varying = true))
  /** Database column desc SqlType(VARCHAR), Length(1000,true), Default(None) */
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column ns_instance_id SqlType(BIGINT) */
  val nsInstanceId: Rep[Long] = column[Long]("ns_instance_id")
  /** Database column user SqlType(VARCHAR), Length(200,true), Default(None) */
  val user: Rep[Option[String]] = column[Option[String]]("user", O.Length(200, varying = true), O.Default(None))
  /** Database column pwd SqlType(VARCHAR), Length(200,true), Default(None) */
  val pwd: Rep[Option[String]] = column[Option[String]]("pwd", O.Length(200, varying = true), O.Default(None))
  /** Database column partitions SqlType(INT), Default(None) */
  val partitions: Rep[Option[Int]] = column[Option[Int]]("partitions", O.Default(None))
  /** Database column config SqlType(VARCHAR), Length(2000,true), Default(None) */
  val config: Rep[Option[String]] = column[Option[String]]("config", O.Length(2000, varying = true), O.Default(None))

  /** Uniqueness Index over (nsDatabase,nsInstanceId) (database name database_UNIQUE) */
  val index1 = index("database_UNIQUE", (nsDatabase, nsInstanceId), unique = true)
}

