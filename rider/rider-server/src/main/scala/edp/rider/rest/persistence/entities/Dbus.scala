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

case class Dbus(id: Long,
                dbusId: Long,
                namespace: String,
                kafka: String,
                topic: String,
                instanceId: Long,
                databaseId: Long,
                createTime: String,
                synchronizedTime: String) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


case class SimpleDbus(id: Long,
                      namespace: String,
                      kafka: String,
                      topic: String,
                      createTime: String) extends SimpleBaseEntity

case class DBusNamespaceResponse(status: String,
                                 message: String,
                                 payload: Seq[SimpleDbus])

case class DBusUser(email: String, password: String)

class DbusTable(_tableTag: Tag) extends BaseTable[Dbus](_tableTag, "dbus") {
  def * = (id, dbusId, namespace, kafka, topic, instanceId, databaseId, createTime, synchronizedTime) <> (Dbus.tupled, Dbus.unapply)

  /** Database column dbus_id SqlType(BIGINT) */
  val dbusId: Rep[Long] = column[Long]("dbus_id")
  /** Database column namespace SqlType(VARCHAR), Length(500,true) */
  val namespace: Rep[String] = column[String]("namespace", O.Length(500, varying = true))
  /** Database column kafka SqlType(VARCHAR), Length(500,true) */
  val kafka: Rep[String] = column[String]("kafka", O.Length(500, varying = true))
  /** Database column topic SqlType(VARCHAR), Length(200,true) */
  val topic: Rep[String] = column[String]("topic", O.Length(200, varying = true))
  /** Database column instance_id SqlType(BIGINT) */
  val instanceId: Rep[Long] = column[Long]("instance_id")
  /** Database column database_id SqlType(BIGINT) */
  val databaseId: Rep[Long] = column[Long]("database_id")
  /** Database column synchronized_time SqlType(TIMESTAMP) */
  val synchronizedTime: Rep[String] = column[String]("synchronized_time")
  val index1 = index("dbus_UNIQUE", (dbusId, kafka, instanceId, databaseId), unique = true)
}

