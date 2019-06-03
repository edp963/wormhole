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
import edp.wormhole.util.config.KVConfig
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}

case class Namespace(id: Long,
                     nsSys: String,
                     nsInstance: String,
                     nsDatabase: String,
                     nsTable: String,
                     nsVersion: String,
                     nsDbpar: String,
                     nsTablepar: String,
                     keys: Option[String],
                     sourceSchema: Option[String],
                     sinkSchema: Option[String],
                     nsDatabaseId: Long,
                     nsInstanceId: Long,
                     active: Boolean,
                     createTime: String,
                     createBy: Long,
                     updateTime: String,
                     updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class SourceSchema(umsType: Option[String],
                        jsonSample: Option[Object],
                        jsonParseArray: Option[Object],
                        umsSchemaTable: Option[Object],
                        umsSchema: Option[Object])

case class SinkSchema(jsonSample: Option[Object],
                      jsonParseArray: Option[Object],
                      schemaTable: Option[Object],
                      schema: Option[Object])

case class NsSchema(source: Option[SourceSchema], sink: Option[SinkSchema])

case class NamespaceInfo(id: Long,
                         nsSys: String,
                         nsInstance: String,
                         nsDatabase: String,
                         nsTable: String,
                         nsVersion: String,
                         nsDbpar: String,
                         nsTablepar: String,
                         keys: Option[String],
                         nsDatabaseId: Long,
                         nsInstanceId: Long,
                         active: Boolean,
                         createTime: String,
                         createBy: Long,
                         updateTime: String,
                         updateBy: Long)

case class NamespaceTopic(id: Long,
                          nsSys: String,
                          nsInstance: String,
                          nsDatabase: String,
                          nsTable: String,
                          nsVersion: String,
                          nsDbpar: String,
                          nsTablepar: String,
                          keys: Option[String],
                          nsDatabaseId: Long,
                          nsInstanceId: Long,
                          active: Boolean,
                          createTime: String,
                          createBy: Long,
                          updateTime: String,
                          updateBy: Long,
                          topic: String)

case class NamespaceTemp(id: Long,
                         nsSys: String,
                         nsInstance: String,
                         nsDatabase: String,
                         nsTable: String,
                         nsVersion: String,
                         nsDbpar: String,
                         nsTablepar: String,
                         keys: Option[String],
                         nsDatabaseId: Long,
                         nsInstanceId: Long,
                         active: Boolean,
                         createTime: String,
                         createBy: Long,
                         updateTime: String,
                         updateBy: Long,
                         nsInstanceSys: String)

case class NsTable(table: String,
                   key: Option[String])

case class SimpleNamespace(nsSys: String,
                           nsInstance: String,
                           nsDatabase: String,
                           nsTables: Seq[NsTable],
                           nsDatabaseId: Long,
                           nsInstanceId: Long) extends SimpleBaseEntity

case class NsDatabaseInstance(nsDatabaseId: Long,
                              nsDatabase: String,
                              nsInstanceId: Long,
                              nsInstance: String,
                              nsUrl: String,
                              nsSys: String)

case class TransNamespace(nsSys: String,
                          nsInstance: String,
                          nsDatabase: String,
                          conn_url: String,
                          user: Option[String],
                          pwd: Option[String],
                          connection_config: Option[Seq[KVConfig]])

case class TransNamespaceTemp(instance: Instance, db: NsDatabase, dbConfig: Option[String], nsSys: String)

case class NamespaceAdmin(id: Long,
                          nsSys: String,
                          nsInstance: String,
                          nsDatabase: String,
                          nsTable: String,
                          nsVersion: String,
                          nsDbpar: String,
                          nsTablepar: String,
                          keys: Option[String],
                          nsDatabaseId: Long,
                          nsInstanceId: Long,
                          active: Boolean,
                          createTime: String,
                          createBy: Long,
                          updateTime: String,
                          updateBy: Long,
                          projectName: String,
                          topic: String)

case class NamespaceName(id: Long, namespace: String)

case class NamespaceProjectName(nsId: Long,
                                name: String)

case class PushDownConnection(name_space: String,
                              jdbc_url: String,
                              username: Option[String],
                              password: Option[String],
                              connection_config: Option[Seq[KVConfig]])


class NamespaceTable(_tableTag: Tag) extends BaseTable[Namespace](_tableTag, "namespace") {
  def * = (id, nsSys, nsInstance, nsDatabase, nsTable, nsVersion, nsDbpar, nsTablepar, keys,
    umsInfo, sinkInfo, nsDatabaseId, nsInstanceId, active, createTime, createBy, updateTime, updateBy) <> (Namespace.tupled, Namespace.unapply)

  val nsSys: Rep[String] = column[String]("ns_sys", O.Length(100, varying = true))
  /** Database column ns_instance SqlType(VARCHAR), Length(100,true) */
  val nsInstance: Rep[String] = column[String]("ns_instance", O.Length(100, varying = true))
  /** Database column ns_database SqlType(VARCHAR), Length(100,true) */
  val nsDatabase: Rep[String] = column[String]("ns_database", O.Length(100, varying = true))
  /** Database column ns_table SqlType(VARCHAR), Length(100,true) */
  val nsTable: Rep[String] = column[String]("ns_table", O.Length(100, varying = true))
  /** Database column ns_version SqlType(VARCHAR), Length(20,true) */
  val nsVersion: Rep[String] = column[String]("ns_version", O.Length(20, varying = true))
  /** Database column ns_dbpar SqlType(VARCHAR), Length(100,true) */
  val nsDbpar: Rep[String] = column[String]("ns_dbpar", O.Length(100, varying = true))
  /** Database column ns_tablepar SqlType(VARCHAR), Length(100,true) */
  val nsTablepar: Rep[String] = column[String]("ns_tablepar", O.Length(100, varying = true))
  /** Database column keys SqlType(VARCHAR), Length(1000,true), Default(None) */
  val keys: Rep[Option[String]] = column[Option[String]]("keys", O.Length(1000, varying = true), O.Default(None))
  val umsInfo: Rep[Option[String]] = column[Option[String]]("ums_info")
  val sinkInfo: Rep[Option[String]] = column[Option[String]]("sink_info")
  /** Database column ns_database_id SqlType(BIGINT) */
  val nsDatabaseId: Rep[Long] = column[Long]("ns_database_id")
  /** Database column ns_instance_id SqlType(BIGINT) */
  val nsInstanceId: Rep[Long] = column[Long]("ns_instance_id")

  /** Uniqueness Index over (nsSys,nsInstance,nsDatabase,nsTable,nsVersion,nsDbpar,nsTablepar) (database name namespace_UNIQUE) */
  val index1 = index("namespace_UNIQUE", (nsSys, nsInstance, nsDatabase, nsTable, nsVersion, nsDbpar, nsTablepar), unique = true)
}
