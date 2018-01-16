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

package edp.mad.persistence.entities

import edp.mad.persistence.base.{BaseEntity, BaseTable}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.Tag



case class NamespaceCacheEntity(
                                id: Long,
                                namespace: String,
                                topicName: String,
                                createdTime: String,
                                updatedTime: String ) extends BaseEntity

class NamespaceCacheTable(_tableTag: Tag) extends BaseTable[NamespaceCacheEntity](_tableTag, "namespace_cache") {
  def * = ( id, namespace, topicName, updateTime, createTime) <> (NamespaceCacheEntity.tupled, NamespaceCacheEntity.unapply)

  val namespace: Rep[String] = column[String]("namespace", O.Length(256, varying = true))
  val topicName: Rep[String] = column[String]("topic_name", O.Length(256, varying = true))
  val updateTime: Rep[String] = column[String]("update_time")
  val createTime: Rep[String] = column[String]("create_time")

}
