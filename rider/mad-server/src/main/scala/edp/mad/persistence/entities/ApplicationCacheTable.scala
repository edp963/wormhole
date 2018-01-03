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

case class ApplicationCacheEntity( id: Long,applicationId: String, streamName: String, updateTime: String, createdTime:String ) extends BaseEntity

class ApplicationCacheTable(_tableTag: Tag) extends BaseTable[ApplicationCacheEntity](_tableTag, "application_cache") {
  def * = ( id,applicationId, streamName, updateTime, createdTime) <> (ApplicationCacheEntity.tupled, ApplicationCacheEntity.unapply)

  val applicationId: Rep[String] = column[String]("application_id", O.Length(256, varying = true))
  val streamName: Rep[String] = column[String]("stream_name", O.Length(256, varying = true))
  val updateTime: Rep[String] = column[String]("update_time")
  val createdTime: Rep[String] = column[String]("create_time")

}
