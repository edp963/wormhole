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

case class CacheEffectEntity( id: Long, streamId: Long, hitCount: Long, latestHitDatetime: String, missedCount:Long, latestMissedDatetime: String) extends BaseEntity

class CacheEffectTable(_tableTag: Tag) extends BaseTable[CacheEffectEntity](_tableTag, "cache_effect") {
  def * = ( id, streamId, hitCount, latestHitDatetime, missedCount, latestMissedDatetime ) <> (CacheEffectEntity.tupled, CacheEffectEntity.unapply)

  val streamId: Rep[Long] = column[Long]("stream_id")
  val hitCount: Rep[Long] = column[Long]("hit_count")
  val latestHitDatetime: Rep[String] = column[String]("latest_hit_time")
  val missedCount: Rep[Long] = column[Long]("missed_count")
  val latestMissedDatetime: Rep[String] = column[String]("latest_missed_time")

  val index1 = index("cache_hit_unique_index", streamId)

}
