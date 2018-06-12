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

case class RelStreamUdf(id: Long,
                        streamId: Long,
                        udfId: Long,
                        createTime: String,
                        createBy: Long,
                        updateTime: String,
                        updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class SimpleStreamUdf(streamId: Long,
                           udfId: Long) extends SimpleBaseEntity


class RelStreamUdfTable(_tableTag: Tag) extends BaseTable[RelStreamUdf](_tableTag, "rel_stream_udf") {
  def * = (id, streamId, udfId, createTime, createBy, updateTime, updateBy) <> (RelStreamUdf.tupled, RelStreamUdf.unapply)

  val streamId: Rep[Long] = column[Long]("stream_id")
  val udfId: Rep[Long] = column[Long]("udf_id")
  /** Uniqueness Index over (nsId,projectId) (database name relProject_UNIQUE) */
  val index1 = index("rel_stream_udf_UNIQUE", (streamId, udfId), unique = true)
}
