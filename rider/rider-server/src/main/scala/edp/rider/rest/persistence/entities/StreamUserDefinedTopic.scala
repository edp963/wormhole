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

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable}
import slick.lifted.{Rep, Tag}
import slick.jdbc.MySQLProfile.api._

case class StreamUserDefinedTopic(id: Long,
                                  streamId: Long,
                                  topic: String,
                                  partitionOffsets: String,
                                  rate: Int,
                                  createTime: String,
                                  createBy: Long,
                                  updateTime: String,
                                  updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class PostUserDefinedTopic(name: String)

class StreamUserDefinedTopicTable(_tableTag: Tag) extends BaseTable[StreamUserDefinedTopic](_tableTag, "rel_stream_userdefined_topic") {
  def * = (id, streamId, topic, partitionOffsets, rate, createTime, createBy, updateTime, updateBy) <> (StreamUserDefinedTopic.tupled, StreamUserDefinedTopic.unapply)

  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
  /** Database column topic SqlType(VARCHAR) */
  val topic: Rep[String] = column[String]("topic")
  /** Database column partition_offsets SqlType(VARCHAR), Length(200,true) */
  val partitionOffsets: Rep[String] = column[String]("partition_offsets", O.Length(200, varying = true))
  /** Database column rate SqlType(INT) */
  val rate: Rep[Int] = column[Int]("rate")

  val index1 = index("topic_UNIQUE", (streamId, topic), unique = true)

}
