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

case class StreamInTopic(id: Long,
                         streamId: Long,
//                         nsInstanceId: Long,
                         nsDatabaseId: Long,
                         partitionOffsets: String,
                         rate: Int,
                         active: Boolean,
                         createTime: String,
                         createBy: Long,
                         updateTime: String,
                         updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class StreamInTopicName(id: Long,
                             streamId: Long,
//                             nsInstanceId: Long,
                             nsDatabaseId: Long,
                             nsDatabase: String,
                             partitions: Int,
                             partitionOffsets: String,
                             rate: Int,
                             active: Boolean,
                             createTime: String,
                             createBy: Long,
                             updateTime: String,
                             updateBy: Long)

case class StreamTopicPartition(streamId: Long, topicName: String, partitions: Option[Int])

class StreamInTopicTable(_tableTag: Tag) extends BaseTable[StreamInTopic](_tableTag, "rel_stream_intopic") {
  def * = (id, streamId, nsDatabaseId, partitionOffsets, rate, active, createTime, createBy, updateTime, updateBy) <> (StreamInTopic.tupled, StreamInTopic.unapply)

  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
//  /** Database column ns_instance_id SqlType(BIGINT) */
//  val nsInstanceId: Rep[Long] = column[Long]("ns_instance_id")
  /** Database column ns_database_id SqlType(BIGINT) */
  val nsDatabaseId: Rep[Long] = column[Long]("ns_database_id")
  /** Database column partition_offsets SqlType(VARCHAR), Length(200,true) */
  val partitionOffsets: Rep[String] = column[String]("partition_offsets", O.Length(200, varying = true))
  /** Database column rate SqlType(INT) */
  val rate: Rep[Int] = column[Int]("rate")

}
