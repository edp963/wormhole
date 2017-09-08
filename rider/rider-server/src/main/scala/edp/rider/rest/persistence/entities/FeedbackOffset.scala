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
import slick.jdbc.MySQLProfile.api._
import slick.lifted.Tag

case class FeedbackOffset(id: Long,
                          protocolType: String,
                          umsTs: String,
                          streamId: Long,
                          topicName: String,
                          partitionNum: Int,
                          partitionOffset: Long,
                          feedbackTime: String) extends BaseEntity

class FeedbackOffsetTable(_tableTag: Tag) extends BaseTable[FeedbackOffset](_tableTag, "feedback_stream_offset") {
  def * = (id, projectType, umsTs, streamId, topicName, partitionNum, partitionOffset, feedbackTime) <> (FeedbackOffset.tupled, FeedbackOffset.unapply)

  val projectType: Rep[String] = column[String]("protocol_type", O.Length(200, varying = true))
  val umsTs: Rep[String] = column[String]("ums_ts")
  val streamId: Rep[Long] = column[Long]("stream_id")
  val topicName: Rep[String] = column[String]("topic_name", O.Length(5000, varying = true))
  val partitionNum: Rep[Int] = column[Int]("partition_num")
  val partitionOffset: Rep[Long] = column[Long]("partition_offset")
  val feedbackTime: Rep[String] = column[String]("feedback_time")
}
