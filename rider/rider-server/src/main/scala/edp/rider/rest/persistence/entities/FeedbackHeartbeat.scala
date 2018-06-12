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

case class FeedbackHeartbeat(id: Long,
                             protocolType: String,
                             streamId: Long,
                             namespace: String,
                             umsTs: String,
                             feedbackTime: String) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class StreamNamespace(streamId: Long, ns: String)

class FeedbackHeartbeatTable(_tableTag: Tag) extends BaseTable[FeedbackHeartbeat](_tableTag, "feedback_heartbeat") {
  def * = (id, projectType, streamId, namespace, umsTs, feedbackTime) <> (FeedbackHeartbeat.tupled, FeedbackHeartbeat.unapply)

  val projectType: Rep[String] = column[String]("protocol_type", O.Length(200, varying = true))
  val streamId: Rep[Long] = column[Long]("stream_id")
  val namespace: Rep[String] = column[String]("namespace", O.Length(1000, varying = true))
  val umsTs: Rep[String] = column[String]("ums_ts")
  val feedbackTime: Rep[String] = column[String]("feedback_time")

}
