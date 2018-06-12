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

case class FeedbackFlowErr(id: Long,
                           protocolType: String,
                           umsTs: String,
                           streamId: Long,
                           sourceNamespace: String,
                           sinkNamespace: String,
                           errorCount: Int,
                           errorMaxWaterMarkTs: String,
                           errorMinWaterMarkTs: String,
                           errorInfo: String,
                           feedbackTime: String) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class SinkError(maxErrorWatermarkTs: String,
                     minErrorWatermarkTs: String,
                     errorCount: Int)

case class StreamSourceSink(streamId: Long, sourceNs: String, sinkNs: String)

class FeedbackFlowErrTable(_tableTag: Tag) extends BaseTable[FeedbackFlowErr](_tableTag, "feedback_flow_error") {
  def * = (id, projectType, umsTs, streamId, sourceNamespace, sinkNamespace, errorCount,
    errorMaxWaterMarkTs, errorMinWaterMarkTs, errorInfo, feedbackTime) <> (FeedbackFlowErr.tupled, FeedbackFlowErr.unapply)

  val projectType: Rep[String] = column[String]("protocol_type", O.Length(200, varying = true))
  val umsTs: Rep[String] = column[String]("ums_ts")
  val streamId: Rep[Long] = column[Long]("stream_id")
  val sourceNamespace: Rep[String] = column[String]("source_namespace")
  val sinkNamespace: Rep[String] = column[String]("sink_namespace")
  val errorCount: Rep[Int] = column[Int]("error_count")
  val errorMaxWaterMarkTs: Rep[String] = column[String]("error_max_watermark_ts")
  val errorMinWaterMarkTs: Rep[String] = column[String]("error_min_watermark_ts")
  val errorInfo: Rep[String] = column[String]("error_info")
  val feedbackTime: Rep[String] = column[String]("feedback_time")

}
