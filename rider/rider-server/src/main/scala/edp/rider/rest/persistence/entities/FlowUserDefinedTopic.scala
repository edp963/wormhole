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

case class FlowUserDefinedTopic(id: Long,
                               flowId: Long,
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


class FlowUserDefinedTopicTable(_tableTag: Tag) extends BaseTable[FlowUserDefinedTopic](_tableTag, "rel_flow_userdefined_topic") {
  def * = (id, flowId, topic, partitionOffsets, rate, createTime, createBy, updateTime, updateBy) <> (FlowUserDefinedTopic.tupled, FlowUserDefinedTopic.unapply)

  /** Database column flow_id SqlType(BIGINT) */
  val flowId: Rep[Long] = column[Long]("flow_id")
  /** Database column topic SqlType(VARCHAR) */
  val topic: Rep[String] = column[String]("topic")
  /** Database column partition_offsets SqlType(VARCHAR), Length(200,true) */
  val partitionOffsets: Rep[String] = column[String]("partition_offsets", O.Length(200, varying = true))
  /** Database column rate SqlType(INT) */
  val rate: Rep[Int] = column[Int]("rate")

  val index1 = index("topic_UNIQUE", (flowId, topic), unique = true)

}
