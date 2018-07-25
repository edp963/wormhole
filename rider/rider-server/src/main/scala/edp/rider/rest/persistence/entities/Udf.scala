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
import slick.lifted.{Rep, Tag}
import slick.jdbc.MySQLProfile.api._

case class FlowUdf(id: Long,
                   flowId: Long,
                   udfId: Long,
                   createTime: String,
                   createBy: Long,
                   updateTime: String,
                   updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}



class FlowUdfTable(_tableTag: Tag) extends BaseTable[FlowUdf](_tableTag, "rel_flow_udf") {
  def * = (id, flowId, udfId, createTime, createBy, updateTime, updateBy) <> (FlowUdf.tupled, FlowUdf.unapply)
  /** Database column flow_id SqlType(BIGINT) */
  val flowId: Rep[Long] = column[Long]("flow_id")
  /** Database column udf_id SqlType(BIGINT) */
  val udfId: Rep[Long] = column[Long]("udf_id")

  val index1 = index("flowUdf_UNIQUE", (flowId, udfId), unique = true)

}

