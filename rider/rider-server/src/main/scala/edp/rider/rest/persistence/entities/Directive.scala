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

case class Directive(id: Long,
                     protocolType: String,
                     streamId: Long,
                     flowId: Long,
                     directive: String,
                     zkPath: String,
                     createTime: String,
                     createBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class SimpleDirective(protocolType: String,
                           flowId: Long)

class DirectiveTable(_tableTag: Tag) extends BaseTable[Directive](_tableTag, "directive") {
  def * = (id, protocolType, streamId, flowId, directive, zkPath, createTime, createBy) <> (Directive.tupled, Directive.unapply)

  /** Database column protocol_type SqlType(VARCHAR), Length(200,true) */
  val protocolType: Rep[String] = column[String]("protocol_type", O.Length(200, varying = true))
  /** Database column stream_id SqlType(BIGINT) */
  val streamId: Rep[Long] = column[Long]("stream_id")
  /** Database column flow_id SqlType(BIGINT) */
  val flowId: Rep[Long] = column[Long]("flow_id")
  /** Database column directive SqlType(VARCHAR), Length(5000,true) */
  val directive: Rep[String] = column[String]("directive", O.Length(5000, varying = true))
  /** Database column zkPath SqlType(VARCHAR), Length(200,true) */
  val zkPath: Rep[String] = column[String]("zk_path", O.Length(200, varying = true))
}
