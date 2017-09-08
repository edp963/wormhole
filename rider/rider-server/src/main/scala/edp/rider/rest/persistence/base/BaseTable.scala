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


package edp.rider.rest.persistence.base

import slick.jdbc.H2Profile.api._
import slick.lifted.{Rep, Tag}

abstract class BaseTable[T](tag: Tag, desc: String) extends Table[T](tag, desc) {
  def id = column[Long]("id", O.AutoInc, O.PrimaryKey)

  def active = column[Boolean]("active")

  def createTime: Rep[String] = column[String]("create_time")

  def createBy: Rep[Long] = column[Long]("create_by")

  /** Database column update_time SqlType(TIMESTAMP) */
  def updateTime: Rep[String] = column[String]("update_time")

  /** Database column update_by SqlType(BIGINT) */
  def updateBy: Rep[Long] = column[Long]("update_by")

}

