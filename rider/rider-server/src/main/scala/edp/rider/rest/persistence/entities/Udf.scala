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

case class Udf(id: Long,
               functionName: String,
               fullClassName: String,
               jarName: String,
               desc: Option[String],
               pubic: Boolean,
               streamType: String,
               mapOrAgg: String,
               createTime: String,
               createBy: Long,
               updateTime: String,
               updateBy: Long) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}


case class SimpleUdf(functionName: String,
                     fullClassName: String,
                     jarName: String,
                     desc: Option[String],
                     public: Boolean,
                     streamType: String,
                     mapOrAgg: String) extends SimpleBaseEntity

case class UdfProjectName(udfId: Long,
                          name: String)

case class UdfProject(id: Long,
                      functionName: String,
                      fullClassName: String,
                      jarName: String,
                      desc: Option[String],
                      pubic: Boolean,
                      streamType: String,
                      mapOrAgg: String,
                      createTime: String,
                      createBy: Long,
                      updateTime: String,
                      updateBy: Long,
                      projectNames: String)

class UdfTable(_tableTag: Tag) extends BaseTable[Udf](_tableTag, "udf") {
  def * = (id, functionName, fullClassName, jarName, desc, public, streamType, mapOrAgg, createTime, createBy, updateTime, updateBy) <> (Udf.tupled, Udf.unapply)

  val functionName: Rep[String] = column[String]("function_name", O.Length(200, varying = true))
  val fullClassName: Rep[String] = column[String]("full_class_name", O.Length(200, varying = true))
  val jarName: Rep[String] = column[String]("jar_name", O.Length(200, varying = true))
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(200, varying = true), O.Default(None))
  val public: Rep[Boolean] = column[Boolean]("public")
  val streamType: Rep[String] =  column[String]("stream_type", O.Length(100, varying = true))
  val mapOrAgg: Rep[String] =  column[String]("map_or_agg", O.Length(100, varying = true))
  val index1 = index("functionName_UNIQUE", functionName, unique = true)
  val index2 = index("fullClassName_UNIQUE", fullClassName, unique = true)
}

