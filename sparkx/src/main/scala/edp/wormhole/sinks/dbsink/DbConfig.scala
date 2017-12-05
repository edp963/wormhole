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


package edp.wormhole.sinks.dbsink

import edp.wormhole.common.KVConfig
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.sinks.utils.SinkDefault._

case class DbConfig(`mutation_type`: Option[String],
                    `db.sql_batch_size`: Option[Int],
                    `db.partition_keys`: Option[String],
                    //                     `db.connection_user`: String,
                    //                     `db.connection_password`: String,
                    `db.function_table`: Option[String]
                   ) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val `db.sql_batch_size.get` = `db.sql_batch_size`.getOrElse(100)
  lazy val partitionKeyList = keys2keyList(`db.partition_keys`.orNull)
  lazy val edpTable = `db.function_table`.getOrElse("edp")
}
