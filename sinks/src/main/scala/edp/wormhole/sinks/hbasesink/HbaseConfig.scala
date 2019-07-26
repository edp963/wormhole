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


package edp.wormhole.sinks.hbasesink

import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.ums.UmsSysField

case class HbaseConfig(`hbase.columnFamily`: Option[String] = None,
                       `hbase.znParent`: Option[String] = None,
                       `hbase.saveAsString`: Option[Boolean] = None,
                       `umsTs.saveAsString`: Option[Boolean] = None,
                       `hbase.version.column`:Option[String] = None,
                       `mutation_type`:Option[String] = None,
                       `hbase.rowKey`: String //separator","

                      ) {
  lazy val `hbase.columnFamily.get` = `hbase.columnFamily`.getOrElse("cf")
  // lazy val `hbase.znParent.get` = `hbase.znParent`.getOrElse("/hbase")
  lazy val `hbase.valueType.get` = `hbase.saveAsString`.getOrElse(false)
  lazy val `umsTs.valueType.get` = `umsTs.saveAsString`.getOrElse(true)
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  //lazy val `hbase.version.column.get` = `hbase.version.column`.getOrElse(UmsSysField.TS.toString)
}

//case class RowkeyInfo(name: String, pattern: String)


