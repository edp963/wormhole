/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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



package edp.wormhole.util.swifts


case class SwiftsSql(optType: String, // MAP, UNION, FILTER, JOIN... //can add key word "code" for wh3 to reflect
                     fields: Option[String], //fields get from database (join)
                     sql: String, // with constant to replace
                     timeout:Option[Int],
                     lookupNamespace: Option[String], // string after "from" before "=" in sql
                     sourceTableFields: Option[Array[String]], //where () in (@@@) @@@ --> sourceTableFields
                     lookupTableFields: Option[Array[String]], // where (@@@) in ... @@@ --> lookupTableFields
                     lookupTableFieldsAlias: Option[Array[String]]) {// final fileds name get from database ,e.g. select a as b, c from... --> get b,c
}


