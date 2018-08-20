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



package edp.wormhole.swifts

object SqlOptType extends Enumeration {
  type SqlOptType = Value

  val PUSHDOWN_SQL = Value("pushdown_sql")
  val FLINK_SQL = Value("flink_sql")
  val PARQUET_SQL = Value("parquet_sql")
  val CUSTOM_CLASS = Value("custom_class")
  val SPARK_SQL = Value("spark_sql")
  val CEP = Value("cep")

//  val PUSHDOWN_HBASE = Value("pushdown_hbase")
//  val PUSHDOWN_REDIS = Value("pushdown_redis")

  val UNION = Value("union")
  val INNER_JOIN = Value("inner join")
  val JOIN = Value("join")
  val LEFT_JOIN = Value("left join")
  val RIGHT_JOIN = Value("right join")

  def toSqlOptType(s: String) = SqlOptType.withName(s.toLowerCase)

}
