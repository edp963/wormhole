///*-
// * <<
// * wormhole
// * ==
// * Copyright (C) 2016 - 2017 EDP
// * ==
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * >>
// */
//
//
//package edp.wormhole.toolkit.datacheck
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//case class DataCheckConfig(`table.name`: String,
//                           `table.keys_cols`: String,
//                           `table.create_ts_col`: String,
//                           `table.update_ts_col`: String,
//                           `table.columns`: String,
//                           `source.type`: String,
//                           `source.namespace`: String,
//                           `source.connection_url`: String,
//                           `sink.type`: String,
//                           `sink.namespace`: String,
//                           `sink.connection_url`: String, //hdfs root
//                           `sink.specific_config`: String,
//                           `compare.start_ts`: String,
//                           `compare.end_ts`: String,
//                           `result.mysql_connection_url`: String,
//                           `process.spark_master`: Option[String],
//                           `process.app_name`: Option[String]
//
//                          ) {
//  //  lazy val `output.hdfs.root.get` = `output.hdfs.root`.getOrElse("hdfs://cluster")
//  lazy val `process.app_name.get` = `process.app_name`.getOrElse("dataCheck_db2parquet")
//  lazy val `process.spark_master.get` = `process.spark_master`.getOrElse("spark://master:7077")
//  val now: Date = new Date()
//  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//  val today: String = dateFormat.format(now)
//  lazy val `today.time` = today
//
//}
