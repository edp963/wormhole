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


package edp.wormhole.sparkx.batchjob

import edp.wormhole.util.config.ConnectionConfig


case class BatchJobConfig(sourceConfig: SourceConfig,
                          transformationConfig: Option[TransformationConfig],
                          sinkConfig: SinkConfig,
                          jobConfig: JobConfig,
                          udfConfig: Option[List[UdfConfig]])

case class UdfConfig(udfName: String, udfClassFullname: String)

case class SourceConfig(startTime: String,
                        endTime: String,
                        sourceNamespace: String,
                        connectionConfig: ConnectionConfig,
                        classFullName: String,
                        specialConfig: Option[String])

case class TransformationConfig(action: Option[String],
                                specialConfig: Option[String])

case class SinkConfig(sinkNamespace: String,
                      connectionConfig: ConnectionConfig,
                      maxRecordPerPartitionProcessed: Int,
                      classFullName: Option[String],
                      specialConfig: Option[String],
                      tableKeys: Option[String],
                      projection: Option[String])

case class JobConfig(appName: String,
                     master: String,
                     `spark.sql.shuffle.partitions`: Option[Int])

//       `spark.default.parallelism`: Int)

