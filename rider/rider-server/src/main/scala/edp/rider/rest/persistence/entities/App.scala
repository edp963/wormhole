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


case class AppJob(sourceSys: String,
                  sourceDatabase: String,
                  sourceTable: String,
                  sourceVersion: Option[String],
                  sourceType: Option[String],
                  consumedProtocol: Option[String],
                  sinkSys: String,
                  sinkInstance: String,
                  sinkDatabase: String,
                  sinkTable: String,
                  sinkKeys: String,
                  sinkColumns: String,
                  sinkConfig: String,
                  eventTsStart: String,
                  eventTsEnd: String)

case class AppFlow(sourceSys: String,
                   sourceDatabase: String,
                   sourceTable: String,
                   consumedProtocol: Option[String],
                   sinkSys: String,
                   sinkInstance: String,
                   sinkDatabase: String,
                   sinkTable: String,
                   sinkKeys: String,
                   sinkColumns: String,
                   sinkConfig: String)

case class AppFlowResponse(flowId: Long, status: String)

case class AppJobResponse(jobId: Long, status: String)

case class JobHealth(jobStatus: String, jobApplicationId: String)


