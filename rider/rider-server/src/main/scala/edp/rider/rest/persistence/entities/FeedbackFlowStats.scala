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

case class FeedbackFlowStats(
                              protocolType: String,
                              dataType: String,
                              umsTs: String,
                              streamId: Long,
                              stats_id: String,
                              sourceNamespace: String,
                              sinkNamespace: String,
                              rddCount: Int,
                              dataUmsTs: Long,
                              rddTs: Long,
                              directiveTs: Long,
                              DataProcessTs: Long,
                              swiftsTs: Long,
                              sinkTs: Long,
                              doneTs: Long)


case class MonitorInfo(
                        statsId: String,
                        umsTs: String,
                        projectId: Long,
                        streamId: Long,
                        streamName: String,
                        flowId: Long,
                        flowNamespace: String,
                        rddCount: Int,
                        throughput: Long,
                        dataGeneratedTs: String,
                        rddTs: String,
                        directiveTs: String,
                        DataProcessTs: String,
                        swiftsTs: String,
                        sinkTs: String,
                        doneTs: String,
                        intervalDataProcessToDataums: Long,
                        intervalDataProcessToRdd: Long,
                        intervalDataProcessToSwifts: Long,
                        intervalDataProcessToSink: Long,
                        intervalDataProcessToDone: Long,
                        intervalDataumsToDone: Long,
                        intervalRddToDone: Long,
                        intervalSwiftsToSink: Long,
                        intervalSinkToDone: Long
                      )

