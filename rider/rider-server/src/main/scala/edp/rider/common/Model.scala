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


package edp.rider.common

case class AppResult(appId: String, appName: String, appStatus: String, finalStatus: String, startedTime: String, finishedTime: String)

case class AppInfo(appId: String, appState: String, startedTime: String, finishedTime: String)

case class WorkerInfo(id: String,
                      host: String,
                      port: Int,
                      webUiAddress: String,
                      cores: Int,
                      coresUsed: Int,
                      coresFree: Int,
                      memory: Long,
                      memoryUsed: Long,
                      memoryFree: Long,
                      state: String,
                      lastHeartbeat: Long)

case class ApplicationInfo(startTime: Any,
                           id: String,
                           name: String,
                           cores: Int,
                           user: String,
                           memoryPerSlave: Int,
                           submitDate: Any,
                           state: String,
                           finalState: String,
                           duration: Long)

case class MasterStateResponse(url: String,
                               workers: Array[WorkerInfo],
                               cores: Int,
                               coresUsed: Int,
                               memory: Long,
                               memoryUsed: Long,
                               activeApps: Array[ApplicationInfo],
                               others: Any)

case class TopicPartitionOffset(topicName: String,
                                partitionId: Int,
                                offset: Long)

case class GrafanaConnectionInfo(dashboardUrl: String)
