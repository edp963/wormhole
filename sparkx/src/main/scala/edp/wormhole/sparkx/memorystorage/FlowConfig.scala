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

package edp.wormhole.sparkx.memorystorage

import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sparkxinterface.swifts.SwiftsProcessConfig

//SwiftsProcessConfig, SinkProcessConfig, directiveId, swiftsConfigStr,sinkConfigStr,consumption_data_type,ums/json
//Option[SwiftsProcessConfig], SinkProcessConfig, Long, String, String, Map[String, Boolean]
case class FlowConfig(swiftsProcessConfig: Option[SwiftsProcessConfig],
                      sinkProcessConfig: SinkProcessConfig,
                      directiveId: Long,
                      swiftsConfigStr: String,
                      sinkConfigStr: String,
                      consumptionDataType: Map[String, Boolean],
                      flowId: Long,
                      incrementTopics: List[String],
                      priorityId:Long) {
}
