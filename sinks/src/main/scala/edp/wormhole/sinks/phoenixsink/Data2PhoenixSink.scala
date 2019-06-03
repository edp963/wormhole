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


package edp.wormhole.sinks.phoenixsink

import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig

class Data2PhoenixSink extends SinkProcessor {
  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val sinkSpecificConfig: PhoenixConfig = JsonUtils.json2caseClass[PhoenixConfig](sinkProcessConfig.specialConfig.get)
    val phoenixProcess = new PhoenixProcess(sinkNamespace, sinkProcessConfig, schemaMap, sinkSpecificConfig,connectionConfig)
    phoenixProcess.doInsert(tupleList)

  }
}
