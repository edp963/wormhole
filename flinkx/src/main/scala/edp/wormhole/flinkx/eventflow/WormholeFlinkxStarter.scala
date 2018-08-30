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

package edp.wormhole.flinkx.eventflow

import edp.wormhole.flinkx.common.WormholeFlinkxConfig
import edp.wormhole.ums.{Ums, UmsSchemaUtils}
import edp.wormhole.util.JsonUtils
//import edp.wormhole.util.FlinkSchemaUtils.{findJsonSchema}
//import edp.wormhole.util.UmsFlowStartUtils.{extractVersion, formatZkPath}
//import edp.wormhole.util.{FlinkSchemaUtils, UmsFlowStartUtils}
import org.apache.log4j.Logger

object WormholeFlinkxStarter extends App {
  val logger: Logger = Logger.getLogger(this.getClass)
  println(args(0) + " --------------wh config")
  println(args(1) + " --------------flow start")
  val config: WormholeFlinkxConfig = JsonUtils.json2caseClass[WormholeFlinkxConfig](args(0))
  val umsFlowStart: Ums = UmsSchemaUtils.toUms(args(1))
  //println(" --------------start initFlow")
  val dataType = WormholeFlinkxFlowDirective.initFlow(umsFlowStart,config)
  //println( dataType + " --------------start process")
  new WormholeFlinkMainProcess(config, umsFlowStart).process()
  //println( dataType + " --------------end process")
}
