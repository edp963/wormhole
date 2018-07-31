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

package edp.wormhole


import edp.wormhole.common.util.JsonUtils
import edp.wormhole.ums.{UmsSchema, UmsSchemaUtils}
import edp.wormhole.util.FlinkSchemaUtils.findJsonSchema
import edp.wormhole.util.UmsFlowStartUtils.{extractVersion, formatZkPath}
import edp.wormhole.util.{FlinkSchemaUtils, UmsFlowStartUtils}
import org.apache.log4j.Logger

object WormholeFlinkxStarter extends App {
  val logger: Logger = Logger.getLogger(this.getClass)
  println(args(0) + " --------------wh config")
  println(args(1) + " --------------flow start")
  val config = JsonUtils.json2caseClass[WormholeFlinkxConfig](args(0))
  val umsFlowStart = UmsSchemaUtils.toUms(args(1))
  FlinkSchemaUtils.setSourceSchemaMap(getJsonSchema)
  new WormholeFlinkMainProcess(config, umsFlowStart).process()

  private def getJsonSchema: UmsSchema = {
    val zkAddress: String = config.zookeeper_address
    val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
    val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head)
    val zkPath: String = formatZkPath(sourceNamespace, streamId)
    val version = extractVersion(sourceNamespace)
    val zkPathWithVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), version)
    findJsonSchema(config, zkAddress, zkPathWithVersion, sourceNamespace)
    //    WormholeZkClient.createPath(zkAddress, zkPath)
    //    val zkPathWithVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), version)
    //    if (!WormholeZkClient.checkExist(zkAddress, zkPathWithVersion)) {
    //      val maxVersion = UmsFlowStartUtils.getMaxVersion(zkAddress, zkPath)
    //      if (null != maxVersion) {
    //        logger.info("maxVersion is not null")
    //        val zkPathWithMaxVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), maxVersion)
    //        getSchemaFromZk(zkAddress, zkPathWithMaxVersion)
    //      } else findJsonSchema(config, zkAddress, zkPathWithVersion, sourceNamespace)
    //    } else {
    //      getSchemaFromZk(zkAddress, zkPathWithVersion)
    //    }
  }


}
