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

package edp.wormhole.sparkx.directive

import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.WormholeConfig
import edp.wormhole.ums.{UmsProtocolType, UmsSchemaUtils}
import org.apache.spark.sql.SparkSession

object UdfWatch extends EdpLogging {

  val udfRelativePath = "/udf"

  def initUdf(config: WormholeConfig, appId: String, session: SparkSession): Unit = {
    logInfo("init udf,appId=" + appId)

    val udfPath = config.zookeeper_path + "/" + config.spark_config.stream_id + udfRelativePath
    if (!WormholeZkClient.checkExist(config.zookeeper_address, udfPath)) WormholeZkClient.createPath(config.zookeeper_address, udfPath)

    WormholeZkClient.setPathChildrenCacheListener(config.zookeeper_address, udfPath, add, remove, update)
  }

  def add(path: String, data: String, time: Long = 1): Unit = {
    try {
      logInfo("add" + data)
      val ums = UmsSchemaUtils.toUms(data)
      ums.protocol.`type` match {
        case UmsProtocolType.DIRECTIVE_UDF_ADD =>
          UdfDirective.addUdfProcess(ums)
        case _ => logWarning("ums type: " + ums.protocol.`type` + " is not supported")
      }
    } catch {
      case e: Throwable => logAlert("udf add error:" + data, e)
    }
  }

  def remove(path: String): Unit = {
    logAlert("do not support udf remove")
  }

  def update(path: String, data: String, time: Long): Unit = {
    logInfo("update" + data)
    add(path, data)
  }

}
