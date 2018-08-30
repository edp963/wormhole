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

package edp.wormhole.flinkx.util

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import org.apache.log4j.Logger

import scala.collection.mutable

object UmsFlowStartUtils {
  val logger: Logger = Logger.getLogger(UmsFlowStartUtils.getClass)
  val schemaBasePath = "/test/flink/schema/"

  def formatZkPath(namespace: String, streamId: String): String = {
    val namespaceSplit = namespace.split("\\.")
    val namespaceDb = namespaceSplit.slice(0, 3).mkString(".")
    val namespaceTable = namespaceSplit(3)

    schemaBasePath + s"$streamId/$namespaceDb/$namespaceTable"
  }

  def formatZkPathWithVersion(namespace: String, streamId: String, version: String): String = {
    formatZkPath(namespace, streamId) + s"/$version"
  }

  def extractVersion(namespace: String): String = {
    val namespaceSplit = namespace.split("\\.")
    namespaceSplit(4)
  }

  def getMaxVersion(zkAddress: String, zkPath: String): String = {
    val child = WormholeZkClient.getChildren(zkAddress, zkPath)
    if (null != child && child.nonEmpty)
      child.max
    else null
  }

  def extractStreamId(schemas: Seq[UmsField], payloads: UmsTuple): String = {
    UmsFieldType.umsFieldValue(payloads.tuple, schemas, "stream_id").toString
  }

  def extractSourceNamespace(umsFlowStart: Ums): String = {
    umsFlowStart.schema.namespace.toLowerCase
  }

  def extractSwifts(schemas: Seq[UmsField], payloads: UmsTuple): String = {
    val swiftsEncoded = UmsFieldType.umsFieldValue(payloads.tuple, schemas, "swifts")
    if (swiftsEncoded != null && !swiftsEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(swiftsEncoded.toString)) else null
  }

  def extractSinks(schemas: Seq[UmsField], payloads: UmsTuple): String = {
    val sinksStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(payloads.tuple, schemas, "sinks").toString))
    logger.info("sinksStr:" + sinksStr)
    sinksStr
  }

  def extractSinkNamespace(schemas: Seq[UmsField], payloads: UmsTuple): String = {
    UmsFieldType.umsFieldValue(payloads.tuple, schemas, "sink_namespace").toString.toLowerCase
  }

  def extractTimeCharacteristic(swifts: JSONObject): String = {
    if (swifts.containsKey("time_characteristic") && swifts.getString("time_characteristic").nonEmpty)
      swifts.getString("time_characteristic")
    else null
  }

  def extractConsumeProtocol(schemas: Seq[UmsField], payloads: UmsTuple): Map[UmsProtocolType, Boolean] = {
    val consumptionDataStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(payloads.tuple, schemas, "consumption_protocol").toString))
    val consumptionDataMap = mutable.HashMap.empty[UmsProtocolType, Boolean]
    val consumption = JSON.parseObject(consumptionDataStr)
    val initial = consumption.getString(InputDataProtocolBaseType.INITIAL.toString).trim.toLowerCase.toBoolean
    val increment = consumption.getString(InputDataProtocolBaseType.INCREMENT.toString).trim.toLowerCase.toBoolean
    val batch = consumption.getString(InputDataProtocolBaseType.BATCH.toString).trim.toLowerCase.toBoolean
    consumptionDataMap(UmsProtocolType.DATA_INITIAL_DATA) = initial
    consumptionDataMap(UmsProtocolType.DATA_INCREMENT_DATA) = increment
    consumptionDataMap(UmsProtocolType.DATA_BATCH_DATA) = batch
    consumptionDataMap.toMap
  }


  def extractDataType(schemas: Seq[UmsField], payloads: UmsTuple): String = {
    UmsFieldType.umsFieldValue(payloads.tuple, schemas, "data_type").toString.toLowerCase()
  }

}
