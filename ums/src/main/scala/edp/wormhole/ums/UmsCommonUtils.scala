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


package edp.wormhole.ums

import java.sql.Timestamp

import edp.wormhole.util.DateUtils.dt2timestamp
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.UmsSchemaUtils.toUms
import edp.wormhole.util.CommonUtils
import org.slf4j.LoggerFactory

object UmsCommonUtils extends Serializable {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def keys2keyList(keys: String): List[String] = if (keys == null) Nil else keys.split(",").map(CommonUtils.trimBothBlank).toList

  def getTypeNamespaceFromKafkaKey(key: String): (UmsProtocolType, String) = {
    try {
      if (null == key) {
        logger.info("in getTypeNamespaceFromKafkaKey ")
        println(UmsProtocolType.DATA_INCREMENT_HEARTBEAT)
        println("")
        (UmsProtocolType.DATA_INCREMENT_HEARTBEAT, "")
      } else {
        val keys = key.split("\\.")
        if (keys.length > 7) (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), keys.slice(1, 8).mkString(".").toLowerCase)
        else (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), "")
      }
    } catch {
      case e: Throwable => logger.error("in getTypeNamespaceFromKafkaKey ", e)
        (UmsProtocolType.DATA_INCREMENT_HEARTBEAT, "")
    }
  }

  def json2Ums(json: String): Ums = {
    try {
      toUms(json)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        println("no data in json2ums") //todo add logging here
        Ums(UmsProtocol(UmsProtocolType.FEEDBACK_FLOW_START_DIRECTIVE), UmsSchema("defaultNamespace"), None)
    }
  }


  def dataTypeProcess(dataType: String): String = {
    //    var result=dataType
    val typeArr: Array[String] = dataType.split("")
    val arrLen = typeArr.length
    if (typeArr.slice(arrLen - 5, arrLen).mkString("") == "array" && dataType != "jsonarray") "simplearray"
    else dataType
  }

  def convertLongTimestamp(timestampStr: String): Timestamp = {
    if (timestampStr.substring(0, 2) == "20") {
      dt2timestamp(timestampStr)
    }
    else {
      val timestampLong = (timestampStr + "000000").substring(0, 16).toLong

      dt2timestamp(timestampLong)
    }
  }


  def getFieldContentFromJson(json: String, fieldName: String): String = {
    var tmpValue = json
    var realKey = null.asInstanceOf[String]
    while (tmpValue != null) {
      val namespacePosition = tmpValue.indexOf("\"" + fieldName + "\"")
      tmpValue = tmpValue.substring(namespacePosition + fieldName.length + 2).trim
      if (tmpValue.startsWith(":")) {
        val from = tmpValue.indexOf("\"")
        val to = tmpValue.indexOf("\"", from + 1)
        realKey = tmpValue.substring(from + 1, to)
        tmpValue = null
      }
    }
    realKey
  }

  def getProtocolTypeFromUms(ums: String): String = {
    var tmpValue = ums
    var realKey = null.asInstanceOf[String]
    while (tmpValue != null) {
      val strPosition = tmpValue.indexOf("\"protocol\"")
      if (strPosition > 0) {
        tmpValue = tmpValue.substring(strPosition + 10).trim
        if (tmpValue.startsWith(":")) {
          val from = tmpValue.indexOf("{")
          val to = tmpValue.indexOf("}")
          val subStr = tmpValue.substring(from, to + 1)
          if (subStr.contains("\"type\"")) {
            realKey = getFieldContentFromJson(subStr, "type")
            tmpValue = null
          }
        }
      } else tmpValue = null
    }
    realKey
  }


  def checkAndGetKey(key: String, umsStr: String): String = {
    if (key == null || key.trim.isEmpty) {
      val protocolType = getProtocolTypeFromUms(umsStr)
      val namespace = getFieldContentFromJson(umsStr, "namespace")
      if (protocolType == null)
        UmsProtocolType.DATA_INCREMENT_DATA.toString + "." + getFieldContentFromJson(umsStr, "namespace")
      else protocolType + "." + namespace
    } else key
  }

  def checkAndGetProtocolNamespace(key: String, umsStr: String): (String,String) = {
    if (key == null || key.trim.isEmpty) {
      val protocolType = getProtocolTypeFromUms(umsStr)
      val namespace = getFieldContentFromJson(umsStr, "namespace")
      if (protocolType == null)
        (UmsProtocolType.DATA_INCREMENT_DATA.toString , getFieldContentFromJson(umsStr, "namespace"))
      else (protocolType , namespace)
    } else {
      val typeNamespace = UmsCommonUtils.getTypeNamespaceFromKafkaKey(key)
      (typeNamespace._1.toString,typeNamespace._2)
    }
  }
}
