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


package edp.wormhole.swifts

import java.lang.reflect.Method

import edp.wormhole.common.{ConnectionConfig, KVConfig}
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.apache.log4j.Logger

import scala.collection.mutable

object SwiftsConfMemoryStorage extends Serializable {
  private val logger: Logger = Logger.getLogger(SwiftsConfMemoryStorage.getClass)
  private val dataStoreConnectionsMap = mutable.HashMap.empty[String, ConnectionConfig]
  private val sinkTransformReflectMap = mutable.HashMap.empty[String, (Any, Method)]

  def getDataStoreConnectionsMap = dataStoreConnectionsMap.toMap

  def registerDataStoreConnectionsMap(lookupNamespace: String, connectionUrl: String, username: Option[String], password: Option[String], parameters: Option[Seq[KVConfig]]) {
    logger.info("register datastore,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    val connectionNamespace = lookupNamespace.split("\\.").slice(0, 3).mkString(".")
    println("connectionNamespace:"+connectionNamespace)
    if (!dataStoreConnectionsMap.contains(connectionNamespace)) {
      dataStoreConnectionsMap(connectionNamespace) = ConnectionConfig(connectionUrl, username, password, parameters)
      logger.info("register datastore success,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    }
  }

  def getDataStoreConnectionsWithMap(dataStoreConnectionsMap: Map[String, ConnectionConfig], namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")
    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

  def getDataStoreConnections(namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")
    for (elem <- dataStoreConnectionsMap.keySet) {

    }
    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

  def getSinkTransformReflect(className: String): (Any, Method) = {
    if (!sinkTransformReflectMap.contains(className)) setSinkTransformReflectMap(className)
    sinkTransformReflectMap(className)
  }

  def setSinkTransformReflectMap(className: String): Unit = {
    synchronized {
      if (!sinkTransformReflectMap.contains(className)) {
        val clazz = Class.forName(className)
        val obj = clazz.newInstance()
        val method = clazz.getMethod("process",
          classOf[UmsProtocolType],
          classOf[String],
          classOf[String],
          classOf[SinkProcessConfig],
          classOf[collection.Map[String, (Int, UmsFieldType, Boolean)]],
          classOf[Seq[Seq[String]]],
          classOf[ConnectionConfig])
        sinkTransformReflectMap(className) = (obj, method)
      }
    }
  }
}
