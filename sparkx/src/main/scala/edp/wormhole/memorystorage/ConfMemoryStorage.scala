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


package edp.wormhole.memorystorage

import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.swifts.parse.SwiftsProcessConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import java.lang.reflect.Method

import edp.wormhole.common.{ConnectionConfig, FieldInfo, KVConfig, UmsSysRename}
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.sinks.utils.SinkCommonUtils.firstTimeAfterSecond
import edp.wormhole.ums.UmsField
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ConfMemoryStorage extends Serializable with EdpLogging {

  //[connectionNamespace(Namespace 3 fields),(connectionUrl,username,password,kvconfig)]
  val dataStoreConnectionsMap = mutable.HashMap.empty[String, ConnectionConfig]

  //[lookupNamespace,Seq[sourceNamespace,sinkNamespace]
  val lookup2SourceSinkNamespaceMap = mutable.HashMap.empty[String, mutable.HashSet[(String, String)]]

  //[sourceNamespace, [sinkNamespace, (SwiftsProcessConfig, SinkProcessConfig, directiveId, swiftsConfigStr,sinkConfigStr,consumption_data_type,ums/json)]]
  val flowConfigMap = mutable.HashMap.empty[String, mutable.LinkedHashMap[String, (Option[SwiftsProcessConfig], SinkProcessConfig, Long, String, String, Map[String, Boolean])]]

  //sourceNamespace,sinkNamespace,minTs
  val eventTsMap = mutable.HashMap.empty[(String, String), String]

  val JsonSourceParseMap = mutable.HashMap.empty[(UmsProtocolType, String), (Seq[UmsField],  Seq[FieldInfo],  ArrayBuffer[(String, String)], UmsSysRename)]

  //[className, (object, method)]
  private val swiftsTransformReflectMap = mutable.HashMap.empty[String, (Any, Method)]

  //[className, (object, method)]
  private val sinkTransformReflectMap = mutable.HashMap.empty[String, (Any, Method)]

  def existJsonSourceParseMap(protocol: UmsProtocolType, namespace: String) = {
    JsonSourceParseMap.contains((protocol, namespace))
  }

  def getJsonUmsFieldsName(protocol: UmsProtocolType, namespace: String): UmsSysRename = {
    if (JsonSourceParseMap.contains((protocol, namespace)))
      JsonSourceParseMap((protocol, namespace))._4
    else
      throw new Exception("get Json Source Ts Name failed")
  }

  def matchNameSpace(namespace1: String, namespace2: String): Boolean = {
    if (flowConfigMap.contains(namespace2)) {
      return true
    }
    val namespaceArray1 = namespace1.split("\\.")
    val namespaceArray2 = namespace2.split("\\.")
    namespaceArray1(0) == namespaceArray2(0) && namespaceArray1(1) == namespaceArray2(1) && namespaceArray1(2) == namespaceArray2(2) && namespaceArray1(3) == namespaceArray2(3)
  }

  def registerJsonSourceParseMap(protocolType: UmsProtocolType, namespace: String, umsField: Seq[UmsField], fieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)], umsSysRename:UmsSysRename) = {
    JsonSourceParseMap((protocolType, namespace)) = (umsField, fieldsInfo,twoFieldsArr,umsSysRename)
  }

  def getMatchSourceNamespaceRule(namespace: String): String = {
    var result: String = null
    flowConfigMap.foreach(k => {
      if (matchNameSpace(k._1, namespace)) {
        result = k._1
      }
    })
    result
  }

  def existStreamLookup(matchSourceNamespace: String, sinkNamespace: String): Boolean = {
    lookup2SourceSinkNamespaceMap.exists(source2sink => {
      source2sink._2.exists(row => row._1 == matchSourceNamespace && row._2 == sinkNamespace)
    })
  }

  def existStreamLookup(matchSourceNamespace: String, sinkNamespace: String, lookupNamespace: String): Boolean = {
    lookup2SourceSinkNamespaceMap.contains(lookupNamespace) && lookup2SourceSinkNamespaceMap(lookupNamespace).exists(row => row._1 == matchSourceNamespace && row._2 == sinkNamespace)
  }

  def getMatchLookupNamespaceRule(namespace: String): String = {
    var result: String = null
    lookup2SourceSinkNamespaceMap.foreach(k => {
      if (matchNameSpace(k._1, namespace)) result = k._1
    })
    result
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
        val method = clazz.getDeclaredMethod("process",
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

  def getStreamLookupNamespaceAndTimeout(matchSourceNamespace: String, sinkNamespace: String): Seq[(String, Int)] = {
    val lookupNamespace2TimeoutList = ListBuffer.empty[(String, Int)]
    if (flowConfigMap.contains(matchSourceNamespace) && flowConfigMap(matchSourceNamespace).contains(sinkNamespace)) {
      val flowConfig = flowConfigMap(matchSourceNamespace)(sinkNamespace)._1
      if (flowConfig.nonEmpty && flowConfig.get.swiftsSql.nonEmpty) {
        val swiftsSql = flowConfig.get.swiftsSql.get
        swiftsSql.foreach(sqlConfig => {
          if (sqlConfig.lookupNamespace.nonEmpty && sqlConfig.timeout.nonEmpty) lookupNamespace2TimeoutList += ((sqlConfig.lookupNamespace.get, sqlConfig.timeout.get))
        })
      }
    }
    lookupNamespace2TimeoutList
  }

  def cleanDataStorage(sourceNamespace: String, sinkNamespace: String): Unit = {
    cleanFlowConfig(sourceNamespace, sinkNamespace)
    cleanStreamLookup(sourceNamespace, sinkNamespace)
  }

  def cleanFlowConfig(sourceNamespace: String, sinkNamespace: String): Unit = {
    if (flowConfigMap.contains(sourceNamespace)) {
      if (!flowConfigMap(sourceNamespace).contains(sinkNamespace)) {
        logWarning("cancelFlowConfigMapLogic  from " + sourceNamespace + " to " + sinkNamespace + " failed, check stop flow directive namespace.Sinknamespace does not exist, or already canceled.")
      } else {
        flowConfigMap(sourceNamespace).remove(sinkNamespace)
        logInfo("flowConfigMapLogic canceled successfully, sourceNamespace: " + sourceNamespace + " sinkNamespace :" + sinkNamespace)
      }
      if (flowConfigMap(sourceNamespace).isEmpty) flowConfigMap.remove(sourceNamespace)
    } else {
      logWarning("flowConfigMapLogic: cancel flow from " + sourceNamespace + " to " + sinkNamespace + " failed, check stop flow directive namespace. Sourcenamespace does not exist, or the flow already stop")
    }
  }

  def cleanStreamLookup(sourceNamespace: String, sinkNamespace: String): Unit = {
    lookup2SourceSinkNamespaceMap.foreach(kv => {
      if (kv._2.contains(sourceNamespace, sinkNamespace)) kv._2.remove((sourceNamespace, sinkNamespace))
    })
    lookup2SourceSinkNamespaceMap.retain((_, v) => v.nonEmpty)
  }

  def registerFlowConfigMap(sourceNamespace: String,
                            sinkNamespace: String,
                            swiftsProcessConfig: Option[SwiftsProcessConfig],
                            sinkProcessConfig: SinkProcessConfig,
                            directiveId: Long,
                            swiftsConfigStr: String,
                            sinkConfigStr: String,
                            consumptionDataTypeMap: Map[String, Boolean]
                           ): Unit = {
    synchronized {
      if (flowConfigMap.contains(sourceNamespace)) flowConfigMap(sourceNamespace) += (sinkNamespace -> (swiftsProcessConfig, sinkProcessConfig, directiveId, swiftsConfigStr, sinkConfigStr, consumptionDataTypeMap))
      else flowConfigMap(sourceNamespace) = mutable.LinkedHashMap(sinkNamespace -> (swiftsProcessConfig, sinkProcessConfig, directiveId, swiftsConfigStr, sinkConfigStr, consumptionDataTypeMap))

      val tmpLinkedHashMap = mutable.LinkedHashMap(flowConfigMap(sourceNamespace).toSeq.sortBy(_._2._3): _*)
      flowConfigMap(sourceNamespace) = tmpLinkedHashMap
    }
  }

  def registerSwiftsTransformReflectMap(className: String): Any = {
    if (!swiftsTransformReflectMap.contains(className)) {
      val clazz = Class.forName(className)
      val reflectObject: Any = clazz.newInstance()
      val transformMethod = clazz.getDeclaredMethod("transform", classOf[SparkSession], classOf[DataFrame], classOf[SwiftsProcessConfig])
      swiftsTransformReflectMap += (className -> (reflectObject, transformMethod))
    }
  }

  def getSwiftsTransformReflectValue(className: String): (Any, Method) = {
    swiftsTransformReflectMap(className)
  }

  def getSwiftsLogic(sourceNamespace: String, sinkNamespace: String): SwiftsProcessConfig = {
    if (flowConfigMap.contains(sourceNamespace)) {
      if (flowConfigMap(sourceNamespace).contains(sinkNamespace)) {
        flowConfigMap(sourceNamespace)(sinkNamespace)._1.get
      } else {
        throw new Exception("Cannot resolve sinkNamespace, you may send other directive and replace original one.")
      }
    } else {
      throw new Exception("Cannot resolve sourceNamespace, you may send other directive and replace original one.")
    }

  }


  def registerDataStoreConnectionsMap(lookupNamespace: String, connectionUrl: String, usrname: Option[String], password: Option[String], parameters: Option[Seq[KVConfig]]) {
    val connectionNamespace = lookupNamespace.split("\\.").slice(0, 3).mkString(".")
    if (!dataStoreConnectionsMap.contains(connectionNamespace)) {
      dataStoreConnectionsMap(connectionNamespace) = ConnectionConfig(connectionUrl, usrname, password, parameters)
    }
  }

  def registerStreamLookupNamespaceMap(sourceNamespace: String, sinkNamespace: String, swiftsProcessConfig: Option[SwiftsProcessConfig]) {
    if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.swiftsSql.nonEmpty) {
      val swiftsSql = swiftsProcessConfig.get.swiftsSql.get
      swiftsSql.foreach(sql => {
        if (sql.timeout.isDefined && sql.lookupNamespace.nonEmpty) {
          sql.lookupNamespace.get.split(",").foreach(lookupNamespace => {
            if (lookup2SourceSinkNamespaceMap.contains(lookupNamespace)) {
              lookup2SourceSinkNamespaceMap(lookupNamespace).add(sourceNamespace, sinkNamespace)
            } else {
              lookup2SourceSinkNamespaceMap(lookupNamespace) = mutable.HashSet((sourceNamespace, sinkNamespace))
            }
          })
        }
      })
    }
  }

  def getSourceAndSinkByStreamLookupNamespace(lookupNamespace: String): mutable.Set[(String, String)] = {
    if (lookup2SourceSinkNamespaceMap.contains(lookupNamespace))
      lookup2SourceSinkNamespaceMap(lookupNamespace)
    else
      throw new Exception("Cannot find lookupnamespace: " + lookupNamespace + " in streamLookupNamespaceMap")
  }

  def getDataStoreConnectionsMap(namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")
    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

  //  def existconnectionNamespace(connectionNamespace: String): Boolean = {
  //    dataStoreConnectionsMap.contains(connectionNamespace)
  //  }

  def getFlowConfigMap(sourceNamespace: String): mutable.Map[String, (Option[SwiftsProcessConfig], SinkProcessConfig, Long, String, String, Map[String, Boolean])] = {
    flowConfigMap(sourceNamespace)
  }


  //  def existSourceNamespace(namespace: String): Boolean = {
  //    flowConfigMap.contains(namespace)
  //  }

  def existNamespace(namespaceSet: Set[String], realNamespace: String): Boolean = {
    //var matchNs:String = null
    var hitCount = 0
    namespaceSet.foreach(k => {
      if (matchNameSpace(k, realNamespace)) {
        hitCount += 1
        //  matchNs = k
      }
    })
    if (hitCount == 0) false
    else if (hitCount == 1) true
    else throw new Exception("you register namespace more than 1")
  }

  //  def existLookupNamespace(namespaceSet: Set[String], realNamespace: String): Boolean = {
  //    var hitCount = 0
  //    namespaceSet.foreach(k => {
  //      if (matchNameSpace(k, realNamespace)) {
  //        hitCount += 1
  //      }
  //    })
  //    if (hitCount == 0) false
  //    else if (hitCount == 1) true
  //    else throw new Exception("you register lookupNamespace more than 1")
  //  }

  //  def existLookupNamespace(namespace: String): Boolean = {
  //    lookup2SourceSinkNamespaceMap.contains(namespace)
  //  }

  def getAllSourceParseMap = {
    JsonSourceParseMap.toMap
  }


  def getAllLookupNamespaceSet: Set[String] = {
    lookup2SourceSinkNamespaceMap.keySet.toSet
  }

  def getAllMainNamespaceSet: Set[String] = {
    flowConfigMap.keySet.toSet
  }

  //  def getAllFlowConfigMap: mutable.Map[String, mutable.LinkedHashMap[String, (Option[SwiftsProcessConfig], SinkProcessConfig, Long, String, String)]] = {
  //    flowConfigMap
  //  }

  def existEventTs(sourceNamespace: String, sinkNamespace: String): Boolean = {
    eventTsMap.contains((sourceNamespace, sinkNamespace))
  }

  def getEventTs(sourceNamespace: String, sinkNamespace: String): String = {
    eventTsMap((sourceNamespace, sinkNamespace))
  }

  def setEventTs(sourceNamespace: String, sinkNamespace: String, minTs: String): Unit = {
    if (existEventTs(sourceNamespace, sinkNamespace)) {
      val storedMinTs = getEventTs(sourceNamespace, sinkNamespace)
      if (firstTimeAfterSecond(storedMinTs, minTs)) eventTsMap((sourceNamespace, sinkNamespace)) = minTs
    } else {
      eventTsMap((sourceNamespace, sinkNamespace)) = minTs
    }
  }
}
