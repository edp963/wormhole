package edp.wormhole.flinkx.common

import java.lang.reflect.Method
import org.apache.log4j.Logger

import edp.wormhole.common.json.FieldInfo
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.ums.UmsField
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.util.config.{ConnectionConfig,KVConfig}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object  ConfMemoryStorage extends Serializable{
  private val logger: Logger = Logger.getLogger(ConfMemoryStorage.getClass)
  private val dataStoreConnectionsMap = mutable.HashMap.empty[String, ConnectionConfig]
  private val sinkTransformReflectMap = mutable.HashMap.empty[String, (Any, Method)]

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



  val JsonSourceParseMap = mutable.HashMap.empty[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])]

  def existJsonSourceParseMap(protocol: UmsProtocolType, namespace: String) = {
    JsonSourceParseMap.contains((protocol, namespace))
  }

  def registerJsonSourceParseMap(protocolType: UmsProtocolType, namespace: String, umsField: Seq[UmsField], fieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)]) = {
    JsonSourceParseMap((protocolType, namespace)) = (umsField, fieldsInfo, twoFieldsArr)
  }

  def getAllSourceParseMap = {
    JsonSourceParseMap.toMap
  }
}
