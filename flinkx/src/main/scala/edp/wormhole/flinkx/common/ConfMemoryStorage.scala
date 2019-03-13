package edp.wormhole.flinkx.common

import java.lang.reflect.Method

import edp.wormhole.common.json.FieldInfo
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.ums.UmsField
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.util.config.ConnectionConfig

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConfMemoryStorage extends Serializable {

  private val sinkTransformReflectMap = mutable.HashMap.empty[String, (Any, Method)]

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

  def matchNameSpace(namespace1: String, namespace2: String): Boolean = {
    //    if (flowConfigMap.contains(namespace2)) {
    //      return true
    //    }
    val namespaceArray1 = namespace1.split("\\.")
    val namespaceArray2 = namespace2.split("\\.")
    namespaceArray1(0) == namespaceArray2(0) && namespaceArray1(1) == namespaceArray2(1) && namespaceArray1(2) == namespaceArray2(2) && namespaceArray1(3) == namespaceArray2(3)
  }
}