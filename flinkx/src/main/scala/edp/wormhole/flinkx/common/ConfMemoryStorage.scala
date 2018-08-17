package edp.wormhole.flinkx.common

import edp.wormhole.common.json.FieldInfo
import edp.wormhole.ums.UmsField
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
//单独

object  ConfMemoryStorage extends Serializable{
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
