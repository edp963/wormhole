package edp.wormhole.common

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.ums.{UmsField, UmsFieldType}

import scala.collection.mutable.ArrayBuffer

object JsonSourceConf {
  def parse(dataSchema: String): RegularJsonSchema = {


    val jsonObj = JSON.parseObject(dataSchema)
    val fieldsValue = jsonObj.getString("fields")
    val fieldsJsonArray = JSON.parseArray(fieldsValue)
    var umsTsField, umsIdField, umsOpField, umsUidField = ""
    //val majorMap = new mutable.HashMap[(String, String), Seq[FieldInfo]]
    val schemaArr = ArrayBuffer[FieldInfo]()
    val seqField = ArrayBuffer[UmsField]()
    val twoFieldArr = ArrayBuffer[(String, String)]()
    for (i <- 0 until fieldsJsonArray.size()) {
      val subFieldsInfo = ArrayBuffer[FieldInfo]()
      val fieldJsonObj = fieldsJsonArray.get(i).asInstanceOf[JSONObject]
      val fieldName = fieldJsonObj.getString("name")
      val dataType: String = fieldJsonObj.getString("type")
      println((fieldName, dataType))
      val rename = if (fieldJsonObj.containsKey("rename")) Some(fieldJsonObj.getString("rename")) else None

      val umsField = if (fieldJsonObj.containsKey("ums_sys_field")) Some(fieldJsonObj.getString("ums_sys_field")) else None
      if (umsField.isDefined) {
        val umsFieldVal = umsField.get
        umsFieldVal match {
          case "ums_ts_" => umsTsField = if (rename.isDefined) rename.get else fieldName
          case "ums_id_" => umsIdField = if (rename.isDefined) rename.get else fieldName
          case "ums_op_" => umsOpField = if (rename.isDefined) rename.get else fieldName
          case "ums_uid_" => umsUidField = if (rename.isDefined) rename.get else fieldName
        }
      }
      val umsMapping = if (fieldJsonObj.containsKey("ums_op_mapping")) Some(fieldJsonObj.getString("ums_op_mapping")) else None
      val actualName = if (rename.isDefined) (fieldName, rename.get) else (fieldName, fieldName)
      val nullable = if (fieldJsonObj.containsKey("nullable")) Some(fieldJsonObj.getBooleanValue("nullable")) else Some(false)
      val separator = if (fieldJsonObj.containsKey("tuple_sep")) Some(fieldJsonObj.getString("tuple_sep")) else None
      if (dataType == "jsonarray" || dataType == "tuple" || dataType == "jsonobj") {
        val subFieldsJsonArr = JSON.parseArray(fieldJsonObj.getString("sub_fields"))
        for (i <- 0 until subFieldsJsonArr.size()) {
          val subFieldJsonObj = subFieldsJsonArr.get(i).asInstanceOf[JSONObject]
          val subFieldName = subFieldJsonObj.getString("name")
          val subDataType = subFieldJsonObj.getString("type")
          val subRename = if (subFieldJsonObj.containsKey("rename")) Some(subFieldJsonObj.getString("rename")) else None
          val subUmsField = if (subFieldJsonObj.containsKey("ums_sys_field")) Some(subFieldJsonObj.getString("ums_sys_field")) else None
          if (subUmsField.isDefined) {
            val subUmsFieldVal = subUmsField.get
            subUmsFieldVal match {
              case "ums_ts_" => umsTsField = if (subRename.isDefined) subRename.get else subFieldName
              case "ums_id_" => umsIdField = if (subRename.isDefined) subRename.get else subFieldName
              case "ums_op_" => umsOpField = if (subRename.isDefined) subRename.get else subFieldName
              case "ums_uid_" => umsUidField = if (subRename.isDefined) subRename.get else subFieldName
            }
          }
          val subUmsMapping = if (subFieldJsonObj.containsKey("ums_op_mapping")) Some(subFieldJsonObj.getString("ums_op_mapping")) else None

          val subActualName = if (subRename.isDefined) (subFieldName, subRename.get) else (subFieldName, subFieldName)
          val subFieldNullable = if (subFieldJsonObj.containsKey("nullable")) Some(subFieldJsonObj.getBooleanValue("nullable")) else Some(false)
          twoFieldArr.append(subActualName)
          seqField.append(UmsField(subActualName._2, UmsFieldType.umsFieldType(subDataType)))
          val subFieldInfo = FieldInfo(subFieldName, subDataType, subUmsField, subUmsMapping, subFieldNullable, None, subRename, None)
          subFieldsInfo.append(subFieldInfo)
        }
      }
      else {
        twoFieldArr.append(actualName)
        seqField.append(UmsField(actualName._2, UmsFieldType.umsFieldType(dataType)))
      }
      val actualSubFieldsInfo = if (subFieldsInfo.size == 0) None else Some(subFieldsInfo)
      schemaArr.append(FieldInfo(fieldName, dataType, umsField, umsMapping, nullable, actualSubFieldsInfo, rename, separator))
    }
    //majorMap((sourceNamespace, protocolType)) = schemaArr
    val umsSysRename = UmsSysRename(umsTsField, judgeIfIsDefined(umsIdField), judgeIfIsDefined(umsOpField), judgeIfIsDefined(umsUidField))
    RegularJsonSchema(schemaArr, twoFieldArr, seqField, umsSysRename)


  }

  def judgeIfIsDefined(judgeStr: String): Option[String] = {
    if (judgeStr == "") None else Some(judgeStr)
  }
}

case class RegularJsonSchema(fieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)], schemaField: Seq[UmsField], umsSysRename: UmsSysRename)

case class UmsSysRename(umsSysTs: String, umsSysId: Option[String], umsSysOp: Option[String], umsSysUid: Option[String])

case class FieldInfo(name: String,
                     `type`: String,
                     umsSysField: Option[String],
                     umsOpMapping: Option[String],
                     nullable: Option[Boolean] = Some(false),
                     subFields: Option[Seq[FieldInfo]],
                     rename: Option[String],
                     separator: Option[String]
                    )


