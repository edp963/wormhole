package edp.wormhole.common.json

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.ums.{UmsField, UmsFieldType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object JsonSourceConf {
  def parse(dataSchema: String): RegularJsonSchema = {
    val jsonObj = JSON.parseObject(dataSchema)
    val fieldsValue = jsonObj.getString("fields")
    val fieldsJsonArray: JSONArray = JSON.parseArray(fieldsValue)
    var umsTsField, umsIdField, umsOpField, umsUidField = ""
    //    val majorMap = new mutable.HashMap[(String, String), Seq[FieldInfo]]
    val schemaArr: ArrayBuffer[FieldInfo] = ArrayBuffer[FieldInfo]()
    val seqField: ArrayBuffer[UmsField] = ArrayBuffer[UmsField]()
    val twoFieldArr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    for (i <- 0 until fieldsJsonArray.size()) {
      //      val subFieldsInfo = ArrayBuffer[FieldInfo]()
      def convert(jsonStr: String): FieldInfo = {
        val jsonObj = JSON.parseObject(jsonStr)
        val name = jsonObj.getString("name")
        val `type` = if (jsonObj.containsKey("rename") && jsonObj.getString("rename").nonEmpty && jsonObj.getString("rename") == "ums_ts_") "datetime" else jsonObj.getString("type")
        val rename = if (jsonObj.containsKey("rename") && jsonObj.getString("rename").nonEmpty) Some(jsonObj.getString("rename")) else None
        val umsOpMapping = if (jsonObj.containsKey("ums_sys_mapping") && jsonObj.getString("ums_sys_mapping").nonEmpty) Some(jsonObj.getString("ums_sys_mapping")) else None
        val actualName = if (rename.isDefined) (name, rename.get) else (name, name)
        val umsFieldName = if (rename.isDefined) rename.get else name
        umsFieldName match {
          case "ums_ts_" => umsTsField = name
          case "ums_id_" => umsIdField = name
          case "ums_op_" => umsOpField = name
          case "ums_uid_" => umsUidField = name
          case _ =>
        }
        val nullable = if (jsonObj.containsKey("nullable") && jsonObj.getString("nullable").nonEmpty) Some(jsonObj.getBooleanValue("nullable")) else Some(true)
        val separator = if (jsonObj.containsKey("tuple_sep") && jsonObj.getString("tuple_sep").nonEmpty) Some(jsonObj.getString("tuple_sep")) else None
        if (`type` == "jsonarray" || `type` == "tuple" || `type` == "jsonobject") {
          val subJsonArray = jsonObj.getJSONArray("sub_fields")
          val subList = ListBuffer.empty[FieldInfo]
          for (i <- 0 until subJsonArray.size()) {
            val subJson: JSONObject = subJsonArray.getJSONObject(i)
            subList += convert(subJson.toJSONString)
          }

          FieldInfo(name, `type`, umsOpMapping, nullable, Some(subList), rename, separator)
        } else {
          twoFieldArr.append(actualName)
          val realType = `type`.split("array")(0)
          seqField.append(UmsField(actualName._2, UmsFieldType.withName(realType.toLowerCase), nullable))
          FieldInfo(name, `type`, umsOpMapping, nullable, None, rename, None)
        }
      }

      val fieldInfo = convert(fieldsJsonArray.getString(i))

      schemaArr.append(fieldInfo)
    }
    RegularJsonSchema(schemaArr, twoFieldArr, seqField)
  }
}

case class RegularJsonSchema(fieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)], schemaField: Seq[UmsField])

//case class UmsSysRename(umsSysTs: String, umsSysId: Option[String], umsSysOp: Option[String], umsSysUid: Option[String])

case class FieldInfo(name: String,
                     `type`: String,
                     umsSysMapping: Option[String],
                     nullable: Option[Boolean] = Some(false),
                     subFields: Option[Seq[FieldInfo]],
                     rename: Option[String],
                     separator: Option[String]
                    )