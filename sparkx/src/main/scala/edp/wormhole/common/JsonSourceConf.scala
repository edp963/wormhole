package edp.wormhole.common

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
        val `type` = if (jsonObj.containsKey("umsSysField") && jsonObj.getString("umsSysField").nonEmpty&&jsonObj.getString("umsSysField")=="ums_ts_") "datetime" else jsonObj.getString("type")
        val rename = if (jsonObj.containsKey("rename") && jsonObj.getString("rename").nonEmpty) Some(jsonObj.getString("rename")) else None
        val umsSysField = if (jsonObj.containsKey("umsSysField") && jsonObj.getString("umsSysField").nonEmpty) Some(jsonObj.getString("umsSysField")) else None
        val umsOpMapping = if (jsonObj.containsKey("umsOpMapping") && jsonObj.getString("umsOpMapping").nonEmpty) Some(jsonObj.getString("umsOpMapping")) else None
        val actualName = if (rename.isDefined) (name, rename.get) else (name, name)
        if (umsSysField.isDefined) {
          val umsFieldVal = umsSysField.get
          umsFieldVal match {
            case "ums_ts_" => umsTsField = if (rename.isDefined) rename.get else name
            case "ums_id_" => umsIdField = if (rename.isDefined) rename.get else name
            case "ums_op_" => umsOpField = if (rename.isDefined) rename.get else name
            case "ums_uid_" => umsUidField = if (rename.isDefined) rename.get else name
          }
        }
        val nullable = if (jsonObj.containsKey("nullable") && jsonObj.getString("nullable").nonEmpty) Some(jsonObj.getBooleanValue("nullable")) else Some(true)
        val separator = if (jsonObj.containsKey("tuple_sep") && jsonObj.getString("tuple_sep").nonEmpty) Some(jsonObj.getString("tuple_sep")) else None
        if (`type` == "jsonarray" || `type` == "tuple" || `type` == "jsonobj") {
          val subJsonArray = jsonObj.getJSONArray("sub_fields")
          val subList = ListBuffer.empty[FieldInfo]
          for (i <- 0 until subJsonArray.size()) {
            val subJson: JSONObject = subJsonArray.getJSONObject(i)
            subList += convert(subJson.toJSONString)
          }

          FieldInfo(name, `type`, umsSysField, umsOpMapping, nullable, Some(subList), rename, separator)
        } else {
          twoFieldArr.append(actualName)
          seqField.append(UmsField(actualName._2, UmsFieldType.withName(`type`.toLowerCase)))
          FieldInfo(name, `type`, umsSysField, umsOpMapping, nullable, None, rename, None)
        }
      }
      val fieldInfo = convert(fieldsJsonArray.getString(i))

      schemaArr.append(fieldInfo)
    }
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
                     umsSysMapping: Option[String],
                     nullable: Option[Boolean] = Some(false),
                     subFields: Option[Seq[FieldInfo]],
                     rename: Option[String],
                     separator: Option[String]
                    )


