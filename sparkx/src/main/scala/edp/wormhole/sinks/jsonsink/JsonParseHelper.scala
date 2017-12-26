package edp.wormhole.sinks.jsonsink

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsSysField

object JsonParseHelper {
   def jsonObjHelper(tuple: Seq[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], subFields: JSONArray): JSONObject = {
    val outputJson = new JSONObject()
    val size = subFields.size()
    for (i <- 0 until size) {
      val jsonObj = subFields.getJSONObject(i)
      val name = jsonObj.getString("name")
      val dataType = jsonObj.getString("type")
      if (schemaMap.contains(name)) {
        val subFields: Option[JSONArray] = if (jsonObj.containsKey("sub_fields")) Some(jsonObj.getJSONArray("sub_fields")) else None
        val value = str2Json(name, tuple(schemaMap(name)._1), dataType, subFields)
        outputJson.put(name, value)
      } else {
        assert(dataType == "jsonobject", "name: " + name + " not found, it should be jsonobject, but it is " + dataType)
        val subFields: JSONArray = jsonObj.getJSONArray("sub_fields")
        val subJsonObj: JSONObject = jsonObjHelper(tuple, schemaMap, subFields)
        outputJson.put(name, subJsonObj)
      }
    }
    outputJson
  }

  private def str2Json(outerName: String, data: String, dataType: String, subFieldsOption: Option[JSONArray]): Any = {
    if (dataType == "jsonobject") {
      val jsonData = JSON.parseObject(data)
      val outputJson = new JSONObject()
      val subFields = subFieldsOption.get
      val size = subFields.size()
      for (i <- 0 until size) {
        val jsonObj = subFields.getJSONObject(i)
        val name = jsonObj.getString("name")
        val subDataType = jsonObj.getString("type")
        val subData = jsonData.getString(name)
        val subSubFields = if (jsonObj.containsKey("sub_fields")) Some(jsonObj.getJSONArray("sub_fields")) else None
        val subResult: Any = str2Json(name, subData, subDataType, subSubFields)
        outputJson.put(name, subResult)
      }
      outputJson
    } else if (dataType == "jsonarray") {
      val jsonArray = JSON.parseArray(data)
      val jsonArraySubFields = subFieldsOption.get
      val dataSize = jsonArray.size()
      val schemaSize = jsonArraySubFields.size()
      val result = new JSONArray()
      for (i <- 0 until dataSize) {
        val outputJson = new JSONObject()
        for (j <- 0 until schemaSize) {
          val schemaObj = jsonArraySubFields.getJSONObject(j)
          val value = str2Json(schemaObj.getString("name"), jsonArray.getJSONObject(i).get(schemaObj.getString("name")).toString, schemaObj.getString("type"), if (schemaObj.containsKey("sub_fields")) Some(schemaObj.getJSONArray("sub_fields")) else None)
          outputJson.put(schemaObj.getString("name"), value)
        }
        result.add(outputJson)
      }
      result
    } else if (dataType.endsWith("array")) {
      JSON.parseArray(data)
    } else {
      if (outerName == UmsSysField.ACTIVE.toString)
        data match {
          case "i" | "u" => 1
          case "d" => 0
          case _ => -1
        } else {
        parseData2CorrectType(dataType, data)
      }
    }
  }
   def parseData2CorrectType(dataType: String, data: String): Any = {
    dataType.toLowerCase match {
      case "int" => data.toInt
      case "long" => data.toLong
      case "double" => data.toDouble
      case "float" => data.toFloat
      case "boolean" => data.toBoolean
      case _ => data
    }
  }
}
