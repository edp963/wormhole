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

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.util.JsonUtils
import org.json4s.DefaultFormats
import org.json4s.ext.EnumNameSerializer

import scala.collection.mutable.ArrayBuffer

object UmsSchemaUtils extends UmsSchemaUtils

trait UmsSchemaUtils {
  JsonUtils.json4sFormats = JsonUtils.json4sFormats + new EnumNameSerializer(UmsProtocolType) + new EnumNameSerializer(UmsFieldType)

  implicit val formats = DefaultFormats

  def toUms(json: String): Ums = {
    val jsonObj: JSONObject = JSON.parseObject(json)
    val protocol = jsonObj.getJSONObject("protocol").getString("type")
    val schema = jsonObj.getJSONObject("schema")
    var context: Option[String] = None
    if (jsonObj.containsKey("context")) {
      context = Some(jsonObj.getString("context"))
    }

    val umsSchema = toUmsSchemaFromJsonObject(schema)

    val payloadArr: Option[Seq[UmsTuple]] = parsePayload(jsonObj)

    Ums(UmsProtocol(UmsProtocolType.umsProtocolType(protocol)),
      umsSchema,
      payloadArr)
  }

  def parsePayload(jsonObj: JSONObject): Option[Seq[UmsTuple]] = {
    if (jsonObj.containsKey("payload") && jsonObj.getJSONArray("payload").size() > 0) {
      val payloadJsonArr = jsonObj.getJSONArray("payload")
      val payloadSize = payloadJsonArr.size()
      val tmpPayload: Array[UmsTuple] = new Array[UmsTuple](payloadSize)
      for (i <- 0 until payloadSize) {
        val tuple: JSONObject = payloadJsonArr.get(i).asInstanceOf[JSONObject]
        val tupleJsonArr: JSONArray = tuple.getJSONArray("tuple")
        val tupleSeq = new ArrayBuffer[String]()
        for (j <- 0 until tupleJsonArr.size()) {
          val ele = tupleJsonArr.get(j)
          val tuple = if (ele == null) null else ele.toString
          tupleSeq += tuple
        }

        val tupleArr = UmsTuple(tupleSeq)
        tmpPayload(i) = tupleArr
      }
      Some(tmpPayload)
    } else {
      None
    }
  }


  def toJsonCompact(ums: Ums): String = JsonUtils.jsonCompact(JsonUtils.caseClass2json[Ums](ums))

  def toFastJsonCompact(ums: Ums): String = {
    toJson(ums, false)
  }


  def toJsonPretty(ums: Ums): String = JsonUtils.jsonPretty(JsonUtils.caseClass2json[Ums](ums))

  def toFastJsonPretty(ums: Ums): String = {
    toJson(ums, true)
  }

  def toJson(ums: Ums, bool: Boolean): String = {

    val protocol: UmsProtocol = ums.protocol
    val umsSchema: UmsSchema = ums.schema

    val umsMap = new java.util.HashMap[String, Object]

    // protocol
    val protocolTypeMap = new java.util.HashMap[String, Object]()
    protocolTypeMap.put("type", protocol.`type`.toString)
    umsMap.put("protocol", protocolTypeMap)

    // schema:Namespace
    val schemaNamespaceMap = new java.util.HashMap[String, Object]()
    schemaNamespaceMap.put("namespace", umsSchema.namespace)

    // schema:Field
    val listField = umsSchema.fields_get.toList
    val schemaFieldList = new java.util.ArrayList[Object]()
    for (i <- listField.indices) {
      val schemaFieldMap = new java.util.HashMap[String, Object]
      schemaFieldMap.put("name", listField(i).name.toString)
      schemaFieldMap.put("type", listField(i).`type`.toString)
      schemaFieldMap.put("nullable", listField(i).nullable.mkString)
      schemaFieldList.add(schemaFieldMap)
    }
    if (listField.nonEmpty) schemaNamespaceMap.put("fields", schemaFieldList)

    umsMap.put("schema", schemaNamespaceMap)

    // payload
    val payloadList = new java.util.ArrayList[Object]

    val umsTupleList: List[UmsTuple] = ums.payload_get.toList


    for (i <- umsTupleList.indices) {
      val tupleMap = new java.util.HashMap[String, Object]
      val umsTuple: UmsTuple = umsTupleList(i)
      tupleMap.put("tuple", umsTuple.tuple.toArray)
      payloadList.add(tupleMap)
    }
    if (umsTupleList.nonEmpty) umsMap.put("payload", payloadList)

    JSON.toJSONString(umsMap, bool).replace("\"false\"", "false").replace("\"true\"", "true")
  }

  def parseUmsSchema(json: String): UmsSchema = {
    val schema: JSONObject = JSON.parseObject(json)
    toUmsSchemaFromJsonObject(schema)
  }

  def toUmsSchema(json: String): UmsSchema = {
    val schema: JSONObject = JSON.parseObject(json)
    toUmsSchemaFromJsonObject(schema.getJSONObject("schema"))
  }

  def toUmsSchemaFromJsonObject(schema: JSONObject): UmsSchema = {
    val namespace = schema.getString("namespace").toLowerCase
    val fields = if (schema.containsKey("fields") && schema.getJSONArray("fields").size() > 0) {
      val fieldsArr: JSONArray = schema.getJSONArray("fields")
      val tmpFields: Array[UmsField] = new Array[UmsField](fieldsArr.size())
      for (i <- 0 until fieldsArr.size()) {
        val oneField: JSONObject = fieldsArr.get(i).asInstanceOf[JSONObject]
        val `type` = UmsFieldType.umsFieldType(oneField.getString("type"))
        val name = oneField.getString("name").toLowerCase
        val nullable: Boolean = if (oneField.containsKey("nullable")) oneField.getBoolean("nullable") else false
        tmpFields(i) = UmsField(name, `type`, Some(nullable))
      }
      Some(tmpFields.toList)
    } else {
      None
    }
    UmsSchema(namespace, fields)
  }

  def toJsonSchemaCompact(schema: UmsSchema): String = JsonUtils.jsonCompact(JsonUtils.caseClass2json[UmsSchema](schema))

  def toFastJsonSchemaCompact(schema: UmsSchema, isFull: Boolean): String = {
    toFastJsonSchema(schema, false, isFull)
  }

  def toJsonSchemaPretty(schema: UmsSchema): String = JsonUtils.jsonPretty(JsonUtils.caseClass2json[UmsSchema](schema))

  def toFastJsonSchemaPretty(schema: UmsSchema, isFull: Boolean): String = {
    toFastJsonSchema(schema, true, isFull)
  }

  def toFastJsonSchema(schema: UmsSchema, bool: Boolean, isFull: Boolean): String = {
    val umsSchema: UmsSchema = schema

    // schema:Namespace
    val schemaNamespaceMap = new java.util.HashMap[String, Object]()
    schemaNamespaceMap.put("namespace", umsSchema.namespace)

    // schema:Field
    val listField = umsSchema.fields_get.toList
    val schemaFieldList = new java.util.ArrayList[Object]()
    for (i <- listField.indices) {
      val schemaFieldMap = new java.util.HashMap[String, Object]
      schemaFieldMap.put("name", listField(i).name.toString)
      schemaFieldMap.put("type", listField(i).`type`.toString)
      if (isFull) schemaFieldMap.put("nullable", listField(i).nullable.mkString)
      schemaFieldList.add(schemaFieldMap)
    }
    if (listField.nonEmpty) schemaNamespaceMap.put("fields", schemaFieldList)

    JSON.toJSONString(schemaNamespaceMap, bool).replace("\"false\"", "false").replace("\"true\"", "true")
  }
}
