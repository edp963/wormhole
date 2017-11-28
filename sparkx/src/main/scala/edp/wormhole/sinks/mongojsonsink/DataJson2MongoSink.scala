package edp.wormhole.sinks.mongojsonsink

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.mongodb.casbah
import com.mongodb.casbah.commons.{Imports, MongoDBList, MongoDBObject}
import com.mongodb.casbah.{MongoCollection, MongoConnection, MongoDB, commons}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataJson2MongoSink extends SinkProcessor with EdpLogging {
  private def str2Json(data: String, dataType: String, subFieldsOption: Option[JSONArray]): Any = {
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
        val subResult: Any = str2Json(subData, subDataType, subSubFields)
        outputJson.put(name, subResult)
      }
      outputJson
    } else {
      data
    }
  }

  private def jsonObjHelper(tuple: Seq[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], subFields: JSONArray): JSONObject = {
    val outputJson = new JSONObject()
    val size = subFields.size()
    for (i <- 0 until size) {
      val jsonObj = subFields.getJSONObject(i)
      val name = jsonObj.getString("name")
      val dataType = jsonObj.getString("type")
      if (schemaMap.contains(name)) {
        val subFields: Option[JSONArray] = if (jsonObj.containsKey("sub_fields")) Some(jsonObj.getJSONArray("sub_fields")) else None
        val value = str2Json(tuple(schemaMap(name)._1), dataType, subFields)
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

  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val database: MongoDB = MongoConnection("ip")("mydb")
    val collection: MongoCollection = database("mycol")
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    tupleList.foreach(tuple => {
      val result: JSONObject = jsonObjHelper(tuple, schemaMap, targetSchemaArr)
      save2Mongo(result, targetSchemaArr, collection)
    })
  }

  private def constructBuilder(jsonData: JSONObject, subField: JSONObject, builder: mutable.Builder[(String, Any), Imports.DBObject]): Unit = {
    val name = subField.getString("name")
    val dataType = subField.getString("type")
    if (dataType == "jsonobject") {
      val jsonContent = jsonData.getJSONObject(name)
      val subContent = constructBuilder(jsonContent, subField.getJSONArray("sub_fields"))
      builder += name -> subContent
    } else {
      val content = jsonData.getString(name)
      if (dataType == "jsonarray") {
        val list = MongoDBList
        val jsonArray = JSON.parseArray(content)

        val jsonArraySubFields = subField.getJSONArray("sub_fields")
        val dataSize = jsonArray.size()
        val schemaSize = jsonArraySubFields.size()

        val toUpsert = ListBuffer.empty[Imports.DBObject]
        for (i <- 0 until dataSize) {
          val subBuilder = MongoDBObject.newBuilder
          for (j <- 0 until schemaSize) {
            constructBuilder(jsonArray.getJSONObject(i), jsonArraySubFields.getJSONObject(j), subBuilder)
          }
          toUpsert.append(subBuilder.result())
        }
        builder += name -> list(toUpsert: _*)
      } else if (dataType.endsWith("array")) {
        if (content != null && content.trim.nonEmpty) {
          val jsonArray = JSON.parseArray(content)
          val toUpsert = ListBuffer.empty[String]
          val size = jsonArray.size()
          for (i <- 0 until size) {
            toUpsert.append(jsonArray.get(i).asInstanceOf[String])
          }
          builder += name -> toUpsert
        }
      } else {
        builder += name -> content
      }
    }
  }

  private def constructBuilder(jsonData: JSONObject, subFields: JSONArray): commons.Imports.DBObject = {
    val builder: mutable.Builder[(String, Any), Imports.DBObject] = MongoDBObject.newBuilder //todo save all data as String
    val size = subFields.size()
    for (i <- 0 until size) {
      val jsonObj = subFields.getJSONObject(i)
      constructBuilder(jsonData, jsonObj, builder)

    }
    builder.result()
  }

  private def save2Mongo(jsonData: JSONObject, subFields: JSONArray, collection: MongoCollection) = {
    val result: casbah.commons.Imports.DBObject = constructBuilder(jsonData: JSONObject, subFields: JSONArray)
    collection.save(result)
  }
}
