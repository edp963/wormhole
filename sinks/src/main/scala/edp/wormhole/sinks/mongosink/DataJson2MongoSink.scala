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

package edp.wormhole.sinks.mongosink

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.mongodb.casbah._
import com.mongodb.casbah.commons.{Imports, MongoDBList, MongoDBObject}
import com.mongodb.{ReadPreference, WriteConcern, casbah}
import edp.wormhole.common.json.JsonParseHelper
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.SourceMutationType.INSERT_ONLY
import edp.wormhole.sinks.{SourceMutationType, _IDHelper}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import javax.net.SocketFactory
import org.apache.log4j.Logger
import org.mongodb.scala.{MongoCredential, ServerAddress}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataJson2MongoSink extends SinkProcessor{
  private lazy val logger = Logger.getLogger(this.getClass)
  def getMongoClient(namespace: UmsNamespace,
                     sinkProcessConfig: SinkProcessConfig,
                     connectionConfig: ConnectionConfig): MongoClient = {
    val db: String = namespace.database
    val user = connectionConfig.username.getOrElse("")
    val password = connectionConfig.password.getOrElse("")

    val kvConfig = connectionConfig.parameters
    val credential =
      if (user != "") MongoCredential.createCredential(user, db, password.toCharArray)
      else null
    val serverList = connectionConfig.connectionUrl.split(",").map(conn => {
      val ip2port = conn.split("\\:")
      new ServerAddress(ip2port(0).trim, ip2port(1).trim.toInt)
    }).toList

    if (kvConfig.nonEmpty) {
      val configMap: Map[String, String] = kvConfig.get.map(kv => {
        (kv.key, kv.value)
      }).toMap
      val connectionsPerHost: Int = if (configMap.contains("connectionsPerHost")) configMap("connectionsPerHost").toInt else MongoClientOptions.Defaults.getConnectionsPerHost
      val connectTimeout: Int = if (configMap.contains("connectTimeout")) configMap("connectTimeout").toInt else MongoClientOptions.Defaults.getConnectTimeout
      val cursorFinalizerEnabled: Boolean = if (configMap.contains("cursorFinalizerEnabled")) configMap("cursorFinalizerEnabled").toBoolean else MongoClientOptions.Defaults.isCursorFinalizerEnabled
      val dbDecoderFactory = MongoClientOptions.Defaults.getDbDecoderFactory
      val DBEncoderFactory = MongoClientOptions.Defaults.getDbEncoderFactory
      val description: String = if (configMap.contains("description")) configMap("description") else MongoClientOptions.Defaults.getDescription
      val maxWaitTime: Int = if (configMap.contains("maxWaitTime")) configMap("maxWaitTime").toInt else MongoClientOptions.Defaults.getMaxWaitTime
      val readPreference: ReadPreference = MongoClientOptions.Defaults.getReadPreference
      val socketFactory: SocketFactory = MongoClientOptions.Defaults.getSocketFactory
      val socketKeepAlive: Boolean = if (configMap.contains("socketKeepAlive")) configMap("socketKeepAlive").toBoolean else MongoClientOptions.Defaults.isSocketKeepAlive
      val socketTimeout: Int = if (configMap.contains("socketTimeout")) configMap("socketTimeout").toInt else MongoClientOptions.Defaults.getSocketTimeout
      val threadsAllowedToBlockForConnectionMultiplier: Int = if (configMap.contains("threadsAllowedToBlockForConnectionMultiplier")) configMap("threadsAllowedToBlockForConnectionMultiplier").toInt else MongoClientOptions.Defaults.getThreadsAllowedToBlockForConnectionMultiplier
      val writeConcern: WriteConcern = MongoClientOptions.Defaults.getWriteConcern
      val alwaysUseMBeans: Boolean = if (configMap.contains("alwaysUseMBeans")) configMap("alwaysUseMBeans").toBoolean else MongoClientOptions.Defaults.isAlwaysUseMBeans
      val heartbeatConnectTimeout: Int = if (configMap.contains("heartbeatConnectTimeout")) configMap("heartbeatConnectTimeout").toInt else MongoClientOptions.Defaults.getHeartbeatConnectTimeout
      val heartbeatFrequency: Int = if (configMap.contains("heartbeatFrequency")) configMap("heartbeatFrequency").toInt else MongoClientOptions.Defaults.getHeartbeatFrequency
      val heartbeatSocketTimeout: Int = if (configMap.contains("heartbeatSocketTimeout")) configMap("heartbeatSocketTimeout").toInt else MongoClientOptions.Defaults.getHeartbeatSocketTimeout
      val maxConnectionIdleTime: Int = if (configMap.contains("maxConnectionIdleTime")) configMap("maxConnectionIdleTime").toInt else MongoClientOptions.Defaults.getMaxConnectionIdleTime
      val maxConnectionLifeTime: Int = if (configMap.contains("maxConnectionLifeTime")) configMap("maxConnectionLifeTime").toInt else MongoClientOptions.Defaults.getMaxConnectionLifeTime
      val minConnectionsPerHost: Int = if (configMap.contains("minConnectionsPerHost")) configMap("minConnectionsPerHost").toInt else MongoClientOptions.Defaults.getMinConnectionsPerHost
      val requiredReplicaSetName: String = if (configMap.contains("requiredReplicaSetName")) configMap("requiredReplicaSetName") else MongoClientOptions.Defaults.getRequiredReplicaSetName
      val minHeartbeatFrequency: Int = if (configMap.contains("minHeartbeatFrequency")) configMap("minHeartbeatFrequency").toInt else MongoClientOptions.Defaults.getMinHeartbeatFrequency
      if (credential != null)
        MongoClient(serverList, List(credential), MongoClientOptions(connectionsPerHost, connectTimeout, cursorFinalizerEnabled, dbDecoderFactory, DBEncoderFactory,
          description, maxWaitTime, readPreference, socketFactory, socketKeepAlive, socketTimeout, threadsAllowedToBlockForConnectionMultiplier, writeConcern, alwaysUseMBeans,
          heartbeatConnectTimeout, heartbeatFrequency, heartbeatSocketTimeout, maxConnectionIdleTime, maxConnectionLifeTime, minConnectionsPerHost, requiredReplicaSetName, minHeartbeatFrequency))
      else
        MongoClient(serverList, MongoClientOptions(connectionsPerHost, connectTimeout, cursorFinalizerEnabled, dbDecoderFactory, DBEncoderFactory,
          description, maxWaitTime, readPreference, socketFactory, socketKeepAlive, socketTimeout, threadsAllowedToBlockForConnectionMultiplier, writeConcern, alwaysUseMBeans,
          heartbeatConnectTimeout, heartbeatFrequency, heartbeatSocketTimeout, maxConnectionIdleTime, maxConnectionLifeTime, minConnectionsPerHost, requiredReplicaSetName, minHeartbeatFrequency))

    } else {
      if (credential != null) MongoClient(serverList, List(credential))
      else MongoClient(serverList)
    }
  }

  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val sinkMap = schemaMap.map { case (name, (index, umsType, nullable)) =>
      if (name == UmsSysField.OP.toString) (UmsSysField.ACTIVE.toString, (index, UmsFieldType.INT, nullable))
      else (name, (index, umsType, nullable))
    }.toMap
    val namespace: UmsNamespace = UmsNamespace(sinkNamespace)
    val mongoClient = getMongoClient(namespace, sinkProcessConfig, connectionConfig)
    val db: String = namespace.database
    val table: String = namespace.table
    val database: MongoDB = mongoClient.getDB(db)
    val collection: MongoCollection = database(table)
    val targetSchemaStr = sinkProcessConfig.jsonSchema.get
    val targetSchemaArr = JSON.parseObject(targetSchemaStr).getJSONArray("fields")
    val keys = sinkProcessConfig.tableKeyList
    val sinkSpecificConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        JsonUtils.json2caseClass[MongoConfig](sinkProcessConfig.specialConfig.get)
      else MongoConfig()
    var allCount = 0
    var errorFlag = false
    try {
      SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`) match {
        case INSERT_ONLY =>
          logger.info("INSERT_ONLY: " + sinkSpecificConfig.`mutation_type.get`)
          tupleList.foreach(tuple => {
            val result: JSONObject = JsonParseHelper.jsonObjHelper(tuple, sinkMap, targetSchemaArr)
            val _id: String = _IDHelper.get_Ids(tuple, sinkSpecificConfig.`_id.get`, schemaMap)
            save2MongoByI(result, targetSchemaArr, collection, _id)
          })
        case _ =>
          logger.info("iud: " + sinkSpecificConfig.`mutation_type.get`)
          tupleList.foreach(tuple => {
            val result: JSONObject = JsonParseHelper.jsonObjHelper(tuple, sinkMap, targetSchemaArr)
            val _id: String = _IDHelper.get_Ids(tuple, sinkSpecificConfig.`_id.get`, schemaMap)
            save2MongoByIud(result, targetSchemaArr, collection, keys, _id)
          })
      }
    } catch {
      case e: Throwable =>
        logger.error("mongo json insert or update error", e)
        allCount += tupleList.size
        errorFlag = true
    } finally mongoClient.close()

    if(errorFlag)throw new Exception("du json mongodb sink has error,count="+allCount)

  }

  private def constructBuilder(jsonData: JSONObject, subField: JSONObject, builder: mutable.Builder[(String, Any), Imports.DBObject]): Unit = {
    val name = subField.getString("name")
    val dataType = subField.getString("type")
    if (dataType == "jsonobject") {
      val jsonContent = if (jsonData.containsKey(name)) jsonData.getJSONObject(name) else null
      val subContent = constructBuilder(jsonContent, subField.getJSONArray("sub_fields"))
      builder += name -> subContent
    } else {
      if (dataType == "jsonarray") {
        val jsonArray = if (jsonData.containsKey(name)) jsonData.getJSONArray(name) else null
        val list = MongoDBList
        val toUpsert = ListBuffer.empty[Imports.DBObject]

        if (jsonArray == null) {
          toUpsert.append(null)
        }
        else {

          val jsonArraySubFields = subField.getJSONArray("sub_fields")
          val dataSize = jsonArray.size()
          val schemaSize = jsonArraySubFields.size()


          for (i <- 0 until dataSize) {
            val subBuilder = MongoDBObject.newBuilder
            for (j <- 0 until schemaSize) {
              if (jsonArray == null) {
                constructBuilder(null, jsonArraySubFields.getJSONObject(j), subBuilder)
              }
              else {
                constructBuilder(jsonArray.getJSONObject(i), jsonArraySubFields.getJSONObject(j), subBuilder)
              }

            }
            toUpsert.append(subBuilder.result())
          }
        }
        builder += name -> list(toUpsert: _*)
      } else if (dataType.endsWith("array")) {
        val jsonArray = if (jsonData.containsKey(name)) jsonData.getJSONArray(name) else null
        val toUpsert = ListBuffer.empty[Any]
        if (jsonArray == null) {
          toUpsert.append(null)
        } else {
          val size = jsonArray.size()
          for (i <- 0 until size) {
            toUpsert.append(jsonArray.get(i))
          }
        }
        builder += name -> toUpsert
      } else {
        builder += name -> JsonParseHelper.parseData2CorrectType(UmsFieldType.umsFieldType(dataType), jsonData.getString(name), name)._2
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

  private def save2MongoByIud(jsonData: JSONObject, subFields: JSONArray, collection: MongoCollection, keys: Seq[String], _id: String) = {
    val result: casbah.commons.Imports.DBObject = constructBuilder(jsonData: JSONObject, subFields: JSONArray)
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> _id
    val condition = builder.result
    val findResult = collection.findOne(condition)
    if (findResult.isDefined) {
      val umsIdInMongo = findResult.get.get(UmsSysField.ID.toString).toString.toLong
      val umsIdInStream = result.get(UmsSysField.ID.toString).toString.toLong
      if (umsIdInStream > umsIdInMongo) collection.save(result)
    } else {
      collection.save(result)
    }
  }

  private def save2MongoByI(jsonData: JSONObject, subFields: JSONArray, collection: MongoCollection, _id: String) = {
    val result: casbah.commons.Imports.DBObject = constructBuilder(jsonData: JSONObject, subFields: JSONArray)
    result.put("_id", _id)
    collection.insert(result)
  }
}
