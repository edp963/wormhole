package edp.wormhole.sinks.jsonsink.mongojsonsink

import javax.net.SocketFactory

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.mongodb.casbah._
import com.mongodb.casbah.commons.{Imports, MongoDBList, MongoDBObject}
import com.mongodb.{ReadPreference, WriteConcern, casbah}
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.util.JsonUtils.json2caseClass
import edp.wormhole.sinks.SourceMutationType.INSERT_ONLY
import edp.wormhole.sinks.jsonsink.JsonParseHelper
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsSysField}
import org.mongodb.scala.{MongoCredential, ServerAddress}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataJson2MongoSink extends SinkProcessor with EdpLogging {
  def getMongoClient(namespace: UmsNamespace, sinkProcessConfig: SinkProcessConfig, connectionConfig: ConnectionConfig): MongoClient = {
    val db: String = namespace.database
    val user = connectionConfig.username.getOrElse("")
    val password = connectionConfig.password.getOrElse("")

    val kvConfig = connectionConfig.parameters
    val credential = MongoCredential.createCredential(user, db, password.toCharArray)
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
      MongoClient(serverList, List(credential), MongoClientOptions(connectionsPerHost, connectTimeout, cursorFinalizerEnabled, dbDecoderFactory, DBEncoderFactory,
        description, maxWaitTime, readPreference, socketFactory, socketKeepAlive, socketTimeout, threadsAllowedToBlockForConnectionMultiplier, writeConcern, alwaysUseMBeans,
        heartbeatConnectTimeout, heartbeatFrequency, heartbeatSocketTimeout, maxConnectionIdleTime, maxConnectionLifeTime, minConnectionsPerHost, requiredReplicaSetName, minHeartbeatFrequency))
    } else MongoClient(serverList)
  }

  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
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
    val sinkSpecificConfig = json2caseClass[MongoJsonConfig](sinkProcessConfig.specialConfig.get)
    try {
      SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`) match {
        case INSERT_ONLY =>
          logInfo("INSERT_ONLY: " + sinkSpecificConfig.`mutation_type.get`)
          tupleList.foreach(tuple => {
            val result: JSONObject = JsonParseHelper.jsonObjHelper(tuple, sinkMap, targetSchemaArr)
            save2MongoByI(result, targetSchemaArr, collection)
          })
        case _ =>
          logInfo("iud: " + sinkSpecificConfig.`mutation_type.get`)
          tupleList.foreach(tuple => {
            val result: JSONObject = JsonParseHelper.jsonObjHelper(tuple, sinkMap, targetSchemaArr)
            save2MongoByIud(result, targetSchemaArr, collection, keys)
          })
      }
    } catch {
      case e: Throwable =>
        logError("mongo json insert or update error", e)
    } finally mongoClient.close()

  }

  private def constructBuilder(jsonData: JSONObject, subField: JSONObject, builder: mutable.Builder[(String, Any), Imports.DBObject]): Unit = {
    val name = subField.getString("name")
    val dataType = subField.getString("type")
    if (dataType == "jsonobject") {
      val jsonContent = jsonData.getJSONObject(name)
      val subContent = constructBuilder(jsonContent, subField.getJSONArray("sub_fields"))
      builder += name -> subContent
    } else {
      if (dataType == "jsonarray") {
        val jsonArray = jsonData.getJSONArray(name)
        val list = MongoDBList

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
          val jsonArray = jsonData.getJSONArray(name)
          val toUpsert = ListBuffer.empty[Any]
          val size = jsonArray.size()
          for (i <- 0 until size) {
            toUpsert.append(jsonArray.get(i))
          }
          builder += name -> toUpsert
      } else {
       builder += name -> JsonParseHelper.parseData2CorrectType(dataType, jsonData.getString(name))
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
    // builder += "_id" -> 123
    builder.result()
  }

  private def save2MongoByIud(jsonData: JSONObject, subFields: JSONArray, collection: MongoCollection, keys: Seq[String]) = {
    val result: casbah.commons.Imports.DBObject = constructBuilder(jsonData: JSONObject, subFields: JSONArray)
    val builder = MongoDBObject.newBuilder
    keys.foreach(key => {
      builder += key -> result.get(key)
    })
    val condition = builder.result
    val findResult = collection.findOne(condition)
    if (findResult.isDefined) {
      val umsIdInMongo = findResult.get.get(UmsSysField.ID.toString).toString.toLong
      val _idInMongo = findResult.get.get("_id")
      val umsIdInStream = result.get(UmsSysField.ID.toString).toString.toLong
      if (umsIdInStream > umsIdInMongo) {
        result.put("_id",_idInMongo)
        collection.save(result)
      }
    } else {
      collection.save(result)
    }
  }

  private def save2MongoByI(jsonData: JSONObject, subFields: JSONArray, collection: MongoCollection) = {
    val result: casbah.commons.Imports.DBObject = constructBuilder(jsonData: JSONObject, subFields: JSONArray)
    collection.insert(result)
  }
}
