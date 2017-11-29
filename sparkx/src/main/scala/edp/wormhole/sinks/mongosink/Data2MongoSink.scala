package edp.wormhole.sinks.mongosink

import javax.net.SocketFactory

import com.alibaba.fastjson.JSON
import com.mongodb.{DBObject, ReadPreference, WriteConcern}
import com.mongodb.casbah.{MongoClient, MongoClientOptions, MongoCollection, MongoDB}
import com.mongodb.casbah.commons.{MongoDBObject, TypeImports}
import com.sun.corba.se.spi.ior.ObjectId
import edp.wormhole.common.util.JsonUtils.json2caseClass
import edp.wormhole.common.{ConnectionConfig, KVConfig, RowkeyPatternContent, RowkeyTool}
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.mongodb.scala.{MongoCredential, ServerAddress}

class Data2MongoSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val namespace: UmsNamespace = UmsNamespace(sinkNamespace)
    val mongoClient = getMongoClient(namespace, sinkProcessConfig, connectionConfig)
    try {
      val db: String = namespace.database
      val table: String = namespace.table
      val database: MongoDB = mongoClient.getDB(db)
      val mongoCollection: MongoCollection = database(table)
      val sinkSpecificConfig: MongoConfig = json2caseClass[MongoConfig](sinkProcessConfig.specialConfig.get)
      val patternContentList: Seq[RowkeyPatternContent] = if (sinkSpecificConfig.row_key.nonEmpty && sinkSpecificConfig.row_key.get.nonEmpty) RowkeyTool.parse(sinkSpecificConfig.row_key.get) else null.asInstanceOf[Seq[RowkeyPatternContent]]
      val keySchema: Seq[(Boolean, Int, String)] = if (sinkSpecificConfig.row_key.nonEmpty && sinkSpecificConfig.row_key.get.nonEmpty) RowkeyTool.generateOnerowKeyFieldsSchema(schemaMap, patternContentList) else null.asInstanceOf[Seq[(Boolean, Int, String)]]
      if (sinkSpecificConfig.`es.mutation_type.get` == SourceMutationType.I_U_D.toString) {
        tupleList.foreach(tuple => {
          val keyDatas = RowkeyTool.generateTupleKeyDatas(keySchema, tuple)
          val key = RowkeyTool.generatePatternKey(keyDatas, patternContentList)
          val o: DBObject = MongoDBObject("_id" -> key.asInstanceOf[ObjectId])
          val field = MongoDBObject(UmsSysField.ID.toString)
          val result: Option[TypeImports.DBObject] = mongoCollection.findOne(o, field)
          if (result.nonEmpty) {
            val umsidInStore = result.get.get(UmsSysField.ID.toString).asInstanceOf[Long]
            val umsidInTuple = tuple(schemaMap(UmsSysField.ID.toString)._1).asInstanceOf[Long]
            if (umsidInStore < umsidInTuple) {
              val data = formatData(schemaMap, tuple,  key)
              mongoCollection.save(data)
            }
          } else {
            val data = formatData(schemaMap, tuple,  key)
            mongoCollection.save(data)
          }
        })
      } else {
        val datas = tupleList.map(tuple => {
          val keyDatas = RowkeyTool.generateTupleKeyDatas(keySchema, tuple)
          val key = RowkeyTool.generatePatternKey(keyDatas, patternContentList)
          formatData(schemaMap, tuple,  key)
        })
        mongoCollection.insert(datas: _*)
      }
    } catch {
      case e: Throwable =>
        logError("", e)
    } finally {
      mongoClient.close()
    }
  }

  def formatData(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tuple: Seq[String], key: String): DBObject = {
    val builder = MongoDBObject.newBuilder
    schemaMap.foreach(schema => {
      builder += schema._1 -> tuple(schema._2._1)
    })
    builder += "_id" -> key
    builder.result()
  }

  def getMongoClient(namespace: UmsNamespace, sinkProcessConfig: SinkProcessConfig, connectionConfig: ConnectionConfig): MongoClient = {
    val db: String = namespace.database
    val authentication = sinkProcessConfig.specialConfig.get
    val (user, password) = if (authentication.length != 0) {
      val json = JSON.parseObject(authentication)
      (json.getString("user"), json.getString("password"))
    } else (null, null)

    val kvConfig: Seq[KVConfig] = connectionConfig.parameters.get
    val credential = MongoCredential.createCredential(user, db, password.toCharArray)
    val serverList = connectionConfig.connectionUrl.split(",").map(conn => {
      val ip2port = conn.split("\\:")
      new ServerAddress(ip2port(0), ip2port(1).toInt)
    }).toList

    if (kvConfig.nonEmpty) {
      val configMap: Map[String, String] = kvConfig.map(kv => {
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
    } else MongoClient(serverList, List(credential), MongoClientOptions.Defaults)
  }
}
