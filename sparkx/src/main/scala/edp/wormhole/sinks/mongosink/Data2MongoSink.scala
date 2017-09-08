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

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.mongodb.ConnectionString
import com.mongodb.async.client.MongoClients
import com.mongodb.client.model.IndexOptions
import edp.wormhole.common.ConnectionConfig
import edp.wormhole.memorystorage.ConfMemoryStorage._
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.sinks.mongosink.MongoHelper._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sinks.SinkProcessor
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType.{apply => _, _}
import edp.wormhole.ums.{UmsFieldType, _}
import org.mongodb.scala.bson.{BsonBinary, BsonBoolean, BsonDateTime, BsonDouble, BsonInt32, BsonInt64, BsonString}
import org.mongodb.scala.connection._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoCollection, MongoDatabase, bson, _}

import scala.collection.mutable.{Builder, ListBuffer}

class Data2MongoSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig:ConnectionConfig): Unit = {

    val start1 = System.currentTimeMillis()
    //      val sinkProcessConfig = sinkConfigMap(sinkNamespace)
    //oracle.oracle01.db.collection.2.0.0
    val namespace = UmsNamespace(sinkNamespace)
    val db: String = namespace.database
    val table: String = namespace.table
    val authentication = sinkProcessConfig.specialConfig.get
    var user: String = null
    var password: String = null
    if (authentication.length != 0) {
      val json = JSON.parseObject(authentication)
      user = json.getString("user")
      password = json.getString("password")
    }
    //val connectionConfig = getDataStoreConnectionsMap(sinkNamespace)
    val kvConfig=connectionConfig.parameters.get
    val kvList =if (kvConfig.nonEmpty){
      kvConfig.map(kv=>
        kv.key+"="+kv.value)
    }
    else Nil
    val connectionUrl=connectionConfig.connectionUrl+"/?"+kvList.mkString("&")
    //      val connectionUrl = sinkProcessConfig.    mongodb://localhost:27017
    val credential = MongoCredential.createCredential(user, db, password.toCharArray)
    val settings: MongoClientSettings = MongoClientSettings.builder()
      .clusterSettings(ClusterSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .connectionPoolSettings(ConnectionPoolSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .serverSettings(ServerSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .heartbeatSocketSettings(SocketSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .socketSettings(SocketSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .sslSettings(SslSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .credentialList(util.Arrays.asList(credential)).build()
    val mongoClient: MongoClient = MongoClient(MongoClients.create(settings))
    // get handle to "mydb" database
    val database: MongoDatabase = mongoClient.getDatabase(db)
    val collection: MongoCollection[Document] = database.getCollection(table)
    val keys = sinkProcessConfig.tableKeyList
    val indexBuilder = Document.builder
    keys.foreach { key =>
      indexBuilder += key -> BsonInt32(1)
    }
    val index = indexBuilder.result()
    collection.createIndex(index, new IndexOptions().unique(true)).headResult()
    indexBuilder.clear()
    println("get connection time : " + (System.currentTimeMillis() - start1))
    val insertDocuments = ListBuffer[Document]()
    tupleList.foreach { payload =>
      val document = getDocument(schemaMap, payload)
      val keyFilter = and(keys.map(key => equal(key, document.get(key).get)): _*)
      if (collection.count(keyFilter).headResult() == 0L) {
        insertDocuments += document
      } else {
        val updateFilter = and(keyFilter, lt("ums_id_", document.get("ums_id_").get))
        collection.replaceOne(updateFilter, document).printHeadResult("replace result:  ")
      }
    }
    collection.insertMany(insertDocuments).printHeadResult("-----> success <-----")
    //collection.dropIndex(index).printHeadResult("-----> drop index <-----")
    mongoClient.close()

  }

  def getDocument(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], payload: Seq[String]): Document = {
    val builder = Document.builder
    val columns = schemaMap.keys
    for (column <- columns) {
      if (column == UmsSysField.OP.toString) {
        if (payload(schemaMap(column)._1) == UmsOpType.DELETE.toString) {
          builderdWithDifferentTypes(builder, UmsSysField.ACTIVE.toString, UmsFieldType.INT, UmsActiveType.INACTIVE.toString)
        } else {
          builderdWithDifferentTypes(builder, UmsSysField.ACTIVE.toString, UmsFieldType.INT, UmsActiveType.ACTIVE.toString)
        }
      } else {
        builderdWithDifferentTypes(builder, column, schemaMap(column)._2, payload(schemaMap(column)._1))
      }
    }
    builder.result()
  }

  def builderdWithDifferentTypes(builder: Builder[(String, bson.BsonValue), Document], columnName: String, fieldType: UmsFieldType, value: String): Unit = fieldType match {
    //mongoDB ISODate time zone 8 hours apart
    case UmsFieldType.STRING => builder += columnName -> BsonString(value.trim)
    case UmsFieldType.INT => builder += columnName -> BsonInt32(value.trim.toInt)
    case UmsFieldType.LONG => builder += columnName -> BsonInt64(value.trim.toLong)
    case UmsFieldType.FLOAT | UmsFieldType.DOUBLE | UmsFieldType.DECIMAL => builder += columnName -> BsonDouble(value.trim.toFloat)
    case UmsFieldType.BOOLEAN => builder += columnName -> BsonBoolean(value.trim.toBoolean)
    case UmsFieldType.DATE => builder += columnName -> BsonDateTime(new SimpleDateFormat("yyyy-MM-dd").parse(value.trim))
    case UmsFieldType.DATETIME => builder += columnName -> BsonDateTime(new SimpleDateFormat("yyyy-MM-dd").parse(value.trim))
    case UmsFieldType.BINARY => builder += columnName -> BsonBinary(value.trim.getBytes())
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
  }
}
