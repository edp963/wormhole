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
import java.util.UUID

import edp.wormhole.sinks.mongosink.MongoHelper._
import com.mongodb.ConnectionString
import com.mongodb.async.client.MongoClients
import edp.wormhole.common.util.JsonUtils.json2caseClass
import edp.wormhole.common.{ConnectionConfig, KVConfig}
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsFieldType, _}
import org.mongodb.scala.bson.{BsonBinary, BsonBoolean, BsonDateTime, BsonDouble, BsonInt32, BsonInt64, BsonString, BsonValue}
import org.mongodb.scala.connection._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.FindOneAndReplaceOptions
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoCollection, MongoDatabase, bson, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data2MongoSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    val namespace = UmsNamespace(sinkNamespace)
    val db: String = namespace.database
    val table: String = namespace.table
    val mongoClient: MongoClient = getMongoClient(db, connectionConfig)
    try {
      val database: MongoDatabase = mongoClient.getDatabase(db)
      val collection: MongoCollection[Document] = database.getCollection(table)
//      val keys = sinkProcessConfig.tableKeyList

      val sinkSpecificConfig = json2caseClass[MongoConfig](sinkProcessConfig.specialConfig.get)
      //if (sinkSpecificConfig.`mutation_type.get` == SourceMutationType.I_U_D.toString) {
      if (sinkSpecificConfig._id.nonEmpty&&sinkSpecificConfig._id.get.nonEmpty){
        tupleList.foreach(payload => {
          val builder = getDocument(schemaMap, payload)
          try {
            val keyFilter = {
              val f = sinkSpecificConfig._id.get.split(",").map(keyname=>{
                payload(schemaMap(keyname)._1)
              }).mkString("_")
              builder += "_id" -> BsonString(f)
              and(equal("_id", f))
            }
            val umsidInTuple = payload(schemaMap(UmsSysField.ID.toString)._1).toLong
            val updateFilter = and(keyFilter, gte(UmsSysField.ID.toString, umsidInTuple))
            val count: Long = collection.count(updateFilter).headResult()
            if(count==0){
              val op: FindOneAndReplaceOptions = FindOneAndReplaceOptions().upsert(true)
              collection.findOneAndReplace(keyFilter, builder.result(), op).results()
            }
          } catch {
            case e: Throwable =>
              logError("findOneAndReplace error,document:"+builder, e)
          }
        })
      } else {
        val insertDocuments = ListBuffer[Document]()
        tupleList.foreach(payload => {
          val builder = getDocument(schemaMap, payload)
//          if ( keys.nonEmpty) {
//            val f = keys.map(keyname=>{
//              payload(schemaMap(keyname)._1)
//            }).mkString("_")
//            builder += "_id" -> BsonString(f)
//          }
          builder += "_id" -> BsonString(UUID.randomUUID().toString)
          insertDocuments += builder.result()
        })
        if (insertDocuments.nonEmpty) {
          try {
            collection.insertMany(insertDocuments).results()
            logInfo("-----> insert only success <-----")
          } catch {
            case e: Throwable =>
              logError("batch insert error, change to insert one by one", e)
              insertDocuments.foreach(doc => {
                try {
                  collection.insertOne(doc).results()
                } catch {
                  case e: Throwable =>
                    logError("insert error,document:" + doc, e)
                }
              })
          }
        }
      }

    }

    catch {
      case e: Throwable => logError("", e)
    }
    finally mongoClient.close()

  }

  def getDocument(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], payload: Seq[String]): mutable.Builder[(String, BsonValue), Document] = {
    val builder: mutable.Builder[(String, BsonValue), Document] = Document.builder
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
    builder
  }

  def builderdWithDifferentTypes(builder: mutable.Builder[(String, bson.BsonValue), Document], columnName: String, fieldType: UmsFieldType, value: String): Unit = fieldType match {
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

  def getMongoClient(db: String, connectionConfig: ConnectionConfig): MongoClient = {
    val kvConfig: Seq[KVConfig] = connectionConfig.parameters.get
    val (user, password) = if (connectionConfig.username.nonEmpty && connectionConfig.username.get.nonEmpty) {
      (connectionConfig.username.get, connectionConfig.password.get)
    } else (null, null)

    val kvList = if (kvConfig.nonEmpty) {
      kvConfig.map(kv =>
        kv.key + "=" + kv.value)
    } else Nil
    val connectionUrl = "mongodb://" + connectionConfig.connectionUrl + "/?" + kvList.mkString("&")
    val credential = if (user != null) MongoCredential.createCredential(user, db, password.toCharArray) else null
    val settings = MongoClientSettings.builder()
      .clusterSettings(ClusterSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .connectionPoolSettings(ConnectionPoolSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .serverSettings(ServerSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .heartbeatSocketSettings(SocketSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .socketSettings(SocketSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
      .sslSettings(SslSettings.builder().applyConnectionString(new ConnectionString(connectionUrl)).build())
    if (credential != null) settings.credentialList(util.Arrays.asList(credential))
    MongoClient(MongoClients.create(settings.build()))
  }
}
