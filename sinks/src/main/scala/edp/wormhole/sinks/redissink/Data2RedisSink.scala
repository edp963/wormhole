package edp.wormhole.sinks.redissink

import edp.wormhole.dbdriver.redis.{JedisConnection, RedisMode}
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.dbdriver.redis.RedisMode.RedisMode
import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import edp.wormhole.util.DateUtils.{currentyyyyMMddHHmmss, dt2dateTime}
import edp.wormhole.util.JsonUtils.json2caseClass
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import org.apache.log4j.Logger
import org.joda.time.{DateTime, Seconds}

import collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class Data2RedisSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String, sinkNamespace: String, sinkProcessConfig: SinkProcessConfig, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tupleList: Seq[Seq[String]], connectionConfig: ConnectionConfig): Unit = {
    logger.info(s"process KafkaLog2RedisSnapshot, size is ${tupleList.size}")
    logger.info("KafkaLog2RedisSnapshot sink config: " + sinkProcessConfig)
    val dt1: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    val sinkSpecialConfig =
      if (sinkProcessConfig.specialConfig.isDefined) {
        json2caseClass[RedisConfig](sinkProcessConfig.specialConfig.get)
      } else RedisConfig()

    val namespace = UmsNamespace(sinkNamespace)
    val sinkTableAsRedisKey = namespace.database + "_" + namespace.table
    val errorList = if (sinkSpecialConfig.`mutation_type.get` == SourceMutationType.INSERT_ONLY.toString) {
      insertOnlyMode(sinkSpecialConfig, sinkProcessConfig.tableKeyList, schemaMap, tupleList, connectionConfig, sinkTableAsRedisKey)
    } else {
      otherMode(sinkSpecialConfig, sinkProcessConfig.tableKeyList, schemaMap, tupleList, connectionConfig, sinkTableAsRedisKey)
    }
    val dt2: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    logger.info("sink redis duration:   " + dt2 + " - " + dt1 + " = " + (Seconds.secondsBetween(dt1, dt2).getSeconds % 60 + " seconds."))
    if (errorList.nonEmpty)
      throw new Exception("some data error ,records count is " + errorList.size)
  }

  private def insertOnlyMode(redisConfig: RedisConfig, tableKeyList: List[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tupleList: Seq[Seq[String]], connectionConfig: ConnectionConfig, sinkTableAsRedisKey: String) = {
    val errorList = ListBuffer.empty[Seq[String]]
    getRedisMode(connectionConfig.parameters) match {
      case RedisMode.CLUSTER =>
        val clusterConnection = JedisConnection.getClusterConnection(connectionConfig.connectionUrl, connectionConfig.password)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val hash = tuple2Map(schemaMap, tuple)
            clusterConnection.sadd(sinkTableAsRedisKey, tableKeys)
            clusterConnection.hmset(tableKeys, hash)
            if (redisConfig.expireTimeInSeconds > 0) {
              clusterConnection.expire(tableKeys, redisConfig.expireTimeInSeconds)
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        clusterConnection.close()
      case RedisMode.STANDALONE =>
        val shardedJedis = JedisConnection.getSharedJedisConnection(connectionConfig.connectionUrl, connectionConfig.password)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val hash = tuple2Map(schemaMap, tuple)
            shardedJedis.sadd(sinkTableAsRedisKey, tableKeys)
            shardedJedis.hmset(tableKeys, hash)
            if (redisConfig.expireTimeInSeconds > 0) {
              shardedJedis.expire(tableKeys, redisConfig.expireTimeInSeconds)
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        shardedJedis.close()
      case RedisMode.SENTINEL =>
        val masterName = getSentinelMasterName(connectionConfig.parameters)
        val jedis = JedisConnection.getJedisSentinelConnection(connectionConfig.connectionUrl, connectionConfig.password, masterName)
        val databaseNo = getSentinelDataBaseNo(connectionConfig.parameters)
        jedis.select(databaseNo)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val hash = tuple2Map(schemaMap, tuple)
            jedis.sadd(sinkTableAsRedisKey, tableKeys)
            jedis.hmset(tableKeys, hash)
            if (redisConfig.expireTimeInSeconds > 0) {
              jedis.expire(tableKeys, redisConfig.expireTimeInSeconds)
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        jedis.close()
    }
    errorList
  }

  private def otherMode(redisConfig: RedisConfig, tableKeyList: List[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tupleList: Seq[Seq[String]], connectionConfig: ConnectionConfig, sinkTableAsRedisKey: String) = {
    val errorList = ListBuffer.empty[Seq[String]]

    getRedisMode(connectionConfig.parameters) match {
      case RedisMode.CLUSTER =>
        val clusterConnection = JedisConnection.getClusterConnection(connectionConfig.connectionUrl, connectionConfig.password)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val idGetFromRedis = clusterConnection.hget(tableKeys, UmsSysField.ID.toString)
            val umsIdInTuple = SinkCommonUtils.fieldValue(UmsSysField.ID.toString, schemaMap, tuple).toString
            if (idGetFromRedis == null || idGetFromRedis < umsIdInTuple) {
              val hash = tuple2Map(schemaMap, tuple)
              clusterConnection.sadd(sinkTableAsRedisKey, tableKeys)
              clusterConnection.hmset(tableKeys, hash)
              if (redisConfig.expireTimeInSeconds > 0) {
                clusterConnection.expire(tableKeys, redisConfig.expireTimeInSeconds)
              }
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        clusterConnection.close()
      case RedisMode.STANDALONE =>
        val shardedJedis = JedisConnection.getSharedJedisConnection(connectionConfig.connectionUrl, connectionConfig.password)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val getFromRedis = shardedJedis.hget(tableKeys, UmsSysField.ID.toString)
            val umsIdInTuple = SinkCommonUtils.fieldValue(UmsSysField.ID.toString, schemaMap, tuple).toString
            if (getFromRedis == null || getFromRedis < umsIdInTuple) {
              val hash = tuple2Map(schemaMap, tuple)
              shardedJedis.sadd(sinkTableAsRedisKey, tableKeys)
              shardedJedis.hmset(tableKeys, hash)
              if (redisConfig.expireTimeInSeconds > 0) {
                shardedJedis.expire(tableKeys, redisConfig.expireTimeInSeconds)
              }
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        shardedJedis.close()
      case RedisMode.SENTINEL =>
        val masterName = getSentinelMasterName(connectionConfig.parameters)
        val jedis = JedisConnection.getJedisSentinelConnection(connectionConfig.connectionUrl, connectionConfig.password, masterName)
        val databaseNo = getSentinelDataBaseNo(connectionConfig.parameters)
        jedis.select(databaseNo)
        tupleList.foreach(tuple => {
          try {
            val tableKeys = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, tuple)
            val getFromRedis = jedis.hget(tableKeys, UmsSysField.ID.toString)
            val umsIdInTuple = SinkCommonUtils.fieldValue(UmsSysField.ID.toString, schemaMap, tuple).toString
            if (getFromRedis == null || getFromRedis < umsIdInTuple) {
              val hash = tuple2Map(schemaMap, tuple)
              jedis.sadd(sinkTableAsRedisKey, tableKeys)
              jedis.hmset(tableKeys, hash)
              if (redisConfig.expireTimeInSeconds > 0) {
                jedis.expire(tableKeys, redisConfig.expireTimeInSeconds)
              }
            }
          } catch {
            case e: Exception => logger.error("sink 2 redis error ", e)
              errorList += tuple
          }
        })
        jedis.close()
    }
    errorList
  }

  private def tuple2Map(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tuple: Seq[String]): collection.Map[String, String] = {
    schemaMap.map(fieldSchema => {
      val value: String = tuple(fieldSchema._2._1)
      if (fieldSchema._1 == UmsSysField.OP.toString) {
        if (value == "d") {
          (UmsSysField.ACTIVE.toString, "-1")
        } else {
          (UmsSysField.ACTIVE.toString, "1")
        }
      } else {
        (fieldSchema._1, value)
      }
    })
  }

  private def getRedisMode(parameters: Option[Seq[KVConfig]]): RedisMode = {
    var redisMode: RedisMode = RedisMode.STANDALONE
    if (parameters.isDefined) {
      val kvPairs = parameters.get
      kvPairs.foreach(kv => {
        if (kv.key == "mode")
          redisMode = RedisMode.redisMode(kv.value)
      })
    }
    redisMode
  }

  private def getSentinelMasterName(parameters: Option[Seq[KVConfig]]): String = {
    var masterName: String = null
    if (parameters.isDefined) {
      val kvPairs = parameters.get
      kvPairs.foreach(kv => {
        if (kv.key == "masterName")
          masterName = kv.value
      })
    }
    masterName
  }

  private def getSentinelDataBaseNo(parameters: Option[Seq[KVConfig]]): Int = {
    var databaseNo: Int = 0
    if (parameters.isDefined) {
      val kvPairs = parameters.get
      kvPairs.foreach(kv => {
        if (kv.key == "databaseNo")
          databaseNo = kv.value.toInt
      })
    }
    databaseNo
  }
}
