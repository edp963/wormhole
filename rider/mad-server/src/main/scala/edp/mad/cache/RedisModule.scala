package edp.mad.cache

import org.apache.log4j.Logger
import edp.mad.module.ConfigObj
import edp.wormhole.redis._
import edp.wormhole.common.util.JsonUtils

trait RedisModule[K<: AnyRef , V<: AnyRef]{
  private val logger = Logger.getLogger(this.getClass)
  val modules = ConfigObj.getModule

  val url = modules.madRedis.url
  var pwd = modules.madRedis.password
  if(pwd == "") pwd = null
  val mode = modules.madRedis.mode
  val expireSeconds = modules.madRedis.expireSeconds

  def set(key: K, value: V) = {
    val kStr = JsonUtils.caseClass2json[K](key)
    val vStr = JsonUtils.caseClass2json[V](value)
    logger.debug(s" ${key}  ${value}  ${kStr}   ${vStr}  \n")
    JedisConnection.set(url, Option(pwd), mode,kStr, vStr)
    JedisConnection.expire(url, Option(pwd), mode, kStr, expireSeconds)
    //println(s" ${key}  ${value}  ${kStr}   ${vStr}  \n")
  }

  def getValueStr( key: K ):String= {
    val kStr = JsonUtils.caseClass2json[K](key)
    val vStr = JedisConnection.get(url, Option(pwd), mode,kStr)
    //println(s" =========1 before JedisConnection get key ${vStr}")
    vStr
  }

  def del(key: K ) ={
    val kStr = JsonUtils.caseClass2json[K](key)
    JedisConnection.del(url, Option(pwd), mode,kStr)
  }

  def expire(key: K, seconds: Int) ={
    val kStr = JsonUtils.caseClass2json[K](key)
    JedisConnection.expire(url, Option(pwd), mode, kStr, seconds)
  }

}
