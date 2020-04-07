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

package edp.wormhole.dbdriver.redis

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisShardInfo, ShardedJedis, ShardedJedisPool}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SharedJedisConnection extends Serializable{

  def createPool(hosts: Array[(String, Int)],password:Option[String]): ShardedJedisPool = {
    import collection.JavaConversions._
    val shards = ListBuffer.empty[JedisShardInfo]
    hosts.foreach(host => {
      val info = new JedisShardInfo(host._1, host._2)
      if(password.nonEmpty&&password.get.nonEmpty) info.setPassword(password.get)
      shards += info
    })
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(1)
    poolConfig.setMaxTotal(3)

    new ShardedJedisPool(poolConfig, shards)
  }

  def getJedis(pool:ShardedJedisPool): ShardedJedis ={
    pool.getResource
  }

  def get(jedis:ShardedJedis,key:String): String ={
    jedis.get(key)
  }

  def set(jedis:ShardedJedis, key: String, value: String):String ={
    jedis.set(key, value)
  }

  def expire(jedis:ShardedJedis, key: String, nSeconds: Int ) :Long={
    jedis.expire(key, nSeconds)
  }

  def del(jedis:ShardedJedis, key: String) = {
    jedis.del(key)
  }

  def hGet(jedis:ShardedJedis,key:String,filed:String): String ={
    jedis.hget(key,filed)
  }

  def hGetAll(jedis:ShardedJedis,key:String):mutable.Map[String, String] ={
    jedis.hgetAll(key).asScala
  }

  def hSet(jedis:ShardedJedis, key:String, field:String, value:String): Unit ={
    jedis.hset(key,field,value)
  }

  def mGet(jedis:ShardedJedis, key:String, fields:Seq[String]): mutable.Seq[String] ={
    jedis.hmget(key,fields: _*).asScala
  }

  def mSet(jedis:ShardedJedis,key:String,hash:Map[String,String]): Unit ={
    jedis.hmset(key,hash.asJava)
  }
}
