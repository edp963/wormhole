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

package edp.wormhole.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{ JedisShardInfo, ShardedJedis, ShardedJedisPool}

import scala.collection.mutable.ListBuffer

object SharedJedisConnection extends Serializable{

  def createPool(hosts: Array[(String, Int)],password:Option[String]): ShardedJedisPool = {
    import collection.JavaConversions._
    val shards = ListBuffer.empty[JedisShardInfo]
    hosts.foreach(host => {
      val info = new JedisShardInfo(host._1, host._2)
      if(password.nonEmpty) info.setPassword(password.get)
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
    val value = jedis.get(key)
    jedis.close()
    value
  }

//  def closeResource(jedis:ShardedJedis): Unit ={
//    jedis.close()
//  }

}
