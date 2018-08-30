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

import redis.clients.jedis.{JedisCluster, ShardedJedisPool}

import scala.collection.mutable

object JedisConnection extends Serializable {

  val shardedPoolMap: mutable.HashMap[String, ShardedJedisPool] = new mutable.HashMap[String, ShardedJedisPool]

  val clusterPoolMap: mutable.HashMap[String, JedisCluster] = new mutable.HashMap[String, JedisCluster]

  private def createJedisPool(url: String, password: Option[String], mode: String): Unit = {
    val hosts: Array[(String, Int)] = {
      url.split(",").map(host => {
        val ip2port = host.split(":")
        (ip2port(0), ip2port(1).toInt)
      })
    }
    if (mode == "cluster") {
      synchronized {
        if (!clusterPoolMap.contains(url)) clusterPoolMap(url) = JedisClusterConnection.createPool(hosts, password)
      }
    } else {
      synchronized {
        if (!shardedPoolMap.contains(url)) shardedPoolMap(url) = SharedJedisConnection.createPool(hosts, password)
      }
    }
  }

  def get(url: String, password: Option[String], mode: String, key: String): String = {
    if (mode == "cluster") {
      if (!clusterPoolMap.contains(url)) createJedisPool(url, password, mode)
      val j = clusterPoolMap(url)
      JedisClusterConnection.get(j, key)
    } else {
      if (!shardedPoolMap.contains(url)) createJedisPool(url, password, mode)
      val j = shardedPoolMap(url)
      SharedJedisConnection.get(SharedJedisConnection.getJedis(j),key)
    }
  }

  def set(url: String, password: Option[String], mode: String, key: String, value: String ):String = {
    if (mode != "cluster") {
      if (!shardedPoolMap.contains(url)) createJedisPool(url, password, mode)
      val j = shardedPoolMap(url)
      SharedJedisConnection.set(SharedJedisConnection.getJedis(j),key, value)
    }else ""
  }

  def expire(url: String, password: Option[String], mode: String, key: String, nSeconds: Int) :Long = {
    if (mode != "cluster") {
      if (!shardedPoolMap.contains(url)) createJedisPool(url, password, mode)
      val j = shardedPoolMap(url)
      SharedJedisConnection.expire(SharedJedisConnection.getJedis(j),key,nSeconds )
    }else -1L

  }

  def del(url: String, password: Option[String], mode: String, key: String):Long = {
    if (mode != "cluster") {
      if (!shardedPoolMap.contains(url)) createJedisPool(url, password, mode)
      val j = shardedPoolMap(url)
      SharedJedisConnection.del(SharedJedisConnection.getJedis(j),key )
    }else -1L
  }

}
