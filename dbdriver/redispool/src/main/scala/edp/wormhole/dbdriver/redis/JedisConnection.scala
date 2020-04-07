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

import redis.clients.jedis.{JedisCluster, ShardedJedis, ShardedJedisPool}

import scala.collection.mutable
import scala.collection.JavaConverters._

object JedisConnection extends Serializable {

  lazy val CLUSTER_MODE = "cluster"

  val shardedPoolMap: mutable.HashMap[String, ShardedJedisPool] = new mutable.HashMap[String, ShardedJedisPool]

  val clusterPoolMap: mutable.HashMap[String, JedisCluster] = new mutable.HashMap[String, JedisCluster]

  private def createJedisPool(url: String, password: Option[String], mode: String): Unit = {
    val hosts: Array[(String, Int)] = {
      url.split(",").map(host => {
        val ip2port = host.split(":")
        (ip2port(0), ip2port(1).toInt)
      })
    }
    if (mode == CLUSTER_MODE) {
      synchronized {
        if (!clusterPoolMap.contains(url)) clusterPoolMap(url) = JedisClusterConnection.createPool(hosts, password)
      }
    } else {
      synchronized {
        if (!shardedPoolMap.contains(url)) shardedPoolMap(url) = SharedJedisConnection.createPool(hosts, password)
      }
    }
  }

  def getConnection(url: String, password: Option[String]): ShardedJedis = {
    if (!shardedPoolMap.contains(url)) createJedisPool(url, password, "")
    val j = shardedPoolMap(url)
    SharedJedisConnection.getJedis(j)
  }

  def getClusterConnection(url: String, password: Option[String]): JedisCluster = {
    if (!clusterPoolMap.contains(url)) createJedisPool(url, password, "cluster")
    clusterPoolMap(url)
  }


  def get(url: String, password: Option[String], mode: String, key: String): String = {
    var value: String = null
    if (mode == CLUSTER_MODE) {
      val j: JedisCluster = getClusterConnection(url,password)
      value = j.get(key)
      j.close()
    } else {
      val shardedJedis = getConnection(url, password)
      value = shardedJedis.get(key)
      shardedJedis.close()
    }
    value
  }

  def set(url: String, password: Option[String], mode: String, key: String, value: String): String = {
    var value: String = null;
    if (mode == CLUSTER_MODE) {
      val jedisCluster = getClusterConnection(url, password)
      value = jedisCluster.set(key, value)
      jedisCluster.close()

    } else {
      val shardedJedis = getConnection(url, password)
      value = shardedJedis.set(key, value)
      shardedJedis.close()
    }
    value
  }

  def expire(url: String, password: Option[String], mode: String, key: String, nSeconds: Int): Long = {
    if (mode == CLUSTER_MODE) {
      -1L
    } else {
      val shardedJedis = getConnection(url, password)
      val value = shardedJedis.expire(key, nSeconds)
      shardedJedis.close()
      value
    }
  }

  def del(url: String, password: Option[String], mode: String, key: String): Long = {
    if (mode == CLUSTER_MODE) {
      -1L
    } else {
      val shardedJedis = getConnection(url, password)
      val value = shardedJedis.del(key)
      shardedJedis.close()
      value
    }
  }

  def mGet(url: String, password: Option[String], mode: String, key: String,field:Seq[String]): Seq[String] = {
    import collection.JavaConversions._
    if (mode == CLUSTER_MODE) {
      val j =  getClusterConnection(url, password)
      j.mget(field: _*)
    } else {
      val shardedJedis = getConnection(url, password)
      val value = shardedJedis.hmget( key,field: _*)
      shardedJedis.close()
      value
    }
  }

  def mSet(url: String, password: Option[String], mode: String, key: String, hash: Map[String,String]): String = {
    if (mode == CLUSTER_MODE) {
      val jedisCluster = getClusterConnection(url, password)
      val value = jedisCluster.hmset(key, hash.asJava)
      jedisCluster.close()
      value
    } else {
      val shardedJedis = getConnection(url, password)
      val value = shardedJedis.hmset(key, hash.asJava)
      shardedJedis.close()
      value
    }
  }

}
