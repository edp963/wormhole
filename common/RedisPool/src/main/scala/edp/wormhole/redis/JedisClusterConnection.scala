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

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.mutable

object JedisClusterConnection {

  def createPool(hosts: Array[(String, Int)],password:Option[String]):JedisCluster={
    import collection.JavaConversions._
    val jedisClusterNodes = mutable.HashSet.empty[HostAndPort]
    hosts.foreach(host=>{
      jedisClusterNodes += new HostAndPort(host._1, host._2)
    })
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(1)
    poolConfig.setMaxTotal(3)
    if(password.nonEmpty) new JedisCluster(jedisClusterNodes, 3000, 3000,  3, password.get, poolConfig)
    else new JedisCluster(jedisClusterNodes, 3000, 3000,  3, poolConfig)
  }

  def get(jedisCluster:JedisCluster,key:String): String ={
    val value = jedisCluster.get(key)
    jedisCluster.close()
    value
  }

  def mget(jedisCluster:JedisCluster,keys:Seq[String]): Seq[String] ={
    import collection.JavaConversions._
    val value = jedisCluster.mget(keys:_*)
    jedisCluster.close()
    value
  }

//  def closeResource(jedisCluster:JedisCluster): Unit ={
//    jedisCluster.close()
//  }

}
