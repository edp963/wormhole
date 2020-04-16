package edp.wormhole.dbdriver.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisSentinelPool}

import scala.collection.mutable

object JedisSentinelConnection {
  def createPool(hosts: Array[(String, Int)], password: Option[String], masterName: String): JedisSentinelPool = {
    import collection.JavaConversions._
    val sentinelSet = mutable.HashSet.empty[String]
    hosts.foreach(host => {
      sentinelSet.add(host._1 + ":" + host._2)
    })
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(1)
    poolConfig.setMaxTotal(3)
    if (password.nonEmpty) new JedisSentinelPool(masterName, sentinelSet, poolConfig, password.get)
    else new JedisSentinelPool(masterName, sentinelSet, poolConfig)
  }

  def getJedis(pool:JedisSentinelPool): Jedis ={
    pool.getResource
  }
}
