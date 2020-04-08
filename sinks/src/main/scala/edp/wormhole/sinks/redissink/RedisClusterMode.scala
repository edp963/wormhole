package edp.wormhole.sinks.redissink

import org.apache.log4j.Logger

object RedisClusterMode extends Enumeration {
  private lazy val logger = Logger.getLogger(this.getClass)
  type RedisClusterMode = Value

  val SHARED = Value("shared")
  val CLUSTER = Value("cluster")

  def redisClusterMode(s: String): RedisClusterMode = try {
    RedisClusterMode.withName(s.toLowerCase)
  } catch {
    case e: Throwable =>
      logger.warn(s"RedisClusterMode invalid string: $s",e)
      SHARED
  }

}
