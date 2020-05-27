package edp.wormhole.dbdriver.redis

import org.apache.log4j.Logger


object RedisMode extends Enumeration {
  private lazy val logger = Logger.getLogger(this.getClass)
  type RedisMode = Value

  val STANDALONE = Value("standalone")
  val CLUSTER = Value("cluster")
  val SENTINEL = Value("sentinel")

  def redisMode(s: String): RedisMode = try {
    RedisMode.withName(s.toLowerCase)
  } catch {
    case e: Throwable =>
      logger.warn(s"RedisMode invalid string: $s",e)
      STANDALONE
  }

}
