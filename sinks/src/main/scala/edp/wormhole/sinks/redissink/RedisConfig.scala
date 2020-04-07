package edp.wormhole.sinks.redissink

import edp.wormhole.sinks.SourceMutationType

case class RedisConfig(`mutation_type`: Option[String] = None,
                       mode: Option[String] = Option(""),
                       expireTime: Option[Int] = None) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val redisMode = mode.get
  lazy val expireTimeInSeconds = expireTime.getOrElse(0)
}
