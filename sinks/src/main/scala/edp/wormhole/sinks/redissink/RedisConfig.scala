package edp.wormhole.sinks.redissink

import edp.wormhole.sinks.SourceMutationType

case class RedisConfig(`mutation_type`: Option[String] = None,
                       expireTime: Option[Int] = None,
                       saveTableNameAsKeyPrefix:Option[Boolean]=None) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val expireTimeInSeconds = expireTime.getOrElse(0)
  lazy val `saveTableNameAsKeyPrefix.get`= saveTableNameAsKeyPrefix.getOrElse(true)
}
