package edp.wormhole.sinks.jsonsink.mongojsonsink

import edp.wormhole.sinks.SourceMutationType

case class MongoJsonConfig(`mutation_type`: Option[String]) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
}

