package edp.wormhole.sinks.mongosink

import edp.wormhole.sinks.SourceMutationType

case class MongoConfig(`es.mutation_type`: Option[String],row_key:Option[String]) {
  lazy val `es.mutation_type.get` = `es.mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
}
