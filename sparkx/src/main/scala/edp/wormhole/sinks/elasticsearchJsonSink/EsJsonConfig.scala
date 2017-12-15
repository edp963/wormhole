package edp.wormhole.sinks.elasticsearchJsonSink

import edp.wormhole.sinks.SourceMutationType

case class EsJsonConfig(`mutation_type`: Option[String]) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
}
