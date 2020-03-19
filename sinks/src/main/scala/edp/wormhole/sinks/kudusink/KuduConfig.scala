package edp.wormhole.sinks.kudusink

import edp.wormhole.sinks.SourceMutationType

case class KuduConfig(`mutation_type`: Option[String] = None,
                      `batch_size`: Option[Int] = None,
                      `query_batch_size`: Option[Int] = None,
                      `table_connect_character`: Option[String] = None) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val `batch_size.get` = `batch_size`.getOrElse(1000)
  lazy val `query_batch_size.get` = `query_batch_size`.getOrElse(1)
  lazy val `table_connect_character.get` = `table_connect_character`.getOrElse("")
}
