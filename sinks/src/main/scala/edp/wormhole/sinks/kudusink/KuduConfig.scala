package edp.wormhole.sinks.kudusink

import edp.wormhole.sinks.SourceMutationType

case class KuduConfig(`mutation_type`:Option[String] = None,
                 `batch_size`: Option[Int] = None){
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val `batch_size.get` = `batch_size`.getOrElse(10000)
}
