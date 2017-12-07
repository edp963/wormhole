package edp.wormhole.sinks.cassandrasink

import edp.wormhole.sinks.SourceMutationType


case class CassandraConfig(`cassandra.batchSize`: Option[Int],
                           `mutation_type`:Option[String]) {
  lazy val `cassandra.batchSize.get` = `cassandra.batchSize`.getOrElse(100)
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)

}
