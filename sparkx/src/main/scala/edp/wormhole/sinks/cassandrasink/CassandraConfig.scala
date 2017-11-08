package edp.wormhole.sinks.cassandrasink

import edp.wormhole.sinks.SourceMutationType


case class CassandraConfig(`cassandra.batchSize`: Option[Int],
                      `cassandra.mutation.type`:Option[String]) {
  lazy val `cassandra.batchSize.get` = `cassandra.batchSize`.getOrElse(100)
  lazy val `cassandra.mutation.type.get` = `cassandra.mutation.type`.getOrElse(SourceMutationType.I_U_D.toString)

}
