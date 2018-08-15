package edp.wormhole.sinks.cassandrasink

import edp.wormhole.sinks.SourceMutationType


case class CassandraConfig(`batch_size`: Option[Int] = None,
                           //`query_size`:Option[Int] = None,
                           `mutation_type`: Option[String] = None,
                           `allow_filtering`: Option[Boolean] = None) {
//  lazy val `cassandra.batchSize.get` = `batch_size`.getOrElse(100)
//  lazy val `cassandra.querySize.get` = `batch_size`.getOrElse(200)
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.I_U_D.toString)

}
