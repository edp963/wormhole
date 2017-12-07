package edp.wormhole.sinks.mongosink

import edp.wormhole.sinks.SourceMutationType

case class MongoConfig(mutation_type: Option[String]){//,rowkey_type:Option[String]) {
  lazy val `mutation_type.get` = mutation_type.getOrElse(SourceMutationType.I_U_D.toString)

//  lazy val `rowkey_type.get` = rowkey_type.getOrElse(RowKeyType.SYSTEM.toString)
}
