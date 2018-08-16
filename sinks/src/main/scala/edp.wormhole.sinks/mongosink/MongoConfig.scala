package edp.wormhole.sinks.mongosink

import edp.wormhole.sinks.SourceMutationType

case class MongoConfig(mutation_type: Option[String] = None,_id:Option[String] = None) {
  lazy val `mutation_type.get` = mutation_type.getOrElse(SourceMutationType.I_U_D.toString)
  lazy val `_id.get` = if(_id.nonEmpty) _id.get.split(",") else Array.empty[String]
//  lazy val `rowkey_type.get` = rowkey_type.getOrElse(RowKeyType.SYSTEM.toString)
}
