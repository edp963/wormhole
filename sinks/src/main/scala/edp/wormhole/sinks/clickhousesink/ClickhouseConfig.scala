package edp.wormhole.sinks.clickhousesink

import edp.wormhole.sinks.SourceMutationType

case class ClickhouseConfig(`mutation_type`: Option[String] = None,
                            `batch_size`: Option[Int] = None,
                            `ch.system_fields_rename`: Option[String] = None,
                            `ch.engine`: Option[String] = None) {
  lazy val `mutation_type.get` = `mutation_type`.getOrElse(SourceMutationType.INSERT_ONLY.toString)
  lazy val `ch.sql_batch_size.get` = `batch_size`.getOrElse(1000)
  lazy val system_fields_rename = `ch.system_fields_rename`.getOrElse("")
  lazy val clickhouse_engine = `ch.engine`.getOrElse(ClickhouseEngine.DISTRIBUTED.toString)
}
