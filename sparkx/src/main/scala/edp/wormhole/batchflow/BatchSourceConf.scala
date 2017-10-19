package edp.wormhole.batchflow

import edp.wormhole.ums.UmsField

import scala.collection.mutable

object BatchSourceConf {
  def parse(dataParseStr: String): RegularJsonSchema = {
    val a: Seq[UmsField] = null
    val b = mutable.HashMap.empty[String, Any].toMap
    val c: String = null
    RegularJsonSchema(b, a, c)
  }
}

case class RegularJsonSchema(schemaMap: Map[String, Any], schemaField: Seq[UmsField], TimeField: String)
