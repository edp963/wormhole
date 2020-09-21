package test.scala

import edp.wormhole.flinkx.common.WormholeFlinkxConfig
import edp.wormhole.flinkx.util.FlinkSchemaUtils.{isCorrectRecord, logger}
import edp.wormhole.ums.UmsCommonUtils
import edp.wormhole.util.JsonUtils

object test extends App {
  val arg0 = ""
  val config: WormholeFlinkxConfig = JsonUtils.json2caseClass[WormholeFlinkxConfig](arg0)
  println(config)
}


object test2 extends App {
  val key = "data_initial_data.mysql.testdb.test_schema1.test5.0.0.0.1599636628045.wh_placeholder"
  val sourceNamespace = "mysql.testdb.test_schema1.test5.*.*.*"
  val value = ""
  val key2Verify = UmsCommonUtils.checkAndGetKey(key, value)
  //val correctData = isCorrectRecord(key2Verify, value, sourceNamespace)
  //println(correctData)
}
