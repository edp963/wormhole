package test.scala

import edp.wormhole.ums.{Ums, UmsSchemaUtils}


object SparkUtilsTest extends App {
  val arg1 = ""
  val umsFlowStart: Ums = UmsSchemaUtils.toUms(arg1)
  println(umsFlowStart)
}
