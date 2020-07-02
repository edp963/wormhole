package edp.wormhole.flinkextension.table.api

import java.time.ZoneId

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @author Suxy
  * @date 2020/7/1
  * @description file description
  */
object TableEnvironmentBuilder {

  def build(env: StreamExecutionEnvironment): StreamTableEnvironment = {
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("GMT+8"))
    tableEnv
  }

}
