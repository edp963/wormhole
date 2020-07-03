package edp.wormhole.flinkextension.table.api

import java.util.TimeZone

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @author Suxy
  * @date 2020/7/1
  * @description file description
  */
object TableEnvironmentBuilder {

  def build(env: StreamExecutionEnvironment): StreamTableEnvironment = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    tableEnv.config.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    tableEnv
  }

}
