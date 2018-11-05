package edp.wormhole.flinkx.util

import org.apache.flink.streaming.api.scala.OutputTag

object FlinkOutputTag extends Serializable {
  val flinkExceptionTag = OutputTag[String]("flinkExceptionTag")
}
