package edp.wormhole.flinkx.util

import org.apache.flink.types.Row

object FeedbackUtils {
  def feedbackDataInfo(schemaName: String, pos: Int, value: Row): String = {
    if(value.getArity > pos) {
      schemaName + ":" + value.getField(pos).toString
    } else {
      schemaName + ":" + "null"
    }
  }
}
