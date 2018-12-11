package edp.wormhole.flinkx.pattern

import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcess, WormholeFlinkxConfig}
import edp.wormhole.ums.UmsProtocolUtils
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.joda.time.DateTime

class PatternOutputFilter(exceptionConfig: ExceptionConfig, config: WormholeFlinkxConfig, outputSchemaMap: Map[String, (TypeInformation[_], Int)]) extends RichFilterFunction[(Boolean,Row)]{

  override def filter(value: (Boolean, Row)): Boolean = {
    if (!value._1) {
      val dataInfoIt: Iterable[String] = outputSchemaMap.map {
        case (schemaName, (_, pos)) =>
          feedbackDataInfo(schemaName, pos, value._2)
      }
      val dataInfo = "{" + dataInfoIt.mkString(",") + "}"
      val feedbackInfo = UmsProtocolUtils.feedbackFlowFlinkxError(exceptionConfig.sourceNamespace, exceptionConfig.streamId, exceptionConfig.flowId, exceptionConfig.sinkNamespace, new DateTime(), dataInfo, "")
      new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config).doExceptionProcess(feedbackInfo)
    }
    value._1
  }

  def feedbackDataInfo(schemaName: String, pos: Int, value: Row): String = {
    if(value.getArity > pos) {
      schemaName + ":" + value.getField(pos).toString
    } else {
      schemaName + ":" + "null"
    }
  }

}
