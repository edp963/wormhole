package edp.wormhole.flinkx.pattern

import java.util.UUID

import edp.wormhole.common.feedback.ErrorPattern
import edp.wormhole.flinkx.common.{ExceptionConfig, ExceptionProcess, FlinkxUtils, WormholeFlinkxConfig}
import edp.wormhole.ums.UmsProtocolType
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row

import scala.collection.Map
class PatternOutputFilter(exceptionConfig: ExceptionConfig, config: WormholeFlinkxConfig, outputSchemaMap: Map[String, (TypeInformation[_], Int)]) extends RichFilterFunction[(Boolean,Row)]{

  override def filter(value: (Boolean, Row)): Boolean = {
    if (!value._1) {
      val dataInfoIt: Iterable[String] = outputSchemaMap.map {
        case (schemaName, (_, pos)) =>
          feedbackDataInfo(schemaName, pos, value._2)
      }
      val dataInfo = "{" + dataInfoIt.mkString(",") + "}"

      val errorMsg = FlinkxUtils.getFlowErrorMessage(dataInfo,
        exceptionConfig.sourceNamespace,
        exceptionConfig.sinkNamespace,
        1,
        null,
        UUID.randomUUID().toString,
        UmsProtocolType.DATA_INCREMENT_DATA.toString,
        exceptionConfig.flowId,
        exceptionConfig.streamId,
        ErrorPattern.FlowError)

//      val feedbackInfo = UmsProtocolUtils.feedbackFlinkxFlowError(exceptionConfig.sourceNamespace, exceptionConfig.streamId, exceptionConfig.flowId, exceptionConfig.sinkNamespace, new DateTime(), dataInfo, "")
      new ExceptionProcess(exceptionConfig.exceptionProcessMethod, config,exceptionConfig).doExceptionProcess(errorMsg)
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
