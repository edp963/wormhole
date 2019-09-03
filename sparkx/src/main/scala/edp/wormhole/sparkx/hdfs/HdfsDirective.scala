package edp.wormhole.sparkx.hdfs

import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.sparkx.directive.Directive
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums.{Ums, UmsFeedbackStatus, UmsFieldType}
import edp.wormhole.util.DateUtils

object HdfsDirective extends Directive {
  override def flowStartProcess(ums: Ums): String = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val tuple = payloads.head
    val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
    val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
    val namespace_rule = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "namespace_rule").toString.toLowerCase
    val data_type = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
    val dataParseEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_parse")
    val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty)
      new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString))
    else null
    val flowId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "flow_id").toString.toLong
    val sourceIncrementTopicList = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "source_increment_topic").toString.split(",").toList
    val hourDuration = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "hour_duration").toString.toLowerCase.toInt
    try {
      val parseResult: RegularJsonSchema = if (dataParseStr != null) {
        JsonSourceConf.parse(dataParseStr)
      } else RegularJsonSchema(null,null,null)

      if(ums.protocol.`type`.toString.toLowerCase.contains("hdfscsv")){
        ConfMemoryStorage.hdfscsvMap(namespace_rule) = HdfsFlowConfig(data_type, parseResult, flowId, sourceIncrementTopicList, hourDuration)
      } else if(ums.protocol.`type`.toString.toLowerCase.contains("hdfslog")){
        ConfMemoryStorage.hdfslogMap(namespace_rule) = HdfsFlowConfig(data_type, parseResult, flowId, sourceIncrementTopicList, hourDuration)
      }

      feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, flowId, "")

    } catch {
      case e: Throwable =>
        logAlert("registerFlowStartDirective,sourceNamespace:" + namespace_rule, e)
        feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId,flowId, e.getMessage)
    }
  }
}
