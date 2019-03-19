/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.wormhole.sparkx.hdfslog

import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.sparkx.directive.Directive
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums.{DataTypeEnum, Ums, UmsFeedbackStatus, UmsFieldType}
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
    val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString)) else null
    val flowId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "flow_id").toString.toLong
    val sourceIncrementTopicList = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "source_increment_topic").toString.split(",").toList
    val hourDuration = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "hour_duration").toString.toLowerCase.toInt
    try {
      val parseResult: RegularJsonSchema = if (dataParseStr != null) {
        JsonSourceConf.parse(dataParseStr)
      } else RegularJsonSchema(null,null,null)
      ConfMemoryStorage.hdfslogMap(namespace_rule) = HdfsLogFlowConfig(data_type, parseResult, flowId, sourceIncrementTopicList, hourDuration)

      //      HdfsMainProcess.directiveNamespaceRule(namespace_rule) = hour_duration
      feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, flowId, "")

    } catch {
      case e: Throwable =>
        logAlert("registerFlowStartDirective,sourceNamespace:" + namespace_rule, e)
        feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId,flowId, e.getMessage)
    }
  }
}
