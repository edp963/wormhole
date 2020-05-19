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


package edp.wormhole.sparkx.batchflow

import com.alibaba.fastjson.JSON
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sparkx.directive._
import edp.wormhole.sparkx.memorystorage.{ConfMemoryStorage, FlowConfig}
import edp.wormhole.sparkx.swifts.parse.ParseSwiftsSql
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, ValidityConfig}
import edp.wormhole.swifts.ConnectionMemoryStorage
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums._
import edp.wormhole.util.config.KVConfig
import edp.wormhole.util.{DateUtils, JsonUtils}

import scala.collection.mutable

object BatchflowDirective extends Directive {

  private def registerFlowStartDirective(flowDirectiveConfig: FlowDirectiveConfig): String = {

    val consumptionDataMap = mutable.HashMap.empty[String, Boolean]
    val consumption = JSON.parseObject(flowDirectiveConfig.consumptionDataStr)
    val initial = consumption.getString(InputDataProtocolBaseType.INITIAL.toString).trim.toLowerCase.toBoolean
    val increment = consumption.getString(InputDataProtocolBaseType.INCREMENT.toString).trim.toLowerCase.toBoolean
    val batch = consumption.getString(InputDataProtocolBaseType.BATCH.toString).trim.toLowerCase.toBoolean
    consumptionDataMap(InputDataProtocolBaseType.INITIAL.toString) = initial
    consumptionDataMap(InputDataProtocolBaseType.INCREMENT.toString) = increment
    consumptionDataMap(InputDataProtocolBaseType.BATCH.toString) = batch

    val sinks = JSON.parseObject(flowDirectiveConfig.sinksStr)
    val sink_connection_url = sinks.getString("sink_connection_url").trim.toLowerCase
    val sink_connection_username = if (sinks.containsKey("sink_connection_username")) Some(sinks.getString("sink_connection_username").trim) else None
    val sink_connection_password = if (sinks.containsKey("sink_connection_password")) Some(sinks.getString("sink_connection_password").trim) else None
    val parameters = if (sinks.containsKey("sink_connection_config") && sinks.getString("sink_connection_config").trim.nonEmpty) Some(JsonUtils.json2caseClass[Seq[KVConfig]](sinks.getString("sink_connection_config"))) else None
    val sink_table_keys = if (sinks.containsKey("sink_table_keys") && sinks.getString("sink_table_keys").trim.nonEmpty) Some(sinks.getString("sink_table_keys").trim.toLowerCase) else None
    val sink_specific_config = if (sinks.containsKey("sink_specific_config") && sinks.getString("sink_specific_config").trim.nonEmpty) Some(sinks.getString("sink_specific_config")) else None
    val sink_process_class_fullname = sinks.getString("sink_process_class_fullname").trim
    val sink_retry_times = sinks.getString("sink_retry_times").trim.toLowerCase.toInt
    val sink_retry_seconds = sinks.getString("sink_retry_seconds").trim.toLowerCase.toInt

    val sinkUid: Boolean = SinkProcessConfig.checkSinkUid(sink_specific_config)
    val mutationType = SinkProcessConfig.getMutaionType(sink_specific_config)

    val sink_output = if (sinks.containsKey("sink_output") && sinks.getString("sink_output").trim.nonEmpty) {
      var tmpOutput = sinks.getString("sink_output").trim.toLowerCase.split(",").map(_.trim).mkString(",")
      if ((flowDirectiveConfig.dataType == "ums" || (flowDirectiveConfig.dataType != "ums" && mutationType != "i")) && tmpOutput.nonEmpty) {
        if (tmpOutput.indexOf(UmsSysField.TS.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.TS.toString
        }
        if (tmpOutput.indexOf(UmsSysField.ID.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.ID.toString
        }
        if (tmpOutput.indexOf(UmsSysField.OP.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.OP.toString
        }
        if (sinkUid && tmpOutput.indexOf(UmsSysField.UID.toString) < 0) {
          tmpOutput = tmpOutput + "," + UmsSysField.UID.toString
        }
      }
      tmpOutput
    } else ""

    val sink_schema = if (sinks.containsKey("sink_schema") && sinks.getString("sink_schema").trim.nonEmpty) {
      val sinkSchemaEncoded = sinks.getString("sink_schema").trim
      Some(new String(new sun.misc.BASE64Decoder().decodeBuffer(sinkSchemaEncoded.toString)))
    } else None

    if (flowDirectiveConfig.dataType != "ums") {
      val parseResult: RegularJsonSchema = JsonSourceConf.parse(flowDirectiveConfig.dataParseStr)
      if (initial)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INITIAL_DATA, flowDirectiveConfig.sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if (increment)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INCREMENT_DATA, flowDirectiveConfig.sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if (batch)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_BATCH_DATA, flowDirectiveConfig.sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
    }

    val sinkProcessConfig = SinkProcessConfig(sink_output, sink_table_keys, sink_specific_config, sink_schema, sink_process_class_fullname, sink_retry_times, sink_retry_seconds, flowDirectiveConfig.kerberos)

    val swiftsProcessConfig: Option[SwiftsProcessConfig] = if (flowDirectiveConfig.swiftsStr != null) {
      val swifts = JSON.parseObject(flowDirectiveConfig.swiftsStr)
      if (swifts.size() > 0) {
        val validity = if (swifts.containsKey("validity") && swifts.getString("validity").trim.nonEmpty && swifts.getJSONObject("validity").size > 0) swifts.getJSONObject("validity") else null
        var validityConfig: Option[ValidityConfig] = None
        if (validity != null) {
          val check_columns = validity.getString("check_columns").trim.toLowerCase
          val check_rule = validity.getString("check_rule").trim.toLowerCase
          val rule_mode = validity.getString("rule_mode").trim.toLowerCase
          val rule_params = validity.getString("rule_params").trim.toLowerCase
          val against_action = validity.getString("against_action").trim.toLowerCase
          var i = 0
          if (check_rule.nonEmpty) i += 1
          if (check_columns.nonEmpty) i += 1
          if (rule_mode.nonEmpty) i += 1
          if (rule_params.nonEmpty) i += 1
          if (against_action.nonEmpty) i += 1
          if (!(i == 5 || i == 0)) {
            throw new Exception("rule related fields must be all null or not null ")
          }
          if (i > 0) validityConfig = Some(ValidityConfig(check_columns.split(",").map(_.trim), check_rule, rule_mode, rule_params, against_action))
        }
        val action: String = if (swifts.containsKey("action") && swifts.getString("action").trim.nonEmpty) swifts.getString("action").trim else null
        val dataframe_show = if (swifts.containsKey("dataframe_show") && swifts.getString("dataframe_show").trim.nonEmpty)
          Some(swifts.getString("dataframe_show").trim.toLowerCase.toBoolean)
        else Some(false)
        val dataframe_show_num: Option[Int] = if (swifts.containsKey("dataframe_show_num"))
          Some(swifts.getInteger("dataframe_show_num")) else Some(20)
        val swiftsSpecialConfig = if (swifts.containsKey("swifts_specific_config")) swifts.getString("swifts_specific_config")
        else ""

        val pushdown_connection = if (swifts.containsKey("pushdown_connection") && swifts.getString("pushdown_connection").trim.nonEmpty && swifts.getJSONArray("pushdown_connection").size > 0) swifts.getJSONArray("pushdown_connection") else null
        if (pushdown_connection != null) {
          val connectionListSize = pushdown_connection.size()
          for (i <- 0 until connectionListSize) {
            val jsonObj = pushdown_connection.getJSONObject(i)
            val name_space = jsonObj.getString("name_space").trim.toLowerCase
            val jdbc_url = jsonObj.getString("jdbc_url")
            val username = if (jsonObj.containsKey("username")) Some(jsonObj.getString("username")) else None
            val password = if (jsonObj.containsKey("password")) Some(jsonObj.getString("password")) else None
            val parameters = if (jsonObj.containsKey("connection_config") && jsonObj.getString("connection_config").trim.nonEmpty) {
              logInfo("connection_config:" + jsonObj.getString("connection_config"))
              Some(JsonUtils.json2caseClass[Seq[KVConfig]](jsonObj.getString("connection_config")))
            } else {
              logInfo("not contains connection_config")
              None
            }
            ConnectionMemoryStorage.registerDataStoreConnectionsMap(name_space, jdbc_url, username, password, parameters)
          }
        }

        val SwiftsSqlArr = if (action != null) {
          val sqlStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(action))
          ParseSwiftsSql.parse(sqlStr, flowDirectiveConfig.sourceNamespace, flowDirectiveConfig.fullSinkNamespace, if (validity == null) false else true, flowDirectiveConfig.dataType, mutationType)
        } else None
        Some(SwiftsProcessConfig(SwiftsSqlArr, validityConfig, dataframe_show, dataframe_show_num, Some(swiftsSpecialConfig)))
      } else {
        None
      }
    } else None


    val swiftsStrCache = if (flowDirectiveConfig.swiftsStr == null) "" else flowDirectiveConfig.swiftsStr


    ConfMemoryStorage.registerStreamLookupNamespaceMap(flowDirectiveConfig.sourceNamespace, flowDirectiveConfig.fullSinkNamespace, swiftsProcessConfig)
    ConfMemoryStorage.registerFlowConfigMap(flowDirectiveConfig.sourceNamespace, flowDirectiveConfig.fullSinkNamespace,
      FlowConfig(swiftsProcessConfig, sinkProcessConfig, flowDirectiveConfig.directiveId, swiftsStrCache,
        flowDirectiveConfig.sinksStr, consumptionDataMap.toMap, flowDirectiveConfig.flowId, flowDirectiveConfig.incrementTopics, flowDirectiveConfig.priorityId))


    ConnectionMemoryStorage.registerDataStoreConnectionsMap(flowDirectiveConfig.fullSinkNamespace, sink_connection_url, sink_connection_username, sink_connection_password, parameters)

    feedbackDirective(DateUtils.currentDateTime, flowDirectiveConfig.directiveId, UmsFeedbackStatus.SUCCESS, flowDirectiveConfig.streamId, flowDirectiveConfig.flowId, "")
  }

  override def flowStartProcess(ums: Ums): String = {
    """
        {
          "protocol": {
            "type": "directive_flow_start"
          },
          "schema": {
            "namespace": "kafka.hdp-kafka.test_source.test_table.*.*.*",
            "fields": [{
              "name": "directive_id",
              "type": "long",
              "nullable": false
            }, {
              "name": "stream_id",
              "type": "long",
              "nullable": false
            }, {
              "name": "flow_id",
              "type": "long",
              "nullable": false
            }, {
              "name": "source_increment_topic",
              "type": "string",
              "nullable": false
            }, {
              "name": "ums_ts_",
              "type": "datetime",
              "nullable": false
            }, {
              "name": "data_type",
              "type": "string",
              "nullable": false
            }, {
              "name": "data_parse",
              "type": "string",
              "nullable": true
            }, {
              "name": "sink_namespace",
              "type": "string",
              "nullable": false
            }, {
              "name": "consumption_protocol",
              "type": "string",
              "nullable": false
            }, {
              "name": "sinks",
              "type": "string",
              "nullable": false
            }, {
              "name": "swifts",
              "type": "string",
              "nullable": true
            }, {
              "name": "kerberos",
              "type": "boolean",
              "nullable": true
            }, {
              "name": "priority_id",
              "type": "long",
              "nullable": true
            }]
          },
          "payload": [{
            "tuple": [35, 1, 2, "test_source", "2020-05-14 11:53:31.000000", "ums_extension", "eyJmaWVsZHMiOlt7Im5hbWUiOiJpZCIsInR5cGUiOiJsb25nIiwibnVsbGFibGUiOnRydWV9LHsibmFtZSI6Im5hbWUiLCJ0eXBlIjoic3RyaW5nIiwibnVsbGFibGUiOnRydWV9LHsibmFtZSI6InBob25lIiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxlIjp0cnVlfSx7Im5hbWUiOiJjaXR5IiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxlIjp0cnVlfSx7Im5hbWUiOiJ0aW1lIiwidHlwZSI6ImRhdGV0aW1lIiwibnVsbGFibGUiOnRydWV9LHsibmFtZSI6InRpbWUiLCJ0eXBlIjoiZGF0ZXRpbWUiLCJudWxsYWJsZSI6dHJ1ZSwicmVuYW1lIjoidW1zX3RzXyJ9XX0=", "mysql.hdp-mysql.testdb.user.*.*.*", "eyJpbml0aWFsIjogdHJ1ZSwgImluY3JlbWVudCI6IHRydWUsICJiYXRjaCI6IGZhbHNlfQ==", "ew0ic2lua19jb25uZWN0aW9uX3VybCI6ICJqZGJjOm15c3FsOi8vbWFzdGVyOjMzMDYvdGVzdGRiIiwNInNpbmtfY29ubmVjdGlvbl91c2VybmFtZSI6ICJyb290IiwNInNpbmtfY29ubmVjdGlvbl9wYXNzd29yZCI6ICJZb3VyQHB3ZDEyMyIsDSJzaW5rX3RhYmxlX2tleXMiOiAiaWQiLA0ic2lua19vdXRwdXQiOiAiIiwNInNpbmtfY29ubmVjdGlvbl9jb25maWciOiAiIiwNInNpbmtfcHJvY2Vzc19jbGFzc19mdWxsbmFtZSI6ICJlZHAud29ybWhvbGUuc2lua3MuZGJzaW5rLkRhdGEyRGJTaW5rIiwNInNpbmtfc3BlY2lmaWNfY29uZmlnIjogeyJtdXRhdGlvbl90eXBlIjoiaSJ9LA0ic2lua19yZXRyeV90aW1lcyI6ICIzIiwNInNpbmtfcmV0cnlfc2Vjb25kcyI6ICIzMDAiDX0=", "eyJwdXNoZG93bl9jb25uZWN0aW9uIjpbeyJwYXNzd29yZCI6IllvdXJAcHdkMTIzIiwibmFtZV9zcGFjZSI6Im15c3FsLmhkcC1teXNxbC5sb29rdXAiLCJjb25uZWN0aW9uX2NvbmZpZyI6W10sImpkYmNfdXJsIjoiamRiYzpteXNxbDovL21hc3RlcjozMzA2L2xvb2t1cD91c2VVbmljb2RlPXRydWUmY2hhcmFjdGVyRW5jb2Rpbmc9dXRmOCZhdXRvUmVjb25uZWN0PXRydWUmZmFpbE92ZXJSZWFkT25seT1mYWxzZSZub0FjY2Vzc1RvUHJvY2VkdXJlQm9kaWVzPXRydWUmemVyb0RhdGVUaW1lQmVoYXZpb3I9Y29udmVydFRvTnVsbCZ0aW55SW50MWlzQml0PWZhbHNlIiwidXNlcm5hbWUiOiJyb290In1dLCJkYXRhZnJhbWVfc2hvdyI6InRydWUiLCJhY3Rpb24iOiJjSFZ6YUdSdmQyNWZjM0ZzSUd4bFpuUWdhbTlwYmlCM2FYUm9JRzE1YzNGc0xtaGtjQzF0ZVhOeGJDNXNiMjlyZFhBZ1BTQnpaV3hsXG5ZM1FnYVdRZ1lYTWdhV1F4TEdOaGNtUkNZVzVySUdaeWIyMGdkWE5sY2tOaGNtUWdkMmhsY21VZ0tHbGtLU0JwYmlBb2EyRm1hMkV1XG5hR1J3TFd0aFptdGhMblJsYzNSZmMyOTFjbU5sTG5SbGMzUmZkR0ZpYkdVdWFXUXBPM053WVhKclgzTnhiQ0E5SUhObGJHVmpkQ0JwXG5aQ3h1WVcxbExHTmhjbVJDWVc1ckxIQm9iMjVsTEdOcGRIa2dabkp2YlNCMFpYTjBYM1JoWW14bE93PT0iLCJkYXRhZnJhbWVfc2hvd19udW0iOjEwfQ==", "false", "1"]
          }]
        }
      """
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    val tuple = payloads.head  // 取第一条

    val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
    val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
    val flowId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "flow_id").toString.toLong
    try {
      val swiftsEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "swifts")  // base64编码的

      val swiftsStr = if (swiftsEncoded != null && !swiftsEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(swiftsEncoded.toString)) else null
      logInfo("swiftsStr:" + swiftsStr)
      val sinksStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sinks").toString))
      logInfo("sinksStr:" + sinksStr)
      val fullSinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sink_namespace").toString.toLowerCase
      val consumptionDataStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "consumption_protocol").toString))
      val dataType = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
      val dataParseEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_parse")
      val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString)) else null
      val kerberos = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kerberos").toString.toBoolean
      val tmpPriorityIdStr = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "priority_id")
      val priorityId = if (tmpPriorityIdStr == null) directiveId else tmpPriorityIdStr.toString.toLong
      //val sourceIncrementTopicList = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "source_increment_topic").toString.split(",").toList
      val sourceIncrementTopic = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "source_increment_topic")

      val sourceIncrementTopicList =
        if(null != sourceIncrementTopic) sourceIncrementTopic.toString.split(",").toList
        else null

      // 定义config对象
      val flowDirectiveConfig = FlowDirectiveConfig(sourceNamespace, fullSinkNamespace, streamId, flowId, directiveId, swiftsStr, sinksStr, consumptionDataStr, dataType, dataParseStr, kerberos, priorityId, sourceIncrementTopicList)
      // 初始化各种配置
      registerFlowStartDirective(flowDirectiveConfig)
    } catch {
      case e: Throwable =>
        logAlert("registerFlowStartDirective,sourceNamespace:" + sourceNamespace, e)
        feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, flowId, e.getMessage)
    }
  }
}
