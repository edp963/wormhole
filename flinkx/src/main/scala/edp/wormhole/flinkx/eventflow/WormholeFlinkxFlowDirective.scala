package edp.wormhole.flinkx.eventflow

import com.alibaba.fastjson.JSON
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.flinkx.common.{ConfMemoryStorage, WormholeFlinkxConfig}
import edp.wormhole.flinkx.util.FlinkSchemaUtils.findJsonSchema
import edp.wormhole.flinkx.util.UmsFlowStartUtils.extractVersion
import edp.wormhole.flinkx.util.{FlinkSchemaUtils, UmsFlowStartUtils}
import edp.wormhole.ums._


object WormholeFlinkxFlowDirective {
  def initFlow(ums: Ums, config: WormholeFlinkxConfig): String = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    val tuple = payloads.head
    val dataType: String = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
    val consumptionDataStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "consumption_protocol").toString))
    val dataParseEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_parse")
    val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString)) else null
    val consumption = JSON.parseObject(consumptionDataStr)
    val initial = consumption.getString(InputDataProtocolBaseType.INITIAL.toString).trim.toLowerCase.toBoolean
    val increment = consumption.getString(InputDataProtocolBaseType.INCREMENT.toString).trim.toLowerCase.toBoolean
    val batch = consumption.getString(InputDataProtocolBaseType.BATCH.toString).trim.toLowerCase.toBoolean

    if (dataType != "ums") {
      val parseResult: RegularJsonSchema = JsonSourceConf.parse(dataParseStr)
      if (initial)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INITIAL_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if (increment)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INCREMENT_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if (batch)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_BATCH_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      FlinkSchemaUtils.setSourceSchemaMap(UmsSchema(sourceNamespace, Some(parseResult.schemaField)))
    } else {
      FlinkSchemaUtils.setSourceSchemaMap(getJsonSchema(config, ums))
    }
    dataType
  }

  private def getJsonSchema(config: WormholeFlinkxConfig, umsFlowStart: Ums): UmsSchema = {
    val zkAddress: String = config.zookeeper_address
    val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
    val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head)

    val version = extractVersion(sourceNamespace)
    val zkPathWithVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), version)
    findJsonSchema(config, zkAddress, zkPathWithVersion, sourceNamespace)


    //    val zkPath: String = formatZkPath(sourceNamespace, streamId)
    //    WormholeZkClient.createPath(zkAddress, zkPath)
    //    val zkPathWithVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), version)
    //    if (!WormholeZkClient.checkExist(zkAddress, zkPathWithVersion)) {
    //      val maxVersion = UmsFlowStartUtils.getMaxVersion(zkAddress, zkPath)
    //      if (null != maxVersion) {
    //        logger.info("maxVersion is not null")
    //        val zkPathWithMaxVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), maxVersion)
    //        getSchemaFromZk(zkAddress, zkPathWithMaxVersion)
    //      } else findJsonSchema(config, zkAddress, zkPathWithVersion, sourceNamespace)
    //    } else {
    //      getSchemaFromZk(zkAddress, zkPathWithVersion)
    //    }
  }
}
