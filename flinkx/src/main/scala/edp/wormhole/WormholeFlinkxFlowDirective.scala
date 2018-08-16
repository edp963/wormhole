package edp.wormhole

//import edp.wormhole.common.{JsonSourceConf, RegularJsonSchema}
import com.alibaba.fastjson.JSON
import edp.wormhole.common.json.{JsonSourceConf, RegularJsonSchema}
import edp.wormhole.ums.Ums
//import edp.wormhole.WormholeFlinkxStarter.{config, umsFlowStart}
import edp.wormhole.common._
import edp.wormhole.util.{FlinkSchemaUtils, InputDataRequirement, UmsFlowStartUtils}
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.ums.{UmsFieldType, UmsProtocolType, UmsSchema, UmsSchemaUtils}
import edp.wormhole.util.FlinkSchemaUtils.findJsonSchema
import edp.wormhole.util.UmsFlowStartUtils.{extractVersion, formatZkPath}


object WormholeFlinkxFlowDirective {
  def initFlow(ums : Ums,config: WormholeFlinkxConfig):String= {
    //val ums = UmsSchemaUtils.toUms(json)
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    val tuple = payloads.head

    val dataType: String = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
    val consumptionDataStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(UmsFieldType.umsFieldValue(tuple.tuple, schemas, "consumption_protocol").toString))
    val dataParseEncoded = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_parse")
    val dataParseStr = if (dataParseEncoded != null && !dataParseEncoded.toString.isEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(dataParseEncoded.toString)) else null

    //println("dataType:"+ dataType + ";\nconsumptionDataStr:" + consumptionDataStr + ";\ndataParseStr:" + dataParseStr + "--------------to json")

    val consumption = JSON.parseObject(consumptionDataStr)
    val initial = consumption.getString(InputDataRequirement.INITIAL.toString).trim.toLowerCase.toBoolean
    val increment = consumption.getString(InputDataRequirement.INCREMENT.toString).trim.toLowerCase.toBoolean
    val batch = consumption.getString(InputDataRequirement.BATCH.toString).trim.toLowerCase.toBoolean

    if (dataType != "ums") {
      //println(" --------------to json")
      val parseResult: RegularJsonSchema = JsonSourceConf.parse(dataParseStr)
      //FlinkxConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INCREMENT_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if(initial)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INITIAL_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if(increment)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_INCREMENT_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      if(batch)
        ConfMemoryStorage.registerJsonSourceParseMap(UmsProtocolType.DATA_BATCH_DATA, sourceNamespace, parseResult.schemaField, parseResult.fieldsInfo, parseResult.twoFieldsArr)
      //json set field
      FlinkSchemaUtils.setSourceSchemaMap(UmsSchema(sourceNamespace,Some(parseResult.schemaField)))
    }else {
      //println(" --------------to ums")
      FlinkSchemaUtils.setSourceSchemaMap(getJsonSchema(config,ums))
    }
    dataType
  }

  private def getJsonSchema(config:WormholeFlinkxConfig,umsFlowStart:Ums): UmsSchema= {
    val zkAddress: String = config.zookeeper_address
    val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
    val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head)
    val zkPath: String = formatZkPath(sourceNamespace, streamId)
    val version = extractVersion(sourceNamespace)
    val zkPathWithVersion = UmsFlowStartUtils.formatZkPathWithVersion(sourceNamespace, streamId.mkString(""), version)
    findJsonSchema(config, zkAddress, zkPathWithVersion, sourceNamespace)
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
