package edp.wormhole.sparkx.hdfs

import edp.wormhole.common.json.RegularJsonSchema
import edp.wormhole.ums.ext.ExtSchemaConfig

case class HdfsFlowConfig(dataType:String,
                          //                             fieldsInfo: Seq[FieldInfo],
                          //                             twoFieldsArr: ArrayBuffer[(String, String)],
                          //                             schemaField: Seq[UmsField],
                          jsonSchema: RegularJsonSchema,
                          extSchemaConfig: ExtSchemaConfig,
                          flowId:Long,
                          incrementTopics:List[String],
                          hourDuration:Int) {



}
