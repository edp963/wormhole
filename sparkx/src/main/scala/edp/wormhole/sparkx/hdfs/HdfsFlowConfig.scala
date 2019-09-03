package edp.wormhole.sparkx.hdfs

import edp.wormhole.common.json.RegularJsonSchema

case class HdfsFlowConfig(dataType:String,
                          //                             fieldsInfo: Seq[FieldInfo],
                          //                             twoFieldsArr: ArrayBuffer[(String, String)],
                          //                             schemaField: Seq[UmsField],
                          jsonSchema: RegularJsonSchema,
                          flowId:Long,
                          incrementTopics:List[String],
                          hourDuration:Int) {



}
