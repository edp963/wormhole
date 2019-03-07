package edp.wormhole.sparkx.memorystorage

import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sparkxinterface.swifts.SwiftsProcessConfig
//SwiftsProcessConfig, SinkProcessConfig, directiveId, swiftsConfigStr,sinkConfigStr,consumption_data_type,ums/json
//Option[SwiftsProcessConfig], SinkProcessConfig, Long, String, String, Map[String, Boolean]
case class FlowConfig( swiftsProcessConfig:Option[SwiftsProcessConfig],
                  sinkProcessConfig:SinkProcessConfig,
                  directiveId:Long,
                  swiftsConfigStr:String,
                  sinkConfigStr:String,
                  consumptionDataType:Map[String, Boolean]) {
}
