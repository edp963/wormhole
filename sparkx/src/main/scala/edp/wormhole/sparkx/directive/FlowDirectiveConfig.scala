package edp.wormhole.sparkx.directive

case class FlowDirectiveConfig(sourceNamespace: String, fullSinkNamespace: String, streamId: Long, directiveId: Long,
                               swiftsStr: String, sinksStr: String, consumptionDataStr: String, dataType: String,
                               dataParseStr: String, kerberos: Boolean, priorityId: Long) {

}
