package edp.wormhole.sparkx.common

case class FlowErrorInfo(flowId:Long,
                         protocolType:String,
                         matchSourceNamespace:String,
                         sinkNamespace:String,
                         errorMsg:String,
                         errorPattern:String,
                         incrementTopicList:List[String],
                         count:Int) {

}
