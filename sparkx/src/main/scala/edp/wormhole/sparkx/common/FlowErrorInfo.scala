package edp.wormhole.sparkx.common

case class FlowErrorInfo(flowId:Long,
                         protocolType:String,
                         matchSourceNamespace:String,
                         sinkNamespace:String,
                         error:Throwable,
                         errorPattern:String,
                         incrementTopicList:List[String],
                         count:Int) {

}
