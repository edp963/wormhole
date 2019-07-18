package edp.wormhole.sparkx.hdfs

case class PartitionResult(index:Int,
                           result: Boolean,
                           errorFileName: String,
                           errorCount: Int,
                           errorMetaContent: String,
                           correctFileName: String,
                           correctCount: Int,
                           correctMetaContent: String,
                           protocol:String,
                           namespace: String,
                           minTs: String,
                           maxTs: String,
                           allCount: Int,
                           flowId:Long) {

}
