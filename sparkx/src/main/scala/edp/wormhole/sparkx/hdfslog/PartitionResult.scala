package edp.wormhole.sparkx.hdfslog

case class PartitionResult(result: Boolean,
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
                           allCount: Int) {

}
