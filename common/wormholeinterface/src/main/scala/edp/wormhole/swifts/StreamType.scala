package edp.wormhole.swifts

object StreamType extends Enumeration {
  type StreamType = Value
  val BATCHFLOW = Value("batchflow")
  val HDFSLOG = Value("hdfslog")
  val ROUTER = Value("router")

  def streamType(s: String) = StreamType.withName(s.toLowerCase)

}


object InputDataProtocolBaseType extends Enumeration {
  type InputDataProtocolBaseType = Value
  val INITIAL = Value("initial")
  val INCREMENT = Value("increment")
  val BATCH = Value("batch")

  def inputDataRequirement(s: String) = InputDataProtocolBaseType.withName(s.toLowerCase)

}