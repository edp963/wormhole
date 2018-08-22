package edp.wormhole.common

object InputDataProtocolBaseType extends Enumeration {
  type InputDataProtocolBaseType = Value
  val INITIAL = Value("initial")
  val INCREMENT = Value("increment")
  val BATCH = Value("batch")

  def inputDataRequirement(s: String) = InputDataProtocolBaseType.withName(s.toLowerCase)

}
