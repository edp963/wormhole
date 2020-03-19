package edp.wormhole.sparkxinterface.swifts

object TopicType extends Enumeration {
  type TopicType = Value

  val INCREMENT = Value("increment")
  val INITIAL = Value("initial")

  def topicType(s: String) = TopicType.withName(s.toLowerCase)
}
