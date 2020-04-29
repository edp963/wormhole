package edp.wormhole.sinks.rocketmqsink

case class RocketMQConfig(producerGroup: Option[String] = None,
                          tags: Option[String] = None,
                          format: Option[String] = None,
                          preserveSystemField: Option[Boolean] = None,
                          `batch_size`: Option[Int] = None,
                          topic: Option[String] = None) {
  lazy val messageFormat = format.getOrElse("ums")//flattenJson
  lazy val limitNum = `batch_size`.getOrElse(50)
  lazy val hasSystemField = preserveSystemField.getOrElse(false)
}
