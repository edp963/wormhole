package edp.wormhole.config

case class KafkaConfig(format: Option[String] = None,
                       preserveSystemField: Option[Boolean] = None,
                       `batch_size`: Option[Int] = None,
                       sinkKafkaTopic:Option[String]) {
                         lazy val messageFormat = format.getOrElse("ums")
                         lazy val limitNum = `batch_size`.getOrElse(50)
                         lazy val hasSystemField = preserveSystemField.getOrElse(false)
                       }
