package edp.wormhole.config

case class SinkProcessConfig(sinkOutput:String,
                             tableKeys: Option[String],
                             specialConfig: Option[String],
                             jsonSchema:Option[String],
                             classFullname: String,
                             retryTimes: Int,
                             retrySeconds: Int) {
                               lazy val tableKeyList = if (tableKeys.isEmpty || tableKeys.get == null) Nil else tableKeys.get.split(",").map(_.trim.toLowerCase).toList
                             }
