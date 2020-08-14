import edp.rider.rest.util.StreamUtils

object StreamUtilsTest extends App {
  //val topicl = "{\"topicl\": \"data_increment_data.kafka.kafka01022.topicl.ums.*.*.*\"}"
  val config = "{\"renameKeyConfig\": [{\"topicName\":\"topicl\",\"renameKey\":\"data_increment_data.kafka.kafka01022.topicl.ums.*.*.*\"}]}"
  val result = StreamUtils.getStreamSpecialConfig(Some(config))
  println(result)

}
