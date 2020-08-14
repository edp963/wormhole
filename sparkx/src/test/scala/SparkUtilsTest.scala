import edp.wormhole.sparkx.common.SparkxUtils
import edp.wormhole.sparkxinterface.swifts.{RenameKeyConfig, StreamSpecialConfig}

object SparkUtilsTest extends App {
  val key = "keyOrg"
  val topic = "topicl"
  val namespace = Set("increament.a.b.c.d")
  val renameKeyConfig = RenameKeyConfig("topicl", None, "data_increment_data.kafka.kafka01022.topicl.ums.*.*.*")
  val streamSpecialConfig = StreamSpecialConfig(None, Some(Seq(renameKeyConfig)))
  val kafkaKeyConfig = SparkxUtils.getKafkaKeyConfig(Some(streamSpecialConfig))
  val renameKey = SparkxUtils.getDefaultKey(key, topic, namespace, kafkaKeyConfig)
  println(renameKey)
}
