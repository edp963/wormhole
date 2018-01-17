package edp.mad.module

import java.util.concurrent.TimeUnit
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.concurrent.duration.{FiniteDuration, _}

trait ConfigOptionImpl {
  this: ConfigModule =>

  case class MadServer(host: String, port: Int, adminUser: String, adminPwd: String, normalUser: String, normalPwd: String)

  case class RiderServer(host: String, port: Int, adminUser: String, adminPwd: String, normalUser: String, normalPwd: String, token:String)

  case class MadKafka(
                       feedbackBrokers: String,
                       feedbackZkUrl: String,
                       feedbackTopic: String,
                       feedbackPartitions: Int,
                       feedbackClientId: String,
                       feedbackGroupId: String,
                       feedbackFromOffset:String,
                       logsEnable : Boolean,
                       logsBrokers: String,
                       logsZkUrl: String,
                       logsTopic: String,
                       logsTopicPartitions: Int,
                       logsClientId: String,
                       logsGroupId: String,
                       logsFromOffset:String,
                       autoCommit: Boolean,
                       keyDeserializer: ByteArrayDeserializer,
                       valueDeserializer: StringDeserializer,
                       pollInterval: FiniteDuration,
                       pollTimeout: FiniteDuration,
                       stopTimeout: FiniteDuration,
                       closeTimeout: FiniteDuration,
                       commitTimeout: FiniteDuration,
                       wakeupTimeout: FiniteDuration,
                       maxWakeups: Int,
                       dispatcher: String)


  case class MadTsql( dbDriver: String, dburl: String, user: String, pwd: String)

  case class MadEs( url: String, user: String, pwd: String, infoRedundance: Boolean )

  case class MadDashboard(url: String, domain: String, esDataSourceName: String, adminToken: String)

  case class MadMaintenance(mysqlRemain: Int, esRemain: Int, cachePersistence: Boolean )

  case class MadRedis( enable: Boolean, url: String, password: String, mode: String , expireSeconds: Int)


  lazy val madServer = MadServer(
    config.getString("madServer.host"), config.getInt("madServer.port"),
    getStringConfig("madServer.admin.username", "admin"),
    getStringConfig("madServer.admin.password", "admin"),
    getStringConfig("madServer.normal.username", "normal"),
    getStringConfig("madServer.normal.password", "normal"))

  lazy val riderServer = RiderServer(
    config.getString("riderServer.host"), config.getInt("riderServer.port"),
    getStringConfig("riderServer.admin.username", "admin"),
    getStringConfig("riderServer.admin.password", "admin"),
    getStringConfig("riderServer.normal.username", "normal"),
    getStringConfig("riderServer.normal.password", "normal"),
    config.getString("riderServer.token"))

  lazy val madHome = s"${System.getenv("MAD_HOME")}"

  lazy val feedbackConsumerStreamId = -1
  lazy val logsConsumerStreamId = -2
  lazy val cachePersistenceToDB = true

  lazy val madRedis = MadRedis(
    getBooleanConfig("redis.enable",false),
    getStringConfig("redis.url",""),
    getStringConfig("redis.password",""),
    getStringConfig("redis.mode","shared"),
    getIntConfig("redis.expireSeconds", 1800)
  )

  lazy val pollInterval = getFiniteDurationConfig("kafka.consumer.settings.poll-interval", FiniteDuration(30, MILLISECONDS))
  lazy val pollTimeout = getFiniteDurationConfig("kafka.consumer.settings.poll-timeout", FiniteDuration(30, MILLISECONDS))
  lazy val stopTimeout = getFiniteDurationConfig("kafka.consumer.settings.stop-timeout", FiniteDuration(30, SECONDS))
  lazy val closeTimeout = getFiniteDurationConfig("kafka.consumer.settings.close-timeout", FiniteDuration(20, SECONDS))
  lazy val commitTimeout = getFiniteDurationConfig("kafka.consumer.settings.commit-timeout", FiniteDuration(15, SECONDS))
  lazy val wakeupTimeout = getFiniteDurationConfig("kafka.consumer.settings.wakeup-timeout", FiniteDuration(3, SECONDS))
  lazy val maxWakeups = getIntConfig("kafka.consumer.settings.max-wakeups", 10)

  lazy val feedbackTopic = getStringConfig("kafka.feedback.topic", "wormhole_feedback")
  lazy val feedbackClientId = getStringConfig("kafka.feedback.client.id", "mad_wormhole_feedback_client")
  lazy val feedbackGroupId = getStringConfig("kafka.feedback.group.id", "mad_wormhole_feedback_group")
  lazy val feedbackFromOffset = getStringConfig("kafka.feedback.fromOffset", "latest")

  lazy val logsEnable = getBooleanConfig("kafka.logs.enable",false)
  lazy val logsBrokers = getStringConfig("kafka.logs.brokers.url","")
  lazy val logsZkUrl = getStringConfig("kafka.logs.zookeeper.url","")
  lazy val logsTopic = getStringConfig("kafka.logs.topic","")
  lazy val logsClientId = getStringConfig("kafka.logs.client.id", "mad_logs_client")
  lazy val logsGroupId = getStringConfig("kafka.logs.group.id", "mad_logs_group")
  lazy val logsFromOffset = getStringConfig("kafka.logs.fromOffset", "latest")
  lazy val hadoopYarnRMSite1 = getStringConfig("yarn.resource.manager.site1","")
  lazy val hadoopYarnRMSite2 = getStringConfig("yarn.resource.manager.site2","")
  lazy val madKafka = MadKafka(
    config.getString("kafka.feedback.brokers.url"),
    config.getString("kafka.feedback.zookeeper.url"),
    feedbackTopic,
    4,
    feedbackClientId,
    feedbackGroupId,
    feedbackFromOffset,
    logsEnable,
    logsBrokers,
    logsZkUrl,
    logsTopic,
    1,
    logsClientId,
    logsGroupId,
    logsFromOffset,
    false,
    new ByteArrayDeserializer, new StringDeserializer,
    pollInterval,
    pollTimeout,
    stopTimeout,
    closeTimeout,
    commitTimeout,
    wakeupTimeout,
    maxWakeups,
    "akka.kafka.default-dispatcher"
  )

  lazy val madTsql = MadTsql(
    getStringConfig("tsql.driver","com.mysql.jdbc.Driver"),
    getStringConfig("tsql.url", "jdbc:mysql://10.120.65.11:3306/mad"),
    "root","root"
  )

  lazy val madEs =
    if (config.hasPath("elasticSearch") && config.getString("elasticSearch.http.url").nonEmpty) {
      MadEs(config.getString("elasticSearch.http.url"),
        getStringConfig("elasticSearch.http.user", ""),
        getStringConfig("elasticSearch.http.password", ""),
        getBooleanConfig("elasticSearch.http.infoRedundance",true))
    } else null

  lazy val grafanaDomain =
    if (config.hasPath("grafana.production.domain.url")) config.getString("grafana.production.domain.url")
    else config.getString("grafana.url")

  lazy val madDashboard =
    if (config.hasPath("grafana") && config.getString("grafana.url").nonEmpty && config.getString("grafana.admin.token").nonEmpty)
      MadDashboard(config.getString("grafana.url"),
        grafanaDomain, "wormhole_stats",
        config.getString("grafana.admin.token"))
    else null

  lazy val madMaintenance = MadMaintenance(
    getIntConfig("maintenance.mysql.feedback.remain.maxDays", 30),
    getIntConfig("maintenance.elasticSearch.feedback.remain.maxDays",7),
    getBooleanConfig("maintenance.cache.persistence", true))

  //override lazy val madInfo = MadInfo( madKafka.feedbackBrokers, madKafka.feedbackTopic)

  def getStringConfig(path: String, default: String): String = {
    if (config.hasPath(path) && config.getString(path) != null && config.getString(path) != "" && config.getString(path) != " ")
      config.getString(path)
    else default
  }

  def getIntConfig(path: String, default: Int): Int = {
    if (config.hasPath(path) && !config.getIsNull(path))
      config.getInt(path)
    else default
  }

  def getFiniteDurationConfig(path: String, default: FiniteDuration): FiniteDuration = {
    if (config.hasPath(path) && !config.getIsNull(path))
      config.getDuration(path, TimeUnit.MILLISECONDS).millis
    else default
  }

  def getBooleanConfig(path: String, default: Boolean): Boolean = {
    if (config.hasPath(path) && !config.getIsNull(path))
      config.getBoolean(path)
    else default
  }

}