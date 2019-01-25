/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package edp.rider.common

import java.util.concurrent.TimeUnit

import edp.rider.RiderStarter.modules.config
import edp.rider.rest.persistence.entities.{FlinkDefaultConfig, FlinkResourceConfig}
import edp.rider.wormhole.FlinkCheckpoint
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.collection.JavaConversions._

import scala.concurrent.duration.{FiniteDuration, _}

case class RiderServer(clusterId: String,
                       host: String,
                       port: Int,
                       requestTimeOut: Duration,
                       defaultLanguage: String = "Chinese",
                       tokenKey: String,
                       adminUser: String,
                       adminPwd: String,
                       normalUser: String,
                       normalPwd: String)

case class RiderKafka(brokers: String,
                      zkUrl: String,
                      feedbackTopic: String,
                      heartbeatTopic: String,
                      refactor: Int,
                      partitions: Int,
                      client_id: String,
                      group_id: String,
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


case class RiderDatabase(url: String, user: String, pwd: String)

case class RiderSpark(user: String,
                      sshPort: Int,
                      sparkHome: String,
                      queueName: String,
                      appTags: String,
                      hdfsRoot: String,
                      remoteHdfsRoot: Option[String],
                      remoteHdfsNamenodeHosts: Option[String],
                      remoteHdfsActiveNamenodeHost: Option[String],
                      remoteHdfsNamenodeIds: Option[String],
                      rm1Url: String,
                      rm2Url: String,
                      kafkaSessionTimeOut: Int,
                      kafkaGroupMaxSessionTimeOut: Int,
                      startShell: String,
                      clientLogRootPath: String,
                      sparkLog4jPath: String,
                      jarPath: String,
                      kafka08JarPath: String,
                      kafka08StreamNames: String,
                      sparkxInterfaceJarPath: String,
                      wormholeHeartBeatTopic: String,
                      driverMemory: Int,
                      driverCores: Int,
                      executorNum: Int,
                      executorMemory: Int,
                      executorCores: Int,
                      batchDurationSec: Int,
                      parallelismPartition: Int,
                      maxPartitionFetchMb: Int,
                      topicDefaultRate: Int,
                      jobMaxRecordPerPartitionProcessed: Int,
                      driverExtraConf: String,
                      executorExtraConf: String,
                      sparkConfig: String,
                      alert: Boolean,
                      metricsConfPath: String,
                      alertEmails: String)


case class RiderEs(url: String,
                   wormholeIndex: String,
                   wormholeType: String,
                   user: String,
                   pwd: String)

case class RiderMonitor(url: String,
                        domain: String,
                        esDataSourceName: String,
                        adminToken: String)

case class Maintenance(mysqlRemain: Int,
                       esRemain: Int)

case class RiderInfo(zookeeper_address: String,
                     kafka: String,
                     feedback_topic: String,
                     heartbeat_topic: String,
                     hdfslog_root_path: String,
                     spark_submit_user: String,
                     spark_app_tags: String,
                     yarn_rm1_http_url: String,
                     yarn_rm2_http_url: String)


case class LdapInfo(enabled: Boolean,
                    user: String,
                    pwd: String,
                    url: String,
                    dc: String,
                    readTimeout: Int,
                    connectTimeout: Int,
                    connectPoolEnabled: Boolean)

case class RiderFlink(homePath: String,
                      yarnQueueName: String,
                      feedbackEnabled:Boolean,
                      feedbackStateCount: Int,
                      feedbackInterval: Int,
                      defaultRate: Int,
                      defaultParallelism: Int,
                      jarPath: String,
                      clientLogPath: String,
                      kafkaSessionTimeOut: Int,
                      kafkaGroupMaxSessionTimeOut: Int)

case class RiderZookeeper(address: String, path: String)

case class DBusConfig(loginUrl: String,
                      user: String,
                      password: String,
                      namespaceUrl: String)


case class RiderKerberos(keyTab:String,
                         serverConfig:String,
                         jaasStartShellConfig:String,
                         jaasYarnConfig:String,
                         sparkPrincipal:String,
                         sparkKeyTab:String,
                         enabled:Boolean)

case class Monitor(databaseType:String)   //it will be combined with case class RiderMonitor during a follow-up operation

object RiderConfig {

  lazy val riderRootPath = s"${System.getProperty("WORMHOLE_HOME")}"

  lazy val riderServer = RiderServer(
    getStringConfig("wormholeServer.cluster.id", ""),
    config.getString("wormholeServer.host"),
    config.getInt("wormholeServer.port"),
    getDurationConfig("wormholeServer.request.timeout", 120.seconds),
    getStringConfig("wormholeServer.ui.default.language", "Chinese").toLowerCase(),
    getStringConfig("wormholeServer.host.token.secret.key", "iytr174395lclkb?lgj~8u;[=L:ljg"),
    getStringConfig("wormholeServer.admin.username", "admin"),
    getStringConfig("wormholeServer.admin.password", "admin"),
    getStringConfig("wormholeServer.normal.username", "normal"),
    getStringConfig("wormholeServer.normal.password", "normal"))

  lazy val udfRootPath = s"${spark.hdfsRoot.stripSuffix("/")}/udfjars"

  lazy val riderDomain = getStringConfig("wormholeServer.domain.url", "")

  lazy val tokenTimeout = getIntConfig("wormholeServer.token.timeout", 1)

  lazy val feedbackTopic = if (getBooleanConfig("kafka.using.cluster.suffix", default = false) && riderServer.clusterId != "")
      getStringConfig("kafka.consumer.feedback.topic","wormhole_feedback") + "_" + riderServer.clusterId
    else getStringConfig("kafka.consumer.feedback.topic","wormhole_feedback")

  lazy val heartbeatTopic = if (getBooleanConfig("kafka.using.cluster.suffix", default = false) && riderServer.clusterId != "")
      getStringConfig("kafka.consumer.heartbeat.topic", "wormhole_heartbeat") + "_" + riderServer.clusterId
    else getStringConfig("kafka.consumer.heartbeat.topic", "wormhole_heartbeat")

  lazy val pollInterval = getFiniteDurationConfig("kafka.consumer.poll-interval", FiniteDuration(20, MILLISECONDS))

  lazy val pollTimeout = getFiniteDurationConfig("kafka.consumer.poll-timeout", FiniteDuration(1, SECONDS))

  lazy val stopTimeout = getFiniteDurationConfig("kafka.consumer.stop-timeout", FiniteDuration(30, SECONDS))

  lazy val closeTimeout = getFiniteDurationConfig("kafka.consumer.close-timeout", FiniteDuration(20, SECONDS))

  lazy val commitTimeout = getFiniteDurationConfig("kafka.consumer.commit-timeout", FiniteDuration(70, SECONDS))

  lazy val wakeupTimeout = getFiniteDurationConfig("kafka.consumer.wakeup-timeout", FiniteDuration(60, SECONDS))

  lazy val maxWakeups = getIntConfig("kafka.consumer.max-wakeups", 10)

  lazy val refactor = getIntConfig("kafka.topic.refactor", 3)

  lazy val consumer = RiderKafka(config.getString("kafka.brokers.url"), config.getString("kafka.zookeeper.url"),
    feedbackTopic,
    heartbeatTopic,
    refactor,
    4,
    "wormhole_rider_group",
    "wormhole_rider_group_consumer",
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

  lazy val zk = RiderZookeeper(config.getString("zookeeper.connection.url"),
    if (riderServer.clusterId == "") getStringConfig("zookeeper.wormhole.root.path", "/wormhole")
    else s"${getStringConfig("zookeeper.wormhole.root.path", "/wormhole")}/${riderServer.clusterId}")


  lazy val appTags = getStringConfig("spark.app.tags", "wormhole")
  lazy val wormholeClientLogPath = getStringConfig("spark.wormhole.client.log.path", s"${RiderConfig.riderRootPath}/logs/streams")
  lazy val wormholeJarPath = getStringConfig("spark.wormhole.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.2.0-0.6.0-jar-with-dependencies.jar")
  lazy val wormholeKafka08JarPath = getStringConfig("spark.wormhole.kafka08.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.2.0-0.6.0-jar-with-dependencies-kafka08.jar")
  lazy val kafka08StreamNames = getStringConfig("spark.wormhole.kafka08.streams", "")
  lazy val sparkxInterfaceJarPath = getStringConfig("spark.wormhole.sparkxinterface.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-sparkxinterface-0.6.0.jar")
  lazy val wormholeUser = config.getString("spark.wormholeServer.user")
  lazy val sshPort = config.getInt("spark.wormholeServer.ssh.port")
  lazy val rm1Url = config.getString("spark.yarn.rm1.http.url")
  lazy val rm2Url = getStringConfig("spark.yarn.rm2.http.url", rm1Url)
  lazy val kafkaSessionTimeOut = getIntConfig("spark.kafka.session.timeout", 30000)
  lazy val kafkaGroupMaxSessionTimeOut = getIntConfig("spark.kafka.group.max.session.timeout.ms", 60000)
  lazy val alert = getBooleanConfig("spark.wormhole.alert.flag", false)
  lazy val metricsConfPath = getStringConfig("spark.wormhole.metric.conf.path", "")
  lazy val alertEmails = getStringConfig("spark.wormhole.alert.emails", "")
  lazy val kafkaConsumerCache = getBooleanConfig("spark.streaming.kafka.consumer.cache.enabled", false)
  lazy val streamDefaultDriverJvmConfig = getStringConfig("spark.driver.extraJavaOptions", "spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=sparkx.log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc/")
  lazy val streamDefaultExecutorJvmConfig = getStringConfig("spark.executor.extraJavaOptions", "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=sparkx.log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc")
  lazy val streamDefaultSparkConfig = getStringConfig("spark.wormhole.default.conf", s"spark.locality.wait=10ms,spark.shuffle.spill.compress=false,spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec,spark.streaming.stopGracefullyOnShutdown=true,spark.scheduler.listenerbus.eventqueue.size=1000000,spark.sql.ui.retainedExecutions=3,spark.streaming.kafka.consumer.cache.enabled=$kafkaConsumerCache")
  lazy val spark = RiderSpark(wormholeUser,
    sshPort,
    config.getString("spark.spark.home"),
    config.getString("spark.yarn.queue.name"),
    appTags,
    if (riderServer.clusterId != "") config.getString("spark.wormhole.hdfs.root.path") + "/" + riderServer.clusterId
    else config.getString("spark.wormhole.hdfs.root.path"),
    getStringConfig("spark.wormhole.hdfslog.remote.root.path", None),
    getStringConfig("spark.wormhole.hdfslog.remote.hdfs.namenode.hosts", None),
    getStringConfig("spark.wormhole.hdfslog.remote.hdfs.activenamenode.host", None),
    getStringConfig("spark.wormhole.hdfslog.remote.hdfs.namenode.ids", None),
    rm1Url, rm2Url, kafkaSessionTimeOut, kafkaGroupMaxSessionTimeOut,
    s"""
       |--class edp.wormhole.WormholeStarter \\
       |--master yarn \\
       |--deploy-mode cluster \\
       |--num-executors 0 \\
       |--conf "spark.locality.wait=10ms" \\
       |--driver-memory 0g \\
       |--executor-memory 0g \\
       |--queue default \\
       |--executor-cores 1 \\
       |--name XHtest \\
       |--files /app/yxh/log4j.properties \\
   """.stripMargin,
    wormholeClientLogPath,
    s"${RiderConfig.riderRootPath}/conf/sparkx.log4j.properties",
    wormholeJarPath,
    wormholeKafka08JarPath, kafka08StreamNames, sparkxInterfaceJarPath,
    consumer.heartbeatTopic, 2, 1, 6, 2, 1, 30, 6, 10, 100, 600,
    streamDefaultDriverJvmConfig, streamDefaultExecutorJvmConfig, streamDefaultSparkConfig, alert, metricsConfPath, alertEmails)

  lazy val es =
    if (config.hasPath("elasticSearch") && config.getString("elasticSearch.http.url").nonEmpty) {
      RiderEs(config.getString("elasticSearch.http.url"),
        if (getBooleanConfig("elasticSearch.wormhole.using.cluster.suffix", default = false))
          getStringConfig("elasticSearch.wormhole.feedback.index", "wormhole_feedback") + "_" + riderServer.clusterId
        else getStringConfig("elasticSearch.wormhole.feedback.index", "wormhole_feedback"),
        "wormhole_stats_feedback",
        getStringConfig("elasticSearch.http.user", ""),
        getStringConfig("elasticSearch.http.password", ""))
    } else null

  lazy val grafanaDomain =
    if (config.hasPath("grafana.production.domain.url")) config.getString("grafana.production.domain.url")
    else config.getString("grafana.url")

  lazy val grafana =
    if (config.hasPath("grafana") && config.getString("grafana.url").nonEmpty && config.getString("grafana.admin.token").nonEmpty)
      RiderMonitor(config.getString("grafana.url"),
        grafanaDomain, es.wormholeIndex,
        config.getString("grafana.admin.token"))
    else null

  lazy val maintenance = Maintenance(config.getInt("maintenance.mysql.feedback.remain.maxDays"),
    config.getInt("maintenance.elasticSearch.feedback.remain.maxDays"))

  lazy val dbusConfigList =
    if (config.hasPath("dbus.api"))
      config.getObjectList("dbus.api").toList.map(configObject => {
        val dbusApiConfig = configObject.toConfig
        DBusConfig(dbusApiConfig.getString("login.url"),
          dbusApiConfig.getString("login.email"),
          dbusApiConfig.getString("login.password"),
          dbusApiConfig.getString("synchronization.namespace.url"))
      })
    else List()

  lazy val dbusUrl =
    if (config.hasPath("dbus.namespace.rest.api.url"))
      config.getStringList("dbus.namespace.rest.api.url")
    else null

  lazy val kerberos=RiderKerberos(getStringConfig("kerberos.keyTab",""),
    getStringConfig("kerberos.server.config",""),
    getStringConfig("kerberos.jaas.startShell.config",""),
    getStringConfig("kerberos.jaas.yarn.config",""),
    getStringConfig("kerberos.spark.principal",""),
    getStringConfig("kerberos.spark.keyTab",""),
    getBooleanConfig("kerberos.server.enabled",false))

  lazy val riderInfo = RiderInfo(zk.address, consumer.brokers, consumer.feedbackTopic, spark.wormholeHeartBeatTopic, spark.hdfsRoot,
    spark.user, spark.appTags, spark.rm1Url, spark.rm2Url)

  lazy val ldapEnabled = getBooleanConfig("ldap.enabled", false)

  lazy val ldapUser = getStringConfig("ldap.user", "")
  lazy val ldapPwd = getStringConfig("ldap.pwd", "")
  lazy val ldapUrl = getStringConfig("ldap.url", "")
  lazy val ldapDc = getStringConfig("ldap.dc", "")
  lazy val readTimeout = getIntConfig("ldap.read.timeout", 5000)
  lazy val connectTimeout = getIntConfig("ldap.connect.timeout", 5000)
  lazy val ldapPoolEnabled = getBooleanConfig("ldap.connect.pool", true)

  lazy val ldap = LdapInfo(ldapEnabled, ldapUser, ldapPwd, ldapUrl, ldapDc, readTimeout, connectTimeout, ldapPoolEnabled)

  //set default flink stream config

  lazy val defaultFlinkConfig = FlinkDefaultConfig("", FlinkResourceConfig(2, 6, 1, 2), "")

  lazy val flink = RiderFlink(config.getString("flink.home"), config.getString("flink.yarn.queue.name"),getBooleanConfig("flink.feedback.enabled",false),getIntConfig("flink.feedback.state.count",100),getIntConfig("flink.feedback.interval",30), 1, 1,
    getStringConfig("flink.wormhole.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-flinkx_1.5.1-0.6.0-jar-with-dependencies.jar"),
    getStringConfig("flink.wormhole.client.log.path", s"$riderRootPath/logs/flows"),
    getIntConfig("spark.kafka.session.timeout", 30000),
    getIntConfig("spark.kafka.group.max.session.timeout.ms", 60000)
  )

  lazy val flinkCheckpoint = FlinkCheckpoint(getBooleanConfig("flink.checkpoint.enable",false), getIntConfig("flink.checkpoint.interval",60000), getStringConfig("flink.stateBackend",""))



  lazy val monitor=Monitor(getStringConfig("monitor.database.type","ES"))

  def getStringConfig(path: String, default: String): String = {
    if (config.hasPath(path) && config.getString(path) != null && config.getString(path) != "" && config.getString(path) != " ")
      config.getString(path)
    else default
  }

  def getStringConfig(path: String, default: Option[String]): Option[String] = {
    if (config.hasPath(path) && config.getString(path) != null && config.getString(path) != "" && config.getString(path) != " ")
      Option(config.getString(path))
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

  def getDurationConfig(path: String, default: Duration): Duration = {
    if (config.hasPath(path) && !config.getIsNull(path))
      config.getDuration(path, TimeUnit.SECONDS).seconds
    else default
  }
}
