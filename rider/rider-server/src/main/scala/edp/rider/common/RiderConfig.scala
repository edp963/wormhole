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
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration.{FiniteDuration, _}

case class RiderServer(host: String, port: Int, adminUser: String, adminPwd: String, normalUser: String, normalPwd: String)

case class RiderKafka(brokers: String,
                      zkUrl: String,
                      topic: String,
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
                      spark_home: String,
                      queue_name: String,
                      app_tags: String,
                      hdfs_root: String,
                      rm1Url: String,
                      rm2Url: String,
                      startShell: String,
                      clientLogRootPath: String,
                      sparkLog4jPath: String,
                      jarPath: String,
                      kafka08JarPath: String,
                      kafka08StreamNames: String,
                      wormholeHeartBeatTopic: String,
                      driverMemory: Int,
                      driverCores: Int,
                      executorNum: Int,
                      executorMemory: Int,
                      executorCores: Int,
                      topicDefaultRate: Int,
                      jobMaxRecordPerPartitionProcessed: Int,
                      driverExtraConf: String,
                      executorExtraConf: String,
                      sparkConfig: String)

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

case class RiderInfo(zookeeper: String,
                     kafka: String,
                     feedback_topic: String,
                     heartbeat_topic: String,
                     hdfslog_root_path: String,
                     spark_submit_user: String,
                     spark_app_tags: String,
                     yarn_rm1_http_url: String,
                     yarn_rm2_http_url: String)


object RiderConfig {

  lazy val riderRootPath = s"${System.getenv("WORMHOLE_HOME")}"

  lazy val riderServer = RiderServer(
    config.getString("wormholeServer.host"), config.getInt("wormholeServer.port"),
    getStringConfig("wormholeServer.admin.username", "admin"),
    getStringConfig("wormholeServer.admin.password", "admin"),
    getStringConfig("wormholeServer.normal.username", "normal"),
    getStringConfig("wormholeServer.normal.password", "normal"))

  lazy val udfRootPath = s"${spark.hdfs_root.stripSuffix("/")}/udfjar"

  lazy val riderDomain = getStringConfig("wormholeServer.domain.url", "")

  lazy val tokenTimeout = getIntConfig("wormholeServer.token.timeout", 1)

  lazy val feedbackTopic = getStringConfig("kafka.consumer.feedback.topic", "wormhole_feedback")

  lazy val pollInterval = getFiniteDurationConfig("kafka.consumer.poll-interval", FiniteDuration(30, MILLISECONDS))

  lazy val pollTimeout = getFiniteDurationConfig("kafka.consumer.poll-timeout", FiniteDuration(30, MILLISECONDS))

  lazy val stopTimeout = getFiniteDurationConfig("kafka.consumer.stop-timeout", FiniteDuration(30, SECONDS))

  lazy val closeTimeout = getFiniteDurationConfig("kafka.consumer.close-timeout", FiniteDuration(20, SECONDS))

  lazy val commitTimeout = getFiniteDurationConfig("kafka.consumer.commit-timeout", FiniteDuration(15, SECONDS))

  lazy val wakeupTimeout = getFiniteDurationConfig("kafka.consumer.wakeup-timeout", FiniteDuration(3, SECONDS))

  lazy val maxWakeups = getIntConfig("kafka.consumer.max-wakeups", 10)

  lazy val consumer = RiderKafka(config.getString("kafka.brokers.url"), config.getString("kafka.zookeeper.url"),
    feedbackTopic,
    4,
    "wormhole_rider_group",
    "wormhole_rider_group_consumer1",
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

  lazy val zk = config.getString("zookeeper.connection.url")

  lazy val appTags = getStringConfig("spark.app.tags", "wormhole")
  lazy val wormholeClientLogPath = getStringConfig("spark.wormhole.client.log.root.path", s"${RiderConfig.riderRootPath}/logs/streams").concat("/")
  lazy val wormholeJarPath = getStringConfig("spark.wormhole.jar.path", s"${RiderConfig.riderRootPath}/lib/wormhole-ums_1.3-sparkx_2.2.0-0.3.0-SNAPSHOTS-jar-with-dependencies.jar")
  lazy val wormholeKafka08JarPath = getStringConfig("spark.wormhole.kafka08.jar.path", s"${RiderConfig.riderRootPath}/lib/wormhole-ums_1.3-sparkx_2.2.0-0.3.0-SNAPSHOTS-jar-with-dependencies-kafka08.jar")
  lazy val kafka08StreamNames = getStringConfig("spark.wormhole.kafka08.streams", "")
  lazy val wormholeUser = config.getString("spark.wormholeServer.user")
  lazy val sshPort = config.getInt("spark.wormholeServer.ssh.port")
  lazy val rm1Url = config.getString("spark.yarn.rm1.http.url")
  lazy val rm2Url = getStringConfig("spark.yarn.rm2.http.url", "")

  lazy val spark = RiderSpark(wormholeUser,
    sshPort,
    config.getString("spark.spark.home"),
    config.getString("spark.yarn.queue.name"),
    appTags,
    config.getString("spark.wormhole.hdfs.root.path"),
    rm1Url, rm2Url,
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
    wormholeKafka08JarPath,
    kafka08StreamNames, "wormhole_heartbeat", 2, 1, 6, 4, 2, 100, 600,
    "spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=sparkx.log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc/",
    "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=sparkx.log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc",
    "spark.locality.wait=10ms,spark.shuffle.spill.compress=false,spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec,spark.streaming.stopGracefullyOnShutdown=true,spark.scheduler.listenerbus.eventqueue.size=1000000,spark.sql.ui.retainedExecutions=3"
  )

  lazy val es =
    if (config.hasPath("elasticSearch") && config.getString("elasticSearch.http.url").nonEmpty) {
      RiderEs(config.getString("elasticSearch.http.url"),
        "wormhole_feedback",
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
        grafanaDomain, "wormhole_stats",
        config.getString("grafana.admin.token"))
    else null

  lazy val maintenance = Maintenance(config.getInt("maintenance.mysql.feedback.remain.maxDays"),
    config.getInt("maintenance.elasticSearch.feedback.remain.maxDays"))

  lazy val dbusUrl =
    if (config.hasPath("dbus.namespace.rest.api.url"))
      config.getStringList("dbus.namespace.rest.api.url")
    else null

  lazy val riderInfo = RiderInfo(zk, consumer.brokers, consumer.topic, spark.wormholeHeartBeatTopic, spark.hdfs_root,
    spark.user, spark.app_tags, spark.rm1Url, spark.rm2Url)

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
