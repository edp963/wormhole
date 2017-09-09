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

import edp.rider.RiderStarter.modules.config
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration.{FiniteDuration, _}

case class RiderServer(host: String, port: Int)

case class RiderKafka(brokers: String,
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
                      rest_api: String,
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
                      driverExtraConf: String,
                      executorExtraConf: String
                     )

case class RiderEs(url: String,
                   wormholeIndex: String,
                   wormholeType: String)

case class RiderMonitor(url: String,
                        domain: String,
                        adminUser: String,
                        adminToken: String,
                        viewUser: String,
                        viewToken: String)

case class Maintenance(mysqlRemain: Int,
                       esRemain: Int)

case class RiderInfo(consumer: RiderKafka,
                     zk: String,
                     db: RiderDatabase,
                     spark: RiderSpark,
                     monitor: RiderMonitor)

object RiderConfig {

  lazy val riderRootPath = s"${System.getenv("WORMHOLE_HOME")}"

  lazy val riderServer = RiderServer(config.getString("wormholeServer.host"), config.getInt("wormholeServer.port"))

  lazy val tokenTimeout =
    if (config.hasPath("wormholeServer.token.timeout")) config.getInt("wormholeServer.token.timeout")
    else 36000

  lazy val feedbackTopic =
    if (config.hasPath("kafka.brokers.feedback.topic")) config.getString("kafka.brokers.feedback.topic")
    else "wormhole_feedback"
  lazy val consumer = RiderKafka(config.getString("kafka.brokers.url"),
    feedbackTopic,
    4,
    "wormhole_rider_group",
    "wormhole_rider_group_consumer1",
    false,
    new ByteArrayDeserializer, new StringDeserializer,
    FiniteDuration(30, MICROSECONDS),
    FiniteDuration(30, MICROSECONDS),
    FiniteDuration(30, SECONDS),
    FiniteDuration(20, SECONDS),
    FiniteDuration(15, SECONDS),
    FiniteDuration(3, SECONDS),
    10,
    "akka.kafka.default-dispatcher"
  )

  lazy val zk = config.getString("zookeeper.connection.url")

  lazy val db = RiderDatabase(config.getString("mysql.db.url"), config.getString("mysql.db.user"), config.getString("mysql.db.password"))

  lazy val appTags =
    if (config.hasPath("spark.app.tags")) config.getString("spark.app.tags") else "wormhole"
  lazy val wormholeClientLogPath =
    if (config.hasPath("spark.wormhole.client.log.root.path"))
      config.getString("spark.wormhole.client.log.root.path").concat("/")
    else s"${RiderConfig.riderRootPath}/logs/streams/"
  lazy val wormholeJarPath =
    if (config.hasPath("spark.wormhole.jar.path")) config.getString("spark.wormhole.jar.path")
    else s"${RiderConfig.riderRootPath}/lib/wormhole-ums_1.3-sparkx_2.2.0-0.3.0-SNAPSHOTS-jar-with-dependencies.jar"
  lazy val wormholeKafka08JarPath =
    if (config.hasPath("spark.wormhole.kafka08.jar.path")) config.getString("spark.wormhole.kafka08.jar.path")
    else s"${RiderConfig.riderRootPath}/lib/wormhole-ums_1.3-spark_2.2.0-0.3.0-SNAPSHOTS-jar-with-dependencies-kafka08.jar"
  lazy val kafka08StreamNames =
    if (config.hasPath("spark.wormhole.kafka08.streams")) config.getString("spark.wormhole.kafka08.streams")
    else ""
  lazy val spark = RiderSpark(
    config.getString("spark.wormholeServer.user"),
    config.getInt("spark.wormholeServer.ssh.port"),
    config.getString("spark.spark.home"),
    config.getString("spark.queue.name"),
    appTags,
    config.getString("spark.wormhole.hdfs.root.path"),
    config.getString("spark.yarn.active.resourceManager.http.url"),
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
    kafka08StreamNames, "wormhole_heartbeat", 2, 1, 6, 4, 2, 100,
    "spark.driver.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc/",
    "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -Dlog4j.configuration=log4j.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc"
  )

  lazy val es = RiderEs(config.getString("elasticSearch.http.url"), "wormhole_feedback", "wormhole_stats_feedback")

  lazy val domain =
    if (config.hasPath("grafana.production.domain.url")) config.getString("grafana.production.domain.url")
    else config.getString("grafana.url")

  lazy val grafana =
    if (config.hasPath("grafana"))
      RiderMonitor(config.getString("grafana.url"),
        domain,
        config.getString("grafana.admin.user"),
        config.getString("grafana.admin.token"),
        config.getString("grafana.viewer.user"),
        config.getString("grafana.viewer.token"))
    else null

  lazy val maintenance = Maintenance(config.getInt("maintenance.mysql.feedback.remain.maxDays"),
    config.getInt("maintenance.elasticSearch.feedback.remain.maxDays"))

  lazy val dbusUrl = config.getStringList("dbus.namespace.rest.api.url")

  lazy val riderInfo = RiderInfo(consumer, zk, db, spark, grafana)
}
