///*-
// * <<
// * wormhole
// * ==
// * Copyright (C) 2016 - 2017 EDP
// * ==
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * >>
// */
//
//package edp.rider.kafka
//
//import edp.rider.RiderStarter.modules.config
//import edp.rider.common.RiderLogger
//
//import scala.language.postfixOps
//
//case class GetKafkaLatestOffsetException(message: String) extends Exception(message)
//
//object KafkaUtils extends RiderLogger {
//
//  lazy val kafkaHome = config.getString("kafka.home")
//  lazy val kafkaClientHost = config.getString("kafka.client.host")
//  lazy val kafkaClientUser = config.getString("kafka.client.user")
//  lazy val sshPort = config.getString("kafka.ssh.port")
//
//  def getKafkaLatestOffset(brokers: String, topic: String): String = {
////    val command = s"ssh -p$sshPort $kafkaClientUser@$kafkaClientHost $kafkaHome/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $brokers --topic $topic --time -1"
//    val command = s"$kafkaHome/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $brokers --topic $topic --time -1"
//    riderLogger.info(s"get kafka latest offset command: $command")
////    try {
////      val result: String = command !!
////      val seq = result.split("\\n").drop(1)
////      seq.map(_.replace(s"$topic:", "").trim).reverse.mkString(",")
////    } catch {
////      case ex: Exception =>
////        riderLogger.error(s"get brokers $brokers $topic topic latest offset failed", ex)
////        ""
////    }
//    ""
//  }
//
//  def getTopicPartitions(brokers: String, topic: String) = {
//    try {
//      val offset = getKafkaLatestOffset(brokers, topic)
//      offset.split("\\:").dropRight(1).mkString("").split("\\,").takeRight(1).mkString("").toInt
//    } catch {
//      case ex: Exception =>
//        riderLogger.error(s"get brokers $brokers $topic topic partition num failed", ex)
//        1
//    }
//  }
//
//}
