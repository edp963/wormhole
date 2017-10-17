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


package edp.wormhole.common

import edp.wormhole.common.util.{CommonUtils, DateUtils}
import edp.wormhole.common.util.DateUtils.currentDateTime
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums._
import edp.wormhole.ums.UmsSchemaUtils.toUms
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal
import org.apache.spark.sql.functions._
import java.sql.Timestamp

import edp.wormhole.core.{PartitionOffsetConfig, WormholeConfig}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType

object WormholeUtils extends EdpLogging {

  def keys2keyList(keys: String): List[String] = if (keys == null) Nil else keys.split(",").map(CommonUtils.trimBothBlank).toList


  def getTypeNamespaceFromKafkaKey(key: String): (UmsProtocolType, String) = {
    val keys = key.split("\\.")
    if (keys.length > 7) (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), keys.slice(1, 8).mkString(".").toLowerCase)
    else (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), "")
  }

  def json2Ums(json: String): Ums = {
    try {
      toUms(json)
    } catch {
      case NonFatal(e) => logError(s"message convert failed:\n$json", e)
        Ums(UmsProtocol(UmsProtocolType.FEEDBACK_DIRECTIVE), UmsSchema("defaultNamespace"), None)
    }
  }

  def jsonGetValue(namespace: String, protocolType: UmsProtocolType, json: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Map[String, Any], String)] , matchNamespace: String, dataType: String): (Seq[UmsField], Seq[UmsTuple]) = {
    if (jsonSourceParseMap.contains((protocolType, namespace))) {
      //todo here@
      val b: Seq[UmsTuple] = null
      (jsonSourceParseMap((protocolType, namespace))._1, b)
    } else {
      val ums = json2Ums(json)
      (ums.schema.fields_get, ums.payload_get)
    }
  }

  def sendTopicPartitionOffset(offsetInfo: ArrayBuffer[OffsetRange], feedbackTopicName: String, config: WormholeConfig): Unit = {
    val topicConfigMap = mutable.HashMap.empty[String, ListBuffer[PartitionOffsetConfig]]

    offsetInfo.foreach { offsetRange =>
      logInfo(s"----------- $offsetRange")
      val topicName = offsetRange.topic
      val partition = offsetRange.partition
      val offset = offsetRange.untilOffset
      logInfo("brokers:" + config.kafka_output.brokers + ",topic:" + feedbackTopicName)
      if (!topicConfigMap.contains(topicName)) topicConfigMap(topicName) = new ListBuffer[PartitionOffsetConfig]
      topicConfigMap(topicName) += PartitionOffsetConfig(partition, offset)
    }

    val tp: Map[String, String] = topicConfigMap.map { case (topicName, partitionOffsetList) => {
      (topicName, partitionOffsetList.map(it => it.partition_num + ":" + it.offset).sorted.mkString(","))
    }
    }.toMap
    WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority2, WormholeUms.feedbackStreamTopicOffset(currentDateTime, config.spark_config.stream_id, tp), None, config.kafka_output.brokers)
  }

  //  def doOtherData(otherDataArray: Array[String], config: WormholeConfig,processedMap: Map[String, String]): Unit = {
  //    if (otherDataArray.nonEmpty) {
  //      otherDataArray.foreach(
  //        row => {
  //          val ums = json2Ums(row)
  //          val namespace = ums.schema.namespace
  //          val umsts = ums.payload_get.head.tuple.head
  //          ums.protocol.`type` match {
  //            case UmsProtocolType.DATA_BATCH_TERMINATION =>
  //              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority1,
  //                WormholeUms.feedbackDataBatchTermination(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
  ////              logAlert("Receive DATA_BATCH_TERMINATION, kill the application")
  ////              val pb = new ProcessBuilder("yarn","application", "-kill", SparkUtils.getAppId())
  ////              pb.start()
  //            case UmsProtocolType.DATA_INCREMENT_TERMINATION =>
  //              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority1,
  //                WormholeUms.feedbackDataIncrementTermination(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
  //            case UmsProtocolType.DATA_INCREMENT_HEARTBEAT =>
  //
  //              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority2,
  //                WormholeUms.feedbackDataIncrementHeartbeat(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
  //            case _ => logWarning(ums.protocol.`type`.toString + " is not supported")
  //          }
  //        }
  //      )
  //    }
  //  }

  def getIncrementByTs(df: DataFrame, keys: List[String], from_yyyyMMddHHmmss: String, to_yyyyMMddHHmmss: String): DataFrame = {
    val fromTs = DateUtils.dt2timestamp(from_yyyyMMddHHmmss)
    val toTs = DateUtils.dt2timestamp(DateUtils.dt2dateTime(to_yyyyMMddHHmmss).plusSeconds(1).minusMillis(1))
    getIncrementByTs(df, keys, fromTs, toTs)
  }

  private def getIncrementByTs(df: DataFrame, keys: List[String], fromTs: Timestamp, toTs: Timestamp): DataFrame = {
    val w = Window
      .partitionBy(keys.head, keys.tail: _*)
      .orderBy(df(UmsSysField.ID.toString).desc)
    //    val w = Window.partitionBy(keys.head, keys.tail: _*).orderBy(df(UmsSysField.TS.toString).desc)

    df.where(df(UmsSysField.TS.toString) >= fromTs).where(df(UmsSysField.TS.toString) <= toTs).withColumn("rn", row_number.over(w)).where("rn = 1").drop("rn").filter("ums_op_ != 'd'")
  }

}

