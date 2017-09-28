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


package edp.rider.rest.util

import edp.rider.RiderStarter.modules
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.entities.{Directive, SimpleTopic}
import edp.rider.rest.util.CommonUtils._
import edp.rider.zookeeper.PushDirective
import edp.wormhole.ums.UmsProtocolType._
import com.alibaba.fastjson.JSON
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.util.parsing.json.JSONObject

object StreamUtils extends RiderLogger {
  def sendTopicDirective(streamId: Long, topicSeq: Seq[SimpleTopic], userId: Long) = {
    try {
      val directiveSeq = new ArrayBuffer[Directive]
      val zkConURL: String = RiderConfig.zk
      topicSeq.foreach({
        topic =>
          val tuple = Seq(streamId, currentMicroSec, topic.name, topic.rate, topic.partitionOffsets).mkString("#")
          directiveSeq += Directive(0, DIRECTIVE_TOPIC_SUBSCRIBE.toString, streamId, 0, tuple, zkConURL, currentSec, userId)
      })
      val directives: Seq[Directive] = if (directiveSeq.isEmpty) directiveSeq
      else {
        Await.result(modules.directiveDal.insert(directiveSeq), minTimeOut)
      }
      val blankTopic = Directive(0, null, streamId, 0, Seq(streamId, currentMicroSec, RiderConfig.spark.wormholeHeartBeatTopic, RiderConfig.spark.topicDefaultRate, "0:0").mkString("#"), zkConURL, currentSec, userId)
      val directiveNew = directives.to[mutable.ArrayBuffer] += blankTopic
      val topicUms = directiveNew.map({
        directive =>
          val topicInfo = directive.directive.split("#")
          s"""
             |{
             |"protocol": {
             |"type": "${DIRECTIVE_TOPIC_SUBSCRIBE.toString}"
             |},
             |"schema": {
             |"namespace": "",
             |"fields": [
             |{
             |"name": "directive_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "stream_id",
             |"type": "long",
             |"nullable": false
             |},
             |{
             |"name": "ums_ts_",
             |"type": "datetime",
             |"nullable": false
             |},
             |{
             |"name": "topic_name",
             |"type": "string",
             |"nullable": false
             |},
             |{
             |"name": "topic_rate",
             |"type": "int",
             |"nullable": false
             |},
             |{
             |"name": "partitions_offset",
             |"type": "string",
             |"nullable": false
             |}
             |]
             |},
             |"payload": [
             |{
             |"tuple": [${directive.id}, ${topicInfo(0)}, "${topicInfo(1)}", "${topicInfo(2)}", ${topicInfo(3)}, "${topicInfo(4)}"]
             |}
             |]
             |}
        """.stripMargin.replaceAll("[\\n\\t\\r]+", "")
      }).mkString("\n")
      PushDirective.sendTopicDirective(streamId, topicUms, directiveNew.head.zkPath)
      riderLogger.info(s"user $userId send ${DIRECTIVE_TOPIC_SUBSCRIBE.toString} directives success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def removeAndSendDirective(streamId: Long, userId: Long) = {
    try {
      val topicSeq: Seq[SimpleTopic] = modules.streamDal.getSimpleTopicSeq(streamId)
      if (topicSeq.nonEmpty) {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      } else {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      }
      sendTopicDirective(streamId, topicSeq, userId)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"remove and send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def removeStreamDirective(streamId: Long, userId: Long) = {
    try {
      val topicSeq = modules.streamDal.getSimpleTopicSeq(streamId)
      if (topicSeq.nonEmpty) {
        PushDirective.removeTopicDirective(streamId, RiderConfig.zk)
        riderLogger.info(s"user $userId remove topic directive success.")
      } else {
        PushDirective.removeTopicDirective(streamId)
        riderLogger.info(s"user $userId remove topic directive success.")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"remove and send stream $streamId topic directive failed", ex)
        throw ex
    }
  }

  def getDuration(launchConfig: String): Int = {
    if (launchConfig != null && launchConfig != "") {
      if (JSON.parseObject(launchConfig).containsKey("durations"))
        JSON.parseObject(launchConfig).getIntValue("durations")
      else 10
    } else 10
  }

  def checkConfigFormat(startConfig: String, launchConfig: String, sparkConfig: String) = {
    (isJson(startConfig), isJson(launchConfig), isKeyEqualValue(sparkConfig)) match {
      case (true, true, true) => (true, "success")
      case (true, true, false) => (false, s"sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (true, false, true) => (false, s"launchConfig $launchConfig is not json type")
      case (true, false, false) => (false, s"launchConfig $launchConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (false, true, true) => (false, s"startConfig $startConfig is not json type")
      case (false, true, false) => (false, s"startConfig $startConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
      case (false, false, true) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type")
      case (false, false, false) => (false, s"startConfig $startConfig is not json type, launchConfig $launchConfig is not json type, sparkConfig $sparkConfig doesn't meet key=value,key1=value1 format")
    }
  }
}
