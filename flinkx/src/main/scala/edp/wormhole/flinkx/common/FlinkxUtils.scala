/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole.flinkx.common

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.{UmsProtocolType, UmsProtocolUtils, UmsWatermark}
import edp.wormhole.util.DateUtils
import org.apache.log4j.Logger


object FlinkxUtils {

  private lazy val logger = Logger.getLogger(this.getClass)

  def sendFlowErrorMessage(msg: String,
                           config: WormholeFlinkxConfig,
                           flowId: Long): Unit = {

    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name,
      FeedbackPriority.feedbackPriority, msg,
      Some(UmsProtocolType.FEEDBACK_FLOW_ERROR + "." + flowId),
      config.kafka_output.brokers)
  }

  def getFlowErrorMessage(dataInfo:String,
                          sourceNamespace: String,
                          sinkNamespace: String,
                          errorCount: Int,
                          error: Throwable,
                          batchId: String,
                          protocolType: String,
                          flowId: Long,
                          streamId: Long,
                          errorPattern: String): String = {
    val ts: String = null
    val errorMaxLength = 2000

    val errorMsg = if(error!=null){
      val first = if(error.getStackTrace!=null&&error.getStackTrace.nonEmpty) error.getStackTrace.head.toString else ""
      val errorAll = error.toString + "\n" + first
      errorAll.substring(0, math.min(errorMaxLength, errorAll.length))
    } else null
    UmsProtocolUtils.feedbackFlowError(sourceNamespace,
      streamId, DateUtils.currentDateTime, sinkNamespace, UmsWatermark(ts),
      UmsWatermark(ts), errorCount, errorMsg, batchId, null, protocolType,
      flowId, errorPattern)
  }

  def getDefaultKeyConfig(specialConfig: Option[StreamSpecialConfig]): Boolean = {
    //log.info(s"stream special config is $specialConfig")
    try {
      specialConfig match {
        case Some(_) =>
          specialConfig.get.useDefaultKey.getOrElse(true)
        case None =>
          true
      }
    } catch {
      case e: Throwable =>
        logger.error("parse stream specialConfig error, ", e)
        true
    }
  }

  def getDefaultKey(key: String, namespace: String, defaultKey: Boolean): String = {
    /*if (key != null) {
      log.info(s"getDefaultKey: key $key")
    } else {
      log.info(s"getDefaultKey: key null")
    }
    if(namespaces != null) {
      log.info(s"getDefaultKey: namespaces $namespaces, defaultKey $defaultKey")
    } else {
      log.info(s"getDefaultKey: namespaces null, $defaultKey")
    }*/
    if (!isRightKey(key) && null != namespace && namespace.nonEmpty && defaultKey) {
      //log.info(s"getDefaultKey: use default namespace ${namespaces.head} as kafka key, all namespace is $namespaces")
      UmsProtocolType.DATA_INCREMENT_DATA.toString + "." + namespace
    } else {
      key
    }
  }

  def isRightKey(key: String): Boolean = {
    if (null == key || key.isEmpty) {
      false
    } else {
      if (key.split(".").length < 5) {
        false
      } else {
        true
      }
    }
  }

}
