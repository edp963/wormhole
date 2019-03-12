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

import com.alibaba.fastjson.{JSONArray, JSONObject}
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.{UmsProtocolType, UmsProtocolUtils, UmsWatermark}
import edp.wormhole.util.DateUtils
import org.apache.log4j.Logger

import scala.collection.mutable

object FlinkxUtils {

  private lazy val logger = Logger.getLogger(this.getClass)

  def setFlowErrorMessage(incrementTopicList:Seq[String],
                          topicPartitionOffset:JSONObject,
                          config: WormholeFlinkxConfig,
                          sourceNamespace:String,
                          sinkNamespace:String,
                          errorCount:Int,
                          errorMsg:String,
                          batchId:String,
                          protocolType: String,
                          flowId:Long,
                          streamId:Long,
                          errorPattern:String): Unit ={
    val ts: String = null
    val tmpJsonArray = new JSONArray()
    val sourceTopicSet = mutable.HashSet.empty[String]
    sourceTopicSet ++ incrementTopicList
    sourceTopicSet.foreach(topic=>{
      tmpJsonArray.add(topicPartitionOffset.getJSONObject(topic))
    })

    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name,
      FeedbackPriority.feedbackPriority, UmsProtocolUtils.feedbackFlowError(sourceNamespace,
        streamId, DateUtils.currentDateTime, sinkNamespace, UmsWatermark(ts),
        UmsWatermark(ts), errorCount, errorMsg, batchId, null,protocolType,
        flowId,errorPattern),
      Some(UmsProtocolType.FEEDBACK_FLOW_ERROR + "." + flowId),
      config.kafka_output.brokers)
  }

}
