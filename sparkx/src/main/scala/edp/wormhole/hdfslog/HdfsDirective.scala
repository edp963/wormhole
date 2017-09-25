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


package edp.wormhole.hdfslog

import edp.wormhole.common.FeedbackPriority
import edp.wormhole.common.util.DateUtils
import edp.wormhole.core.Directive
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums.{Ums, UmsFeedbackStatus, UmsFieldType}

object HdfsDirective extends Directive  {
  override def flowStartProcess(ums: Ums, feedbackTopicName: String, brokers: String): Unit = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    payloads.foreach(tuple => {
      val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val namespace_rule = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "namespace_rule").toString.toLowerCase
      try{
      val hour_duration = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "hour_duration").toString.toLowerCase.toInt
      HdfsMainProcess.directiveNamespaceRule(namespace_rule) = hour_duration
      WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), None, brokers)

      } catch {
        case e: Throwable =>
          logAlert("registerFlowStartDirective,sourceNamespace:" + namespace_rule , e)
          WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, e.getMessage), None, brokers)

      }
    })
  }



}
