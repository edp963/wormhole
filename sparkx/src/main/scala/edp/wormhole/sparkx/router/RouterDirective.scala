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


package edp.wormhole.sparkx.router

import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.directive.Directive
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage.routerMap
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import edp.wormhole.ums.{Ums, UmsFeedbackStatus, UmsFieldType}
import edp.wormhole.util.DateUtils

import scala.collection.mutable

object RouterDirective extends Directive {


  private def registerFlowStartDirective(sourceNamespace: String,
                                         sinkNamespace: String,
                                         streamId: Long,
                                         target_kafka_broker: String,
                                         kafka_topic: String,
                                         directiveId: Long,
                                         feedbackTopicName: String,
                                         brokers: String,
                                         data_type: String): Unit = {
    //[sourceNs,([sinkNs,(brokers,topic)],ums/json)]
    synchronized {
      if (routerMap.contains(sourceNamespace)) {
        val sinkMap: mutable.Map[String, (String, String)] = routerMap(sourceNamespace)._1
        sinkMap(sinkNamespace) = (target_kafka_broker, kafka_topic)
      } else {
        routerMap(sourceNamespace) = (mutable.HashMap(sinkNamespace -> (target_kafka_broker, kafka_topic)), data_type)
      }
      WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), None, brokers)
    }
  }

  override def flowStartProcess(ums: Ums, feedbackTopicName: String, brokers: String): Unit = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    payloads.foreach(tuple => {
      val data_type = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
      val sinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sink_namespace").toString.toLowerCase
      val target_kafka_broker = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_broker").toString.toLowerCase
      val kafka_topic = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_topic").toString
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
      registerFlowStartDirective(sourceNamespace, sinkNamespace, streamId, target_kafka_broker, kafka_topic, directiveId, feedbackTopicName, brokers, data_type)
    })
  }


}
