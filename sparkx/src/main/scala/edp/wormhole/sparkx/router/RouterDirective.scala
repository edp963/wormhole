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
                                         data_type: String,
                                         flowId:Long,
                                         sourceIncrementTopicList:List[String]): String = {
    //[sourceNs,([sinkNs,(brokers,topic)],ums/json)]
    synchronized {
      if (routerMap.contains(sourceNamespace)) {
        val sinkMap: mutable.Map[String, RouterFlowConfig] = routerMap(sourceNamespace)._1
        sinkMap(sinkNamespace) = RouterFlowConfig(target_kafka_broker, kafka_topic,flowId,sourceIncrementTopicList)
      } else {
        routerMap(sourceNamespace) = (mutable.HashMap(sinkNamespace -> RouterFlowConfig(target_kafka_broker, kafka_topic,flowId,sourceIncrementTopicList)), data_type)
      }
      feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId,flowId, "")
    }
  }

  override def flowStartProcess(ums: Ums): String = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    val tuple = payloads.head
    val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
    val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
    val flowId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "flow_id").toString.toLong
    try {
      val data_type = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "data_type").toString.toLowerCase
      val sinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sink_namespace").toString.toLowerCase
      val target_kafka_broker = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_broker").toString.toLowerCase
      val kafka_topic = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_topic").toString
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong

      val sourceIncrementTopicList = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "source_increment_topic").toString.split(",").toList
      registerFlowStartDirective(sourceNamespace, sinkNamespace, streamId, target_kafka_broker, kafka_topic, directiveId, data_type,flowId,sourceIncrementTopicList)
    } catch {
      case e: Throwable =>
        logAlert("registerFlowStartDirective,sourceNamespace:" + sourceNamespace, e)
        feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId,flowId, e.getMessage)
    }
  }

}
