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


package edp.wormhole.router

import edp.wormhole.directive.Directive

import edp.wormhole.ums.{Ums, UmsFieldType}


import scala.collection.mutable


object RouterDirective  extends Directive {


  private def registerFlowStartDirective(sourceNamespace:String,
                                         sinkNamespace:String,
                                         target_kafka_broker: String,
                                         kafka_topic: String,
                                         directiveId: Long,
                                         feedbackTopicName: String,
                                         brokers: String): Unit = {
    if (RouterMainProcess.routerMap.contains(sourceNamespace)) {
      if (RouterMainProcess.routerMap(sourceNamespace).contains(sinkNamespace)) {
        RouterMainProcess.routerMap(sourceNamespace)(sinkNamespace) = (target_kafka_broker, kafka_topic)
      } else {
        RouterMainProcess.routerMap(sourceNamespace) = mutable.HashMap(sinkNamespace -> (target_kafka_broker, kafka_topic))
      }
    } else {
      RouterMainProcess.routerMap(sourceNamespace) = mutable.HashMap(sinkNamespace -> (target_kafka_broker, kafka_topic))
    }

  }

  override def flowStartProcess(ums: Ums, feedbackTopicName: String, brokers: String): Unit = {
    val payloads = ums.payload_get
    val schemas = ums.schema.fields_get
    val sourceNamespace = ums.schema.namespace.toLowerCase
    payloads.foreach(tuple => {
      val sinkNamespace = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "sink_namespace").toString.toLowerCase
      val target_kafka_broker = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_broker").toString.toLowerCase
      val kafka_topic = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "kafka_topic").toString
      val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
      registerFlowStartDirective(sourceNamespace, sinkNamespace,target_kafka_broker, kafka_topic, directiveId, feedbackTopicName, brokers)
    })
  }


}
