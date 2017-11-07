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

package edp.wormhole.core

import java.util.concurrent.ConcurrentLinkedQueue

import edp.wormhole.common.FeedbackPriority
import edp.wormhole.common.util.DateUtils
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.udf.UdfRegister
import edp.wormhole.ums.{Ums, UmsFeedbackStatus, UmsFieldType}
import edp.wormhole.ums.UmsProtocolUtils.feedbackDirective
import org.apache.spark.sql.SparkSession

object UdfDirective extends EdpLogging {

  val udfDirectiveQueue = new ConcurrentLinkedQueue[Ums]()

  def addUdfProcess(ums: Ums): Unit = {
    udfDirectiveQueue.add(ums)
  }

  def getUdfUms: Ums = {
    udfDirectiveQueue.poll()
  }

  def registerUdfProcess(feedbackTopicName: String, brokers: String, session: SparkSession): Unit = {
    while (udfDirectiveQueue.size() > 0) {
      val ums = getUdfUms
      val payloads = ums.payload_get
      val schemas = ums.schema.fields_get
      payloads.foreach(tuple => {
        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
        val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
        val udfName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_name").toString
        val udfClassFullname = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_class_fullname").toString
        val udfJarPath = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_jar_path").toString
        //        rdd.foreachPartition(_=>{
        //          UdfUtils.removeUdf(udfName)
        //        })
        try {
          UdfRegister.register(udfName, udfClassFullname, udfJarPath, session)
          WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), None, brokers)
        } catch {
          case e: Throwable =>
            logError(udfName + " register fail", e)
            WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, ""), None, brokers)
        }
      })
    }
  }

}
