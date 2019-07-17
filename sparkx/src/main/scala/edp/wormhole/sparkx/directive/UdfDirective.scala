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

package edp.wormhole.sparkx.directive

import java.util.concurrent.ConcurrentLinkedQueue

import edp.wormhole.sparkextension.udf.UdfRegister
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.{Ums, UmsFieldType}
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
        //        val streamId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "stream_id").toString.toLong
        //        val directiveId = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "directive_id").toString.toLong
        val udfName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_name").toString
        val udfClassFullName = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_class_fullname").toString
        val udfJarPath = UmsFieldType.umsFieldValue(tuple.tuple, schemas, "udf_jar_path").toString
        try {
          // todo udf register feedback暂未使用，先不发送消息
          UdfRegister.register(udfName, udfClassFullName, udfJarPath, session, true)
          //          WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.feedbackPriority, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), Some(UmsProtocolType.FEEDBACK_DIRECTIVE + "." + streamId), brokers)
        } catch {
          case e: Throwable =>
            logError(udfName + " register fail", e)
          //            WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.feedbackPriority, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.FAIL, streamId, ""), Some(UmsProtocolType.FEEDBACK_DIRECTIVE + "." + streamId), brokers)
        }
      })
    }
  }

}
