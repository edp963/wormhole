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

import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sparkx.memorystorage.OffsetPersistenceManager
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.WormholeConfig

object DirectiveOffsetWatch extends EdpLogging {

  val timeOut = 60

  val watchRelativePath = "watch"
  var flag = false

  def offsetWatch(config: WormholeConfig, appId: String): Unit = {
    logInfo("appId=" + appId)

    val offsetPath = config.zookeeper_path + "/" + config.spark_config.stream_id + OffsetPersistenceManager.offsetRelativePath

    val watchPath = offsetPath + "/" + watchRelativePath
    logInfo("offsetWatch:"+watchPath)
    WormholeZkClient.setNodeCacheListener(config.zookeeper_address, watchPath, add(config.kafka_output.feedback_topic_name), remove, update(config.kafka_output.feedback_topic_name))

  }


  def add(feedbackTopicName: String)(path: String, data: String, time: Long): Unit = {
    logWarning("watch path can not be added")
  }


  def remove(path: String): Unit = {
    logError("watch path can not be deleted")
  }

  def update(feedbackTopicName: String)(path: String, data: String, time: Long): Unit = {
    try {
      logInfo("kafka update:"+data)
      if(flag)
        OffsetPersistenceManager.directiveList.add(OffsetPersistenceManager.getSubAndUnsubUms(data))
      else flag=true
    } catch {
      case e: Throwable => logAlert("kafka topic error:" + data, e)
    }
  }


}
