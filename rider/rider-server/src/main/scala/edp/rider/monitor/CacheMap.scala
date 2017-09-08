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


package edp.rider.monitor

import edp.rider.RiderStarter.modules
import edp.rider.common.RiderLogger
import edp.rider.rest.util.CommonUtils._

import scala.concurrent.Await


class CacheMap private {

  private val streamCacheMap = new scala.collection.mutable.HashMap[Long, (String, Long)]()
  private val flowCacheMap = new scala.collection.mutable.HashMap[String, Long]()

  private def getProjectId(stream_id: Long): Long = {
    var pid: Long = 0L
    streamCacheMap.get(stream_id) match {
      case Some(x) =>
        pid = x._2
      case None =>
    }
    pid
  }

  private def getStreamName(stream_id: Long): String = {
    var streamName = "default"
    streamCacheMap.get(stream_id) match {
      case Some(x) =>
        streamName = x._1
      case None =>
    }
    streamName
  }

  private def setStreamCacheMap(stream_id: Long, stream_name: String, project_id: Long) = {
    if (streamCacheMap.contains(stream_id)) {
      streamCacheMap.update(stream_id, (stream_name, project_id))
    } else {
      streamCacheMap.put(stream_id, (stream_name, project_id))
    }
  }

  private def getFlowId(flow_name: String): Long = {
    var fid: Long = 0L
    flowCacheMap.get(flow_name) match {
      case Some(x) =>
        fid = x
      case None =>
    }
    fid
  }

  private def setFlowCacheMap(flow_name: String, flow_id: Long) = {
    if (flowCacheMap.contains(flow_name)) {
      flowCacheMap.update(flow_name, flow_id)
    } else {
      flowCacheMap.put(flow_name, flow_id)
    }
  }
}

object CacheMap extends RiderLogger {
  val singleMap = new CacheMap

  def getProjectId(stream_id: Long): Long = singleMap.getProjectId(stream_id)

  def getStreamName(stream_id: Long): String = singleMap.getStreamName(stream_id)

  def setStreamIdProjectId(stream_id: Long, stream_name: String, project_id: Long) = singleMap.setStreamCacheMap(stream_id, stream_name, project_id)

  def getFlowId(flowName: String): Long = singleMap.getFlowId(flowName)

  def setFlowCacheMap(flow_name: String, flow_id: Long) = singleMap.setFlowCacheMap(flow_name, flow_id)

  def streamCacheMapRefresh: Unit =
    try {
      Await.result(modules.streamDal.getAllActiveStream, minTimeOut)
        .foreach(e => singleMap.setStreamCacheMap(e.streamId, e.streamName, e.projectId))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }


  def flowCacheMapRefresh: Unit =
    try {
      Await.result(modules.flowDal.getAllActiveFlowName, minTimeOut)
        .foreach(e => singleMap.setFlowCacheMap(e.flowName, e.flowId))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"flow cache map refresh failed", ex)
        throw ex
    }

}
