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


package edp.rider.service

import edp.rider.RiderStarter.modules
import edp.rider.common.RiderLogger
import edp.rider.rest.util.CommonUtils._

import scala.collection.mutable
import scala.concurrent.Await

class CacheMap private {

  private case class StreamMapKey(streamId: Long)

  private case class StreamMapValue(streamName: String, projectId: Long)

  private val streamCacheMap = new scala.collection.mutable.HashMap[StreamMapKey, StreamMapValue]()

  private case class FlowMapKey(flowName: String)

  private case class FlowMapValue(flowId: Long)

  private val flowCacheMap = new scala.collection.mutable.HashMap[FlowMapKey, FlowMapValue]()

  private case class OffsetMapkey(streamId: Long, topicName: String, partitionId: Int)

  private case class OffsetMapValue(offset: Long)

  private val topicLatestOffsetMap = new mutable.HashMap[OffsetMapkey, OffsetMapValue]()

  private def getProjectId(streamId: Long): Long = {
    var pid: Long = 0L
    streamCacheMap.get(StreamMapKey(streamId)) match {
      case Some(x) =>
        pid = x.projectId
      case None =>
    }
    pid
  }

  private def getStreamName(streamId: Long): String = {
    var streamName = "default"
    streamCacheMap.get(StreamMapKey(streamId)) match {
      case Some(x) =>
        streamName = x.streamName
      case None =>
    }
    streamName
  }

  private def setStreamCacheMap(stream_id: Long, stream_name: String, project_id: Long): Unit = {
    if (streamCacheMap.contains(StreamMapKey(stream_id))) {
      streamCacheMap.update(StreamMapKey(stream_id), StreamMapValue(stream_name, project_id))
    } else {
      streamCacheMap.put(StreamMapKey(stream_id), StreamMapValue(stream_name, project_id))
    }
  }

  private def getFlowId(flow_name: String): Long = {
    var fid: Long = 0L
    flowCacheMap.get(FlowMapKey(flow_name)) match {
      case Some(x) =>
        fid = x.flowId
      case None =>
    }
    fid
  }

  private def setFlowCacheMap(flow_name: String, flow_id: Long): Unit = {
    if (flowCacheMap.contains(FlowMapKey(flow_name))) {
      flowCacheMap.update(FlowMapKey(flow_name), FlowMapValue(flow_id))
    } else {
      flowCacheMap.put(FlowMapKey(flow_name), FlowMapValue(flow_id))
    }
  }

  private def setOffsetMap(streamId: Long, topicName: String, partitionId: Int, offset: Long) = {
    if (topicLatestOffsetMap.contains(OffsetMapkey(streamId, topicName, partitionId))) {
      topicLatestOffsetMap.update(OffsetMapkey(streamId, topicName, partitionId), OffsetMapValue(offset))
    } else {
      topicLatestOffsetMap.put(OffsetMapkey(streamId, topicName, partitionId), OffsetMapValue(offset))
    }
  }

}

object CacheMap extends RiderLogger {
  val singleMap = new CacheMap

  def setStreamIdProjectId(streamId: Long, streamName: String, projectId: Long): Unit =
    singleMap.setStreamCacheMap(streamId, streamName, projectId)

  def setFlowCacheMap(flow_name: String, flow_id: Long): Unit =
    singleMap.setFlowCacheMap(flow_name, flow_id)

  def setOffsetMap(streamId: Long, topicName: String, partitionId: Int, offset: Long): Unit =
    singleMap.setOffsetMap(streamId, topicName, partitionId, offset)

  def getProjectId(streamId: Long): Long =
    try {
      val pid = singleMap.getProjectId(streamId)
      if (pid == 0) {
        singleMap.getProjectId(streamId)
      } else pid
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }


  def getStreamName(streamId: Long): String =
    try {
      val streamName = singleMap.getStreamName(streamId)
      if (streamName == "default") {
        singleMap.getStreamName(streamId)
      } else streamName
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }

  def getFlowId(flowName: String): Long =
    try {
      singleMap.getFlowId(flowName)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }

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


  def cacheMapInit: Unit = {
    streamCacheMapRefresh
    flowCacheMapRefresh
  }

}
