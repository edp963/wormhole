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


package edp.rider.service.util

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

  private case class OffsetMapkey(streamid: Long, topicName: String, partitionId: Int)

  private case class OffsetMapValue(offset: Long)

  private val topicLatestOffsetMap = new mutable.HashMap[OffsetMapkey, OffsetMapValue]()

  private def getProjectId(stream_id: Long): Long = {
    var pid: Long = 0L
    streamCacheMap.get(StreamMapKey(stream_id)) match {
      case Some(x) =>
        pid = x.projectId
      case None =>
    }
    pid
  }

  private def getStreamName(stream_id: Long): String = {
    var streamName = "default"
    streamCacheMap.get(StreamMapKey(stream_id)) match {
      case Some(x) =>
        streamName = x.streamName
      case None =>
    }
    streamName
  }

  private def setStreamCacheMap(stream_id: Long, stream_name: String, project_id: Long) = {
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

  private def setFlowCacheMap(flow_name: String, flow_id: Long) = {
    if (flowCacheMap.contains(FlowMapKey(flow_name))) {
      flowCacheMap.update(FlowMapKey(flow_name), FlowMapValue(flow_id))
    } else {
      flowCacheMap.put(FlowMapKey(flow_name), FlowMapValue(flow_id))
    }
  }

  private def getOffsetValue(streamid: Long, topicName: String, partitionId: Int): Long = {
    var offset: Long = -1L
    topicLatestOffsetMap.get(OffsetMapkey(streamid, topicName, partitionId)) match {
      case Some(x) =>
        offset = x.offset
      case None =>
    }
    offset
  }

  private def setOffsetMap(streamid: Long, topicName: String, partitionId: Int, offset: Long) = {
    if (topicLatestOffsetMap.contains(OffsetMapkey(streamid, topicName, partitionId))) {
      topicLatestOffsetMap.update(OffsetMapkey(streamid, topicName, partitionId), OffsetMapValue(offset))
    } else {
      topicLatestOffsetMap.put(OffsetMapkey(streamid, topicName, partitionId), OffsetMapValue(offset))
    }
  }
}

object CacheMap extends RiderLogger {
  val singleMap = new CacheMap

  def setStreamIdProjectId(stream_id: Long, stream_name: String, project_id: Long) = singleMap.setStreamCacheMap(stream_id, stream_name, project_id)

  def setFlowCacheMap(flow_name: String, flow_id: Long) = singleMap.setFlowCacheMap(flow_name, flow_id)

  def setOffsetMap(streamid: Long, topicName: String, partitionId: Int, offset: Long) = singleMap.setOffsetMap(streamid, topicName, partitionId, offset)

  def getProjectId(streamId: Long): Long =
    try {
      val pid = singleMap.getProjectId(streamId)
      if (pid == 0) {
        streamCacheMapRefresh
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
        streamCacheMapRefresh
        singleMap.getStreamName(streamId)
      } else streamName
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }

  def getFlowId(flowName: String): Long =
    try {
      val pid = singleMap.getFlowId(flowName)
      if (pid == 0) {
        flowCacheMapRefresh
        singleMap.getFlowId(flowName)
      } else pid
    } catch {
      case ex: Exception =>
        riderLogger.error(s"stream cache map refresh failed", ex)
        throw ex
    }


  def getOffsetValue(streamid: Long, topicName: String, partitionId: Int) =
    try {
      val offset = singleMap.getOffsetValue(streamid, topicName, partitionId)
      if (offset == 0) {
        flowCacheMapRefresh
        singleMap.getOffsetValue(streamid, topicName, partitionId)
      } else offset
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


  def offsetMapRefresh: Unit = {
    try {
      Await.result(modules.streamDal.getAllActiveStream, minTimeOut).foreach { stream =>
        FeedbackOffsetUtil.getOffsetFromFeedback(stream.streamId).foreach { e =>
          singleMap.setOffsetMap(e.streamId, e.topicName, e.partitionId, e.offset)
        }
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"offset cache map refresh failed", ex)
        throw ex
    }
  }

  def offsetMapRefreshByStreamId(streamId: Long) = {
    try {
      FeedbackOffsetUtil.getOffsetFromFeedback(streamId).foreach { e =>
        singleMap.setOffsetMap(e.streamId, e.topicName, e.partitionId, e.offset)
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"offset cache map refresh failed", ex)
        throw ex
    }
  }

  def cacheMapInit = {
    streamCacheMapRefresh
    flowCacheMapRefresh
    offsetMapRefresh
    cacheMapPrint
  }

  def cacheMapPrint = {

    Await.result(modules.streamDal.getAllActiveStream, minTimeOut).foreach { stream =>
      //      riderLogger.info(s" StreamMap  ${stream.streamId} -> ( ${singleMap.getStreamName(stream.streamId)}, ${singleMap.getProjectId(stream.streamId)} )")
    }

    Await.result(modules.flowDal.adminGetAll(), minTimeOut).foreach { flow =>
      //      riderLogger.info(s" FlowMap  ${flow.sourceNs}_${flow.sinkNs} -> ${flow.id}")
    }

    Await.result(modules.streamDal.getAllActiveStream, minTimeOut).foreach { stream =>
      val a = FeedbackOffsetUtil.getOffsetFromFeedback(stream.streamId)
      a.foreach { e =>
        singleMap.getOffsetValue(e.streamId, e.topicName, e.partitionId)
        //        riderLogger.info(s" OffsetMap ( ${e.streamId},${e.topicName},${e.partitionId} ) -> ${b}")
      }
    }
  }

}
