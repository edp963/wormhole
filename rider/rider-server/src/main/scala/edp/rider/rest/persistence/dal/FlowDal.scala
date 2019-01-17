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


package edp.rider.rest.persistence.dal

import edp.rider.RiderStarter.modules._
import edp.rider.common.{FlowStatus, RiderLogger, StreamStatus, StreamType}
import edp.rider.kafka.KafkaUtils._
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.ActionClass
import edp.rider.rest.util.{CommonUtils, FlowUtils}
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.FlowUtils._
import edp.rider.service.util.CacheMap
import edp.wormhole.util.DateUtils.dt2long
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{CanBeQueryCondition, TableQuery}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class FlowDal(flowTable: TableQuery[FlowTable], streamTable: TableQuery[StreamTable], projectTable: TableQuery[ProjectTable], streamDal: StreamDal, inTopicDal: StreamInTopicDal, flowInTopicDal: FlowInTopicDal, flowUdfTopicDal: FlowUserDefinedTopicDal)
  extends BaseDalImpl[FlowTable, Flow](flowTable) with RiderLogger {

  def defaultGetAll[C: CanBeQueryCondition](f: (FlowTable) => C, action: String = "refresh"): Future[Seq[FlowStream]] = {
    val flows = Await.result(super.findByFilter(f), minTimeOut)

    val streamIds = flows.map(_.streamId).distinct

    val streamMap = streamDal.refreshStreamStatus(None, Some(streamIds))
      .map(stream => (stream.id, StreamInfo(stream.name, stream.sparkAppid, stream.streamType, stream.functionType, stream.status)))
      .toMap[Long, StreamInfo]

    val flowStreams = FlowUtils.getFlowStatusByYarn(flows.map(flow => FlowStream(flow.id, flow.flowName, flow.projectId, flow.streamId, flow.sourceNs, flow.sinkNs, flow.parallelism, flow.consumedProtocol,
      flow.sinkConfig, flow.tranConfig, flow.tableKeys, flow.desc, flow.status, flow.startedTime, flow.stoppedTime, flow.logPath, flow.active, flow.createTime,
      flow.createBy, flow.updateTime, flow.updateBy, streamMap(flow.streamId).name, streamMap(flow.streamId).appId, streamMap(flow.streamId).status, streamMap(flow.streamId).streamType, streamMap(flow.streamId).functionType, "", "", None, Seq(), "")))

    val flowDisableActions = getDisableActions(flows)

    Future(flowStreams.map(flowStream => {
      genFlowStreamByAction(FlowStream(flowStream.id, flowStream.flowName, flowStream.projectId, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
        flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.status, flowStream.startedTime, flowStream.stoppedTime, flowStream.logPath, flowStream.active, flowStream.createTime,
        flowStream.createBy, flowStream.updateTime, flowStream.updateBy, flowStream.streamName, flowStream.streamAppId, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowDisableActions(flowStream.id), getHideActions(flowStream.streamType, flowStream.functionType), flowStream.topicInfo, flowStream.currentUdf, flowStream.msg), action)
    }))

  }

  def getById(flowId: Long): Future[Option[FlowStream]] = {
    try {
      val flowStreamOpt = Await.result(defaultGetAll(_.id === flowId), minTimeOut).headOption
      flowStreamOpt match {
        case Some(flowStream) =>
          Future(Some(FlowStream(flowStream.id, flowStream.flowName, flowStream.projectId, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
            flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.status, flowStream.startedTime, flowStream.stoppedTime, flowStream.logPath, flowStream.active, flowStream.createTime,
            flowStream.createBy, flowStream.updateTime, flowStream.updateBy, flowStream.streamName, flowStream.streamAppId, flowStream.streamStatus, flowStream.streamType,
            flowStream.functionType, flowStream.disableActions, flowStream.hideActions,
            Option(getFlowTopicsAllOffsets(flowId)), flowUdfDal.getFlowUdf(flowId), flowStream.msg)))
        case None => Future(None)
      }
    }

    catch {
      case ex: Exception =>
        riderLogger.error(s"Failed to get flow $flowId", ex)
        throw ex
    }
  }


  def adminGetById(flowId: Long): Future[Option[FlowAdminAllInfo]] = {
    try {
      val flowStreamOpt = Await.result(defaultGetAll(_.id === flowId), minTimeOut).headOption
      flowStreamOpt match {
        case Some(flowStream) =>
          val project = Await.result(db.run(projectTable.filter(_.id === flowStream.projectId).result).mapTo[Seq[Project]], maxTimeOut).head
          //          val stream = streamDal.getStreamDetail(Some(projectId), Some(Seq(flowStream.streamId))).head
          val flow = FlowStreamAdminInfo(flowStream.id, flowStream.flowName, flowStream.projectId, project.name, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
            flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.startedTime, flowStream.stoppedTime, flowStream.status, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
            flowStream.updateBy, flowStream.streamName, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowStream.disableActions, flowStream.hideActions, flowStream.msg)
          Future(Some(FlowAdminAllInfo(flow.id, flow.flowName, flow.projectId, flow.projectName, flow.streamId, flow.sourceNs, flow.sinkNs, flow.parallelism, flow.consumedProtocol, flow.sinkConfig, flow.tranConfig, flowStream.tableKeys, flowStream.desc, flow.status, flow.startedTime, flow.stoppedTime,
            flow.active, flow.createTime, flow.createBy, flow.updateTime, flow.updateBy, flow.streamName, flow.streamStatus, flow.streamType, flow.functionType, flow.disableActions, flow.hideActions,
            getFlowTopicsAllOffsets(flowStream.id), flowUdfDal.getFlowUdf(flowStream.id), flow.msg)))
        case None => Future(None)
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Failed to get flow $flowId", ex)
        throw ex
    }
  }

  def adminGetAll(visible: Boolean = true): Future[Seq[FlowStreamAdmin]] = {
    try {
      defaultGetAll(_.active === true).map[Seq[FlowStreamAdmin]] {
        flowStreams =>
          flowStreams.map {
            flowStream =>
              val project = Await.result(db.run(projectTable.filter(_.id === flowStream.projectId).result).mapTo[Seq[Project]], maxTimeOut).head
              FlowStreamAdmin(flowStream.id, flowStream.flowName, flowStream.projectId, project.name, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
                flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.startedTime, flowStream.stoppedTime, flowStream.status, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
                flowStream.updateBy, flowStream.streamName, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowStream.disableActions, flowStream.msg)
          }
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Failed to get all flows", ex)
        throw ex
    }
  }

  def adminGetAllInfo(visible: Boolean = true): Future[Seq[FlowAdminAllInfo]] = {
    try {
      defaultGetAll(_.active === true).map[Seq[FlowAdminAllInfo]] {
        flowStreams =>
          flowStreams.map {
            flowStream =>
              val project = Await.result(db.run(projectTable.filter(_.id === flowStream.projectId).result).mapTo[Seq[Project]], maxTimeOut).head
              val flow = FlowStreamAdminInfo(flowStream.id, flowStream.flowName, flowStream.projectId, project.name, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
                flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStream.startedTime, flowStream.stoppedTime, flowStream.status, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
                flowStream.updateBy, flowStream.streamName, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowStream.disableActions, flowStream.hideActions, flowStream.msg)
              FlowAdminAllInfo(flow.id, flow.flowName, flow.projectId, flow.projectName, flow.streamId, flow.sourceNs, flow.sinkNs, flow.parallelism, flow.consumedProtocol, flow.sinkConfig, flow.tranConfig, flowStream.tableKeys, flowStream.desc, flow.status, flow.startedTime, flow.stoppedTime,
                flow.active, flow.createTime, flow.createBy, flow.updateTime, flow.updateBy, flow.streamName, flow.streamStatus, flow.streamType, flow.functionType, flow.disableActions, flow.hideActions,
                getFlowTopicsAllOffsets(flowStream.id), flowUdfDal.getFlowUdf(flowStream.id), flow.msg)
          }
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"Failed to get all flows", ex)
        throw ex
    }
  }

  def updateStatusByFeedback(flowId: Long, flowNewStatus: String) = {
    if (flowNewStatus == "failed")
      Await.result(db.run(flowTable.filter(flow => flow.id === flowId && flow.status =!= "failed").map(c => (c.status, c.stoppedTime, c.updateTime)).update(flowNewStatus, Some(currentSec), currentSec)), minTimeOut)
    else if (flowNewStatus == "stopped")
      Await.result(db.run(flowTable.filter(flow => flow.id === flowId && flow.status =!= "stopped").map(c => (c.status, c.stoppedTime, c.updateTime)).update(flowNewStatus, Some(currentSec), currentSec)), minTimeOut)
    else if (flowNewStatus == "running")
      Await.result(db.run(flowTable.filter(flow => flow.id === flowId && flow.status =!= "running").map(c => (c.status, c.startedTime, c.updateTime)).update(flowNewStatus, Some(currentSec), currentSec)), minTimeOut)
  }

  def updateStatusByAction(flowId: Long, flowNewStatus: String, startTime: Option[String], stopTime: Option[String]) = {
    var flag = true
    if (flowNewStatus == "starting" || flowNewStatus == "updating") {
      val flow = Await.result(super.findById(flowId), minTimeOut).get
      if (flow.startedTime.nonEmpty && startTime.nonEmpty && dt2long(startTime.get) < dt2long(flow.startedTime.get))
        flag = false
      if (flow.stoppedTime.nonEmpty && stopTime.nonEmpty && dt2long(stopTime.get) < dt2long(flow.stoppedTime.get))
        flag = false
    }
    if (flag)
      Await.result(db.run(flowTable.filter(_.id === flowId).map(c => (c.status, c.startedTime, c.stoppedTime)).update(flowNewStatus, startTime, stopTime)), minTimeOut)
  }

  def getByNs(projectId: Long, sourceNs: String, sinkNs: String): Flow = {
    Await.result(db.run(flowTable.filter(_.active === true).filter(_.projectId === projectId).filter(_.sourceNs === sourceNs).filter(_.sinkNs === sinkNs).result).mapTo[Seq[Flow]], minTimeOut).head
  }

  def getByNsOnly(sourceNs: String, sinkNs: String) = {
    Await.result(db.run(flowTable.filter(_.active === true).filter(_.sourceNs === sourceNs).filter(_.sinkNs === sinkNs).result).mapTo[Seq[Flow]], minTimeOut)
  }

  def genFlowStreamByAction(flowStream: FlowStream, action: String): FlowStream = {
    try {
      val flowStatus = actionRule(flowStream, action)

      updateStatusByAction(flowStream.id, flowStatus.flowStatus, flowStatus.startTime, flowStatus.stopTime)

      val flow = FlowStream(flowStream.id, flowStream.flowName, flowStream.projectId, flowStream.streamId, flowStream.sourceNs, flowStream.sinkNs, flowStream.parallelism, flowStream.consumedProtocol,
        flowStream.sinkConfig, flowStream.tranConfig, flowStream.tableKeys, flowStream.desc, flowStatus.flowStatus, flowStatus.startTime, flowStatus.stopTime, flowStream.logPath, flowStream.active, flowStream.createTime, flowStream.createBy, flowStream.updateTime,
        flowStream.updateBy, flowStream.streamName, flowStream.streamAppId, flowStream.streamStatus, flowStream.streamType, flowStream.functionType, flowStatus.disableActions, flowStream.hideActions,
        flowStream.topicInfo, flowStream.currentUdf, flowStatus.msg)
      flow
    } catch {
      case ex: Exception =>
        riderLogger.error(s"update flow status by actionRule failed", ex)
        throw ex
    }
  }

  def getAllActiveFlowName: Future[Seq[FlowCacheMap]] =
    db.run(flowTable.map(flow => (flow.sourceNs, flow.sinkNs, flow.id)).result).map[Seq[FlowCacheMap]] {
      flows =>
        flows.map(flow => FlowCacheMap(flow._1 + "_" + flow._2, flow._3))
    }


  def flowAction(flowAction: ActionClass, userId: Long): Future[Seq[FlowStream]] = {
    try {
      val flowIdSeq = flowAction.flowIds.split(",").map(_.toLong)
      val flowSeq = Await.result(super.findByFilter(_.id inSet flowIdSeq), minTimeOut)
      if (flowAction.action == "delete") {
        deleteFlow(flowSeq, userId)
      } else {
        updateTimeAndUser(flowIdSeq, userId)
        defaultGetAll(_.id inSet flowIdSeq, flowAction.action)
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId ${
          flowAction.action
        } flow ${
          flowAction.flowIds
        } failed", ex)
        throw ex
    }
  }

  def insertOrAbort(flows: Seq[Flow]): Seq[Flow] = {
    try {
      val flowInsertSeq = new ListBuffer[Flow]
      flows.foreach(
        flow => {
          val search = Await.result(super.findByFilter(row => row.sourceNs === flow.sourceNs && row.sinkNs === flow.sinkNs), minTimeOut)
          if (search.isEmpty)
            flowInsertSeq += Await.result(insert(flow), CommonUtils.minTimeOut)
        }
      )
      flowInsertSeq
    } catch {
      case ex: Exception =>
        riderLogger.error(s"flow insert or abort failed", ex)
        throw ex
    }
  }

  def deleteFlow(flowSeq: Seq[Flow], userId: Long) = {
    val flowStream = Await.result(defaultGetAll(_.id inSet flowSeq.map(_.id)), minTimeOut)
    flowStream.foreach(flow => {
      if (flow.streamType == StreamType.SPARK.toString)
        stopFlow(flow.streamId, flow.id, userId, flow.functionType, flow.sourceNs, flow.sinkNs, flow.tranConfig.getOrElse(""))
      else {
        if (flow.streamStatus == StreamStatus.RUNNING.toString && flow.status == FlowStatus.RUNNING.toString)
          stopFlinkFlow(flow.streamAppId.get, getFlowName(flow.id, flow.sourceNs, flow.sinkNs))
        Await.result(flowUdfDal.deleteByFilter(_.flowId === flow.id), minTimeOut)
        Await.result(flowInTopicDal.deleteByFilter(_.flowId === flow.id), minTimeOut)
        Await.result(flowUdfTopicDal.deleteByFilter(_.flowId === flow.id), minTimeOut)
      }
      riderLogger.info(s"delete flow ${flow.id}: $flow")
      Await.result(super.deleteById(flow.id), minTimeOut)
      CacheMap.flowCacheMapRefresh
    })
    riderLogger.info(s"user $userId delete flow ${
      flowSeq.map(_.id).mkString(",")
    } success")
    Future(Seq())
  }

  def updateTimeAndUser(flowIds: Seq[Long], userId: Long) = {
    Await.result(db.run(flowTable.filter(_.id inSet flowIds).map(c => (c.updateTime, c.updateBy)).update(currentSec, userId)), minTimeOut)
  }

  def getActiveStatusIdsByStreamId(streamId: Long): Seq[Long] = {
    Await.result(db.run(flowTable.filter(flow => flow.streamId === streamId && flow.status =!= "new" && flow.status =!= "stopping" && flow.status =!= "stopped").map(_.id).result), minTimeOut)
  }

  def getFlowTopicsAllOffsets(flowId: Long): GetTopicsResponse = {
    getFlowTopicsMap(Seq(flowId))(flowId)
  }

  def checkFlowTopicExists(flowId: Long, topic: String): Boolean = {
    var exist = false
    if (flowInTopicDal.checkAutoRegisteredTopicExists(flowId, topic) || flowUdfTopicDal.checkUdfTopicExists(flowId, topic))
      exist = true
    exist
  }

  def getFlowKafkaInfo(flowId: Long): (Long, String) = {
    Await.result(db.run(
      ((flowQuery.filter(_.id === flowId) join streamQuery on (_.streamId === _.id))
        join instanceQuery on (_._2.instanceId === _.id))
        .map {
          case ((flow, _), instance) => (flow.id, instance.connUrl)
        }.result.head).mapTo[(Long, String)], minTimeOut)

  }

  def getFlowKafkaMap(flowIds: Seq[Long]): Map[Long, String] = {
    Await.result(db.run(
      ((flowQuery.filter(_.id inSet flowIds) join streamQuery on (_.streamId === _.id))
        join instanceQuery on (_._2.instanceId === _.id)).map {
        case ((flow, _), instance) => (flow.id, instance.connUrl) <> (FlowIdKafkaUrl.tupled, FlowIdKafkaUrl.unapply)
      }.result).mapTo[Seq[FlowIdKafkaUrl]], minTimeOut)
      .map(flowKafka => (flowKafka.flowId, flowKafka.kafkaUrl)).toMap
  }

  def updateByFlowStatus(flowId: Long, status: String, userId: Long): Future[Int] = {
    if (status == FlowStatus.STARTING.toString) {
      db.run(flowTable.filter(_.id === flowId)
        .map(flow => (flow.status, flow.startedTime, flow.stoppedTime, flow.updateTime, flow.updateBy))
        .update(status, Some(currentSec), null, currentSec, userId)).mapTo[Int]
    } else {
      db.run(flowTable.filter(_.id === flowId)
        .map(flow => (flow.status, flow.updateTime, flow.updateBy))
        .update(status, currentSec, userId)).mapTo[Int]
    }
  }

  def updateStatusByStreamStop(flowIds: Seq[Long], status: String): Future[Int] = {
    if (status == FlowStatus.STOPPED.toString || status == FlowStatus.FAILED.toString) {
      db.run(flowTable.filter(_.id inSet flowIds)
        .map(flow => (flow.status, flow.stoppedTime))
        .update(status, Some(currentSec))).mapTo[Int]
    } else {
      db.run(flowTable.filter(_.id inSet flowIds)
        .map(flow => (flow.status))
        .update(status)).mapTo[Int]
    }
  }

  def updateLogPath(flowId: Long, logPath: String): Int = {
    Await.result(db.run(flowTable.filter(_.id === flowId).map(_.logPath).update(Option(logPath))), minTimeOut)
  }

  def updateStreamId(flowId: Long, streamId: Long): Future[Int] = {
    db.run(flowTable.filter(flow => flow.id === flowId).map(_.streamId).update(streamId))
  }

}
