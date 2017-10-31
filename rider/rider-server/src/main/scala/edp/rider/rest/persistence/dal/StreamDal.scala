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

import edp.rider.common.Action._
import edp.rider.common._
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.StreamUtils._
import edp.rider.spark.SparkJobClientLog
import edp.rider.spark.SparkStatusQuery._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import edp.wormhole.common.util.JsonUtils._
import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class StreamDal(streamTable: TableQuery[StreamTable],
                instanceDal: InstanceDal,
                streamInTopicDal: StreamInTopicDal,
                streamUdfDal: RelStreamUdfDal,
                projectTable: TableQuery[ProjectTable]) extends BaseDalImpl[StreamTable, Stream](streamTable) with RiderLogger {

  def getStreamDetail(projectIdOpt: Option[Long] = None, streamIdOpt: Option[Long] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      val streamSeq = (projectIdOpt, streamIdOpt) match {
        case (Some(projectId), Some(streamId)) => Await.result(super.findByFilter(stream => stream.projectId === projectId && stream.id === streamId), minTimeOut)
        case (Some(projectId), None) => Await.result(super.findByFilter(_.projectId === projectId), minTimeOut)
        case (None, Some(streamId)) => Await.result(super.findByFilter(_.id === streamId), minTimeOut)
        case (None, None) => Await.result(super.findAll, minTimeOut)
      }
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val streamIds = streamSeq.map(_.id)
      val streamTopicSeq = streamInTopicDal.getStreamTopic(streamIds)
      val streamUdfSeq = streamUdfDal.getStreamUdf(streamIds)
      val streamZkUdfSeq = getZkStreamUdf(streamIds)
      val refreshStreamSeq = getStatus(action, streamSeq)
      Await.result(super.update(refreshStreamSeq), Inf)
      refreshStreamSeq.map(
        stream => {
          val topics = streamTopicSeq.filter(_.streamId == stream.id).map(
            topic => StreamTopic(topic.id, topic.name, topic.partitionOffsets, topic.rate)
          )
          val udfs = streamUdfSeq.filter(_.streamId == stream.id).map(
            udf => StreamUdf(udf.id, udf.functionName, udf.fullClassName, udf.jarName)
          )
          val zkUdfs = streamZkUdfSeq.filter(_.streamId == stream.id).map(
            udf => StreamZkUdf(udf.functionName, udf.fullClassName, udf.jarName)
          )
          StreamDetail(stream, getProjectNameByStreamName(stream.name), streamKafkaMap(stream.id), topics, udfs, zkUdfs, getDisableActions(stream.status))
        }
      )
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream detail failed", ex)
        throw GetStreamDetailException(ex.getMessage, ex.getCause)
    }

  }

  def updateByPutRequest(putStream: PutStream, userId: Long): Future[Int] = {
    db.run(streamTable.filter(_.id === putStream.id)
      .map(stream => (stream.desc, stream.sparkConfig, stream.startConfig, stream.launchConfig, stream.updateTime, stream.updateBy))
      .update(putStream.desc, putStream.sparkConfig, putStream.startConfig, putStream.launchConfig, currentSec, userId)).mapTo[Int]
  }

  def updateByStatus(streamId: Long, status: String, userId: Long): Future[Int] = {

    if (status == StreamStatus.STARTING.toString) {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime, stream.updateTime, stream.updateBy))
        .update(status, null, Some(currentSec), null, currentSec, userId)).mapTo[Int]
    } else {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.updateTime, stream.updateBy))
        .update(status, currentSec, userId)).mapTo[Int]
    }
  }

  def getResource(projectId: Long): Future[Resource] = {
    try {
      val project = Await.result(db.run((projectTable.filter(_.id === projectId)).result).mapTo[Seq[Project]], minTimeOut).head
      val streamSeq = super.findByFilter(stream => stream.projectId === projectId && (stream.status === "running" || stream.status === "waiting" || stream.status === "starting" || stream.status === "stopping")).mapTo[Seq[Stream]]
      val totalCores = project.resCores
      val totalMemory = project.resMemoryG
      var usedCores = 0
      var usedMemory = 0
      val streamResources = Await.result(streamSeq.map[Seq[AppResource]] {
        streamSeq =>
          streamSeq.sortBy(_.id).map {
            stream =>
              val config = json2caseClass[StartConfig](stream.startConfig)
              usedCores += config.driverCores + config.executorNums * config.perExecutorCores
              usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
              AppResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
          }
      }, minTimeOut)
      Future(Resource(totalCores, totalMemory, totalCores - usedCores, totalMemory - usedMemory, streamResources))
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream resource information by project id $projectId failed", ex)
        throw ex
    }
  }

  def getAllActiveStream: Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }

  def getStreamNameByStreamID(streamId: Long): Future[Stream] = {
    db.run(streamTable.filter(_.active === true).filter(_.id === streamId).result.head).mapTo[Stream]
  }

  def getActiveStreamByProjectId(projectId: Long): Future[Seq[StreamCacheMap]] = {
    db.run(streamTable.filter(_.active === true).filter(_.projectId === projectId).map { case (sTable) =>
      (sTable.id, sTable.name, sTable.projectId) <> (StreamCacheMap.tupled, StreamCacheMap.unapply)
    }.result).mapTo[Seq[StreamCacheMap]]
  }

  def getConfList = {
    lazy val driverConf = RiderConfig.spark.driverExtraConf
    lazy val executorConf = RiderConfig.spark.executorExtraConf
    driverConf + "," + executorConf
  }

  def updateStreamTable(stream: Stream): Future[Int] = {
    db.run(streamTable.filter(_.id === stream.id).update(stream))
  }

  def updateStreamsTable(streams: Seq[Stream]) = {
    db.run(DBIO.seq(streams.map(stream => streamTable.filter(_.id === stream.id).update(stream)): _*))
  }

  def checkStreamNameUnique(streamName: String) = {
    db.run(streamTable.filter(_.name === streamName).result)
  }

}