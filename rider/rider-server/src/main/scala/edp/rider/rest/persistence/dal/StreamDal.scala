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

import java.util
import edp.rider.kafka.KafkaUtils._
import edp.rider.common.Action._
import edp.rider.common._
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.StreamUtils._
import edp.wormhole.common.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import edp.rider.RiderStarter.modules._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class StreamDal(streamTable: TableQuery[StreamTable],
                instanceDal: InstanceDal,
                streamInTopicDal: StreamInTopicDal,
                streamUdfDal: RelStreamUdfDal,
                projectTable: TableQuery[ProjectTable]) extends BaseDalImpl[StreamTable, Stream](streamTable) with RiderLogger {

  def getStreamProjectMap(streamSeq: Seq[Stream]): Map[Long, String] = {
    val projectSeq = Await.result(db.run(projectTable.filter(_.id inSet streamSeq.map(_.projectId)).result).mapTo[Seq[Project]], minTimeOut)
    projectSeq.map(project => (project.id, project.name)).toMap
  }

  def refreshStreamStatus(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[Stream] = {
    val streamSeq = getStreamSeq(projectIdOpt, streamIdsOpt)
    val streamMap = streamSeq.map(stream => (stream.id, (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime)))).toMap
    val refreshStreamSeq = getStatus(action, streamSeq)
    val updateStreamSeq = refreshStreamSeq.filter(stream => {
      if (streamMap(stream.id) == (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime))) false else true
    })
    updateByRefresh(updateStreamSeq)
    refreshStreamSeq
  }

  def getStreamSeq(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None): Seq[Stream] = {
    (projectIdOpt, streamIdsOpt) match {
      case (_, Some(streamIds)) => Await.result(super.findByFilter(stream => stream.id inSet streamIds), minTimeOut)
      case (Some(projectId), None) => Await.result(super.findByFilter(_.projectId === projectId), minTimeOut)
      case (None, None) => Await.result(super.findAll, minTimeOut)
    }
  }

  def getBriefDetail(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      val streamSeq = refreshStreamStatus(projectIdOpt, streamIdsOpt, action)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val projectMap = getStreamProjectMap(streamSeq)
      streamSeq.map(
        stream => {
          StreamDetail(stream, projectMap(stream.projectId), streamKafkaMap(stream.id), None, Seq[StreamUdf](), getDisableActions(StreamStatus.withName(stream.status)))
        }
      )
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream detail failed", ex)
        throw GetStreamDetailException(ex.getMessage, ex.getCause)
    }
  }

  def getStreamDetail(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      val streamSeq = refreshStreamStatus(projectIdOpt, streamIdsOpt, action)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val streamIds = streamSeq.map(_.id)
      val streamTopicMap = getStreamTopicsMap(streamIds)
      val streamUdfSeq = streamUdfDal.getStreamUdf(streamIds)
      //      val streamZkUdfSeq = getZkStreamUdf(streamIds)
      val projectMap = getStreamProjectMap(streamSeq)

      streamSeq.map(
        stream => {
          //          val topics = streamTopicSeq.filter(_.streamId == stream.id).map(
          //            topic => StreamTopic(topic.id, topic.name, topic.partitionOffsets, topic.rate)
          //          )
          val udfs = streamUdfSeq.filter(_.streamId == stream.id).map(
            udf => StreamUdf(udf.id, udf.functionName, udf.fullClassName, udf.jarName)
          )
          //          val zkUdfs = streamZkUdfSeq.filter(_.streamId == stream.id).map(
          //            udf => StreamZkUdf(udf.functionName, udf.fullClassName, udf.jarName)
          //          )
          StreamDetail(stream, projectMap(stream.projectId), streamKafkaMap(stream.id), Option(streamTopicMap(stream.id)), udfs, getDisableActions(StreamStatus.withName(stream.status)))
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

  def updateByStatus(streamId: Long, status: String, userId: Long, logPath: String): Future[Int] = {

    if (status == StreamStatus.STARTING.toString) {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.sparkAppid, stream.logPath, stream.startedTime, stream.stoppedTime, stream.updateTime, stream.updateBy))
        .update(status, null, Some(logPath), Some(currentSec), null, currentSec, userId)).mapTo[Int]
    } else {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.updateTime, stream.updateBy))
        .update(status, currentSec, userId)).mapTo[Int]
    }
  }

  def updateByRefresh(streams: Seq[Stream]): Seq[Int] = {
    streams.map(stream =>
      Await.result(db.run(streamTable.filter(_.id === stream.id)
        .map(stream => (stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime))
        .update(stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime)).mapTo[Int], minTimeOut))
  }

  def getResource(projectId: Long): Future[Resource] = {
    try {
      val project = Await.result(db.run(projectTable.filter(_.id === projectId).result).mapTo[Seq[Project]], minTimeOut).head
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


  def updateStreamTable(stream: Stream): Future[Int] = {
    db.run(streamTable.filter(_.id === stream.id).update(stream))
  }

  def updateStreamsTable(streams: Seq[Stream]) = {
    db.run(DBIO.seq(streams.map(stream => streamTable.filter(_.id === stream.id).update(stream)): _*))
  }

  def getProjectStreamsUsedResource(projectId: Long) = {
    val streamSeq: Seq[Stream] = Await.result(super.findByFilter(job => job.projectId === projectId && (job.status === "running" || job.status === "waiting" || job.status === "starting" || job.status === "stopping")), minTimeOut)
    var usedCores = 0
    var usedMemory = 0
    val streamResources: Seq[AppResource] = streamSeq.map(
      stream => {
        val config = json2caseClass[StartConfig](stream.startConfig)
        usedCores += config.driverCores + config.executorNums * config.perExecutorCores
        usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
        AppResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)

      }
    )
    (usedCores, usedMemory, streamResources)
  }

  def checkTopicExists(streamId: Long, topic: String): Boolean = {
    var exist = false
    if (streamInTopicDal.checkAutoRegisteredTopicExists(streamId, topic) || udfTopicDal.checkUdfTopicExists(streamId, topic))
      exist = true
    exist
  }

  // get kafka instance id, url
  def getKafkaInfo(streamId: Long): (Long, String) = {
    Await.result(db.run((streamQuery.filter(_.id === streamId) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (_, instance) => (instance.id, instance.connUrl)
      }.result.head).mapTo[(Long, String)], minTimeOut)
  }

  def getStreamKafkaMap(streamIds: Seq[Long]): Map[Long, String] = {
    Await.result(db.run((streamQuery.filter(_.id inSet streamIds) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (stream, instance) => (stream.id, instance.connUrl) <> (StreamIdKafkaUrl.tupled, StreamIdKafkaUrl.unapply)
      }.result).mapTo[Seq[StreamIdKafkaUrl]], minTimeOut)
      .map(streamKafka => (streamKafka.streamId, streamKafka.kafkaUrl)).toMap
  }

  def getTopicsAllOffsets(streamId: Long): GetTopicsResponse = {
    val autoRegisteredTopics = inTopicDal.getAutoRegisteredTopics(streamId)
    val udfTopics = udfTopicDal.getUdfTopics(streamId)
    val kafkaInfo = getKafkaInfo(streamId)
    val autoRegisteredTopicsResponse = autoRegisteredTopics.map(topic => {
      val earliest = getKafkaEarliestOffset(kafkaInfo._2, topic.name)
      val latest = getKafkaLatestOffset(kafkaInfo._2, topic.name)
      val consumed = earliest //todo
      TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)
    })
    val udfTopicsResponse = udfTopics.map(topic => {
      val earliest = getKafkaEarliestOffset(kafkaInfo._2, topic.name)
      val latest = getKafkaLatestOffset(kafkaInfo._2, topic.name)
      val consumed = earliest //todo
      TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)
    })
    GetTopicsResponse(autoRegisteredTopicsResponse, udfTopicsResponse)
  }

  def getStreamTopicsMap(streamIds: Seq[Long]): Map[Long, GetTopicsResponse] = {
    val autoRegisteredTopics = inTopicDal.getAutoRegisteredTopics(streamIds)
    val udfTopics = udfTopicDal.getUdfTopics(streamIds)
    val kafkaMap = getStreamKafkaMap(streamIds)
    streamIds.map(id => {
      val autoTopicsResponse = autoRegisteredTopics.filter(_.streamId == id)
        .map(topic => {
          val earliest = getKafkaEarliestOffset(kafkaMap(topic.streamId), topic.name)
          val latest = getKafkaLatestOffset(kafkaMap(topic.streamId), topic.name)
          val consumed = earliest //todo
          TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)
        })
      val udfTopicsResponse = udfTopics.filter(_.streamId == id)
        .map(topic => {
          val earliest = getKafkaEarliestOffset(kafkaMap(topic.streamId), topic.name)
          val latest = getKafkaLatestOffset(kafkaMap(topic.streamId), topic.name)
          val consumed = earliest //todo
          TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)
        })
      (id, GetTopicsResponse(autoTopicsResponse, udfTopicsResponse))
    }).toMap
    //    GetTopicsResponse(autoRegisteredTopicsResponse, udfTopicsResponse)
  }

}