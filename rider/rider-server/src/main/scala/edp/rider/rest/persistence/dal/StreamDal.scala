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
import edp.rider.common.Action._
import edp.rider.common._
import edp.rider.kafka.WormholeGetOffsetUtils._
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.{InstanceUtils, StreamUtils}
import edp.rider.rest.util.StreamUtils._
import edp.rider.yarn.{ShellUtils, SubmitYarnJob}
import edp.wormhole.util.DateUtils
import edp.wormhole.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable
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

  def updateStreamStatusByYarn(streams: Seq[Stream], appInfoMap: Map[String, AppResult], userId: Long): Map[Long, StreamInfo] = {
    val streamMap = streams.map(stream => (stream.id, (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime)))).toMap
    val streamPidMap = streams.map(stream => (stream.id, stream.sparkAppid)).toMap
    val refreshStreamSeq: Seq[Stream] = getStreamYarnAppStatus(streams, appInfoMap, userId)
    val updateStreamSeq = refreshStreamSeq.filter(stream => {
      if (streamMap(stream.id) == (stream.sparkAppid, stream.status, getStreamTime(stream.startedTime), getStreamTime(stream.stoppedTime))) {
        false
      } else {
        //riderLogger.info(s"stream status ${streamMap(stream.id)._2}, yarn status ${stream.status}, stream pid ${streamPidMap(stream.id)}")
        /*if(streamMap(stream.id)._2 == "starting" && (stream.status == "waiting" || stream.status == "running" || stream.status == "failed")) {
          SubmitYarnJob.killPidCommand(streamPidMap(stream.id), stream.name)
        }*/
        true
      }
    })
    //riderLogger.info(s"updateStreamSeq ${updateStreamSeq}")
    updateByRefresh(updateStreamSeq)
    refreshStreamSeq.map(stream => (stream.id, StreamInfo(stream.name, stream.sparkAppid, stream.streamType, stream.functionType, stream.status))).toMap
  }

  def refreshStreamStatus(streamId: Long): Option[Stream] = {
    refreshStreamStatus(streamIdsOpt = Option(Seq(streamId))).headOption
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
          StreamDetail(StreamUtils.hidePid(stream), projectMap(stream.projectId), streamKafkaMap(stream.id), None, Seq[StreamUdf](), getDisableActions(stream.streamType, stream.status), getHideActions(stream.streamType))
        }
      )
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream detail failed", ex)
        throw GetStreamDetailException(ex.getMessage, ex.getCause)
    }
  }

  def getSimpleStreamInfo(projectId: Long, streamType: String, functionTypeOpt: Option[String] = None): Seq[SimpleStreamInfo] = {
    if (streamType.equals("flink")) {
      val streamSeq = Await.result(streamDal.findByFilter(stream => stream.streamType === "flink" && stream.projectId === projectId), minTimeOut)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val startConfigMap = streamSeq.map(stream => (stream.id, stream.startConfig)).toMap[Long, String]
      val configMap = streamSeq.map(stream => (stream.id, json2caseClass[FlinkResourceConfig](startConfigMap(stream.id)))).toMap[Long, FlinkResourceConfig]
      val maxParallelismMap = streamSeq.map(stream => (stream.id, configMap(stream.id).taskManagersNumber * configMap(stream.id).perTaskManagerSlots)).toMap[Long, Int]
      streamSeq.map(
        stream => {
          SimpleStreamInfo(stream.id, stream.name, maxParallelismMap(stream.id), streamKafkaMap(stream.id).instance, getStreamTopicsName(stream.id)._2)
        }
      )
    } else {
      val streamSeq = Await.result(streamDal.findByFilter(stream => stream.streamType === "spark" && stream.projectId === projectId && stream.functionType === functionTypeOpt.get), minTimeOut)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      streamSeq.map(
        stream => {
          SimpleStreamInfo(stream.id, stream.name, 0, streamKafkaMap(stream.id).instance, getStreamTopicsName(stream.id)._2)
        }
      )
    }
  }

  def getStreamDetail(projectIdOpt: Option[Long] = None, streamIdsOpt: Option[Seq[Long]] = None, action: String = REFRESH.toString): Seq[StreamDetail] = {
    try {
      val streamSeq = refreshStreamStatus(projectIdOpt, streamIdsOpt, action)
      val streamKafkaMap = instanceDal.getStreamKafka(streamSeq.map(stream => (stream.id, stream.instanceId)).toMap[Long, Long])
      val streamIds = streamSeq.map(_.id)
      val streamGroupIdMap = streamSeq.map(stream => (stream.id, stream.name)).toMap[Long, String]
      val streamTopicMap = getStreamTopicsMap(streamIds, streamGroupIdMap)
      val streamUdfSeq = streamUdfDal.getStreamUdf(streamIds)
      val projectMap = getStreamProjectMap(streamSeq)

      streamSeq.map(
        stream => {
          val udfs = streamUdfSeq.filter(_.streamId == stream.id).map(
            udf => StreamUdf(udf.id, udf.functionName, udf.fullClassName, udf.jarName)
          )
          StreamDetail(stream, projectMap(stream.projectId), streamKafkaMap(stream.id), Option(streamTopicMap(stream.id)), udfs, getDisableActions(stream.streamType, stream.status), getHideActions(stream.streamType))
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
      .map(stream => (stream.desc, stream.jvmDriverConfig, stream.jvmExecutorConfig, stream.othersConfig, stream.startConfig, stream.launchConfig, stream.specialConfig, stream.updateTime, stream.updateBy))
      .update(putStream.desc, putStream.JVMDriverConfig, putStream.JVMExecutorConfig, putStream.othersConfig, putStream.startConfig, putStream.launchConfig, putStream.specialConfig, currentSec, userId)).mapTo[Int]
  }

  def updateStatusByStart(streamId: Long, status: String, userId: Long, logPath: String, pid: Option[String]): Future[Int] = {
    db.run(streamTable.filter(_.id === streamId)
      .map(stream => (stream.status, stream.sparkAppid, stream.logPath, stream.startedTime, stream.stoppedTime, stream.updateTime, stream.updateBy))
      .update(status, pid, Some(logPath), Some(currentSec), null, currentSec, userId)).mapTo[Int]
  }

  def updateStatusByStop(streamId: Long, status: String, userId: Long): Future[Int] = {
    if (status == StreamStatus.STOPPING.toString || status == StreamStatus.STOPPED.toString) {
      db.run(streamTable.filter(_.id === streamId)
        .map(stream => (stream.status, stream.stoppedTime, stream.updateTime, stream.updateBy))
        .update(status, Some(currentSec), currentSec, userId)).mapTo[Int]
    } else {
      Future(0)
    }
  }

  def updateByRefresh(streams: Seq[Stream]): Seq[Int] = {
    streams.map(stream =>
      Await.result(db.run(streamTable.filter(_.id === stream.id)
        .map(stream => (stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime, stream.updateTime))
        .update(stream.status, stream.sparkAppid, stream.startedTime, stream.stoppedTime, stream.userTimeInfo.updateTime)).mapTo[Int], minTimeOut))
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

  def updateStreamsTable(streams: Seq[Stream]): Future[Unit] = {
    db.run(DBIO.seq(streams.map(stream => streamTable.filter(_.id === stream.id).update(stream)): _*))
  }

  def getProjectStreamsUsedResource(projectId: Long): (Int, Int, Seq[AppResource]) = {
    val streamSeq: Seq[Stream] = Await.result(super.findByFilter(job => job.projectId === projectId && (job.status === "running" || job.status === "waiting" || job.status === "starting" || job.status === "stopping")), minTimeOut)
    var usedCores = 0
    var usedMemory = 0
    val streamResources: Seq[AppResource] = streamSeq.map(
      stream => {
        StreamType.withName(stream.streamType) match {
          case StreamType.SPARK =>
            val config = json2caseClass[StartConfig](stream.startConfig)
            usedCores += config.driverCores + config.executorNums * config.perExecutorCores
            usedMemory += config.driverMemory + config.executorNums * config.perExecutorMemory
            AppResource(stream.name, config.driverCores, config.driverMemory, config.executorNums, config.perExecutorMemory, config.perExecutorCores)
          case StreamType.FLINK =>
            val config = json2caseClass[FlinkResourceConfig](stream.startConfig)
            usedCores += config.taskManagersNumber * config.perTaskManagerSlots
            usedMemory += config.jobManagerMemoryGB + config.taskManagersNumber * config.perTaskManagerMemoryGB
            AppResource(stream.name, 0, config.jobManagerMemoryGB, config.taskManagersNumber, config.perTaskManagerMemoryGB, config.perTaskManagerSlots)
        }
      }
    )
    (usedCores, usedMemory, streamResources)
  }

  def checkTopicExists(streamId: Long, topic: String): Boolean = {
    var exist = false
    if (streamInTopicDal.checkAutoRegisteredTopicExists(streamId, topic) || streamUdfTopicDal.checkUdfTopicExists(streamId, topic))
      exist = true
    exist
  }


  // get kafka instance id, url
  def getKafkaInfo(streamId: Long): (Long, String, Option[String]) = {
    Await.result(db.run((streamQuery.filter(_.id === streamId) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (_, instance) => (instance.id, instance.connUrl, instance.connConfig)
      }.result.head).mapTo[(Long, String, Option[String])], minTimeOut)
  }

  def getStreamKafkaMap(streamIds: Seq[Long]): Map[Long, String] = {
    Await.result(db.run((streamQuery.filter(_.id inSet streamIds) join instanceQuery on (_.instanceId === _.id))
      .map {
        case (stream, instance) => (stream.id, instance.connUrl) <> (StreamIdKafkaUrl.tupled, StreamIdKafkaUrl.unapply)
      }.result).mapTo[Seq[StreamIdKafkaUrl]], minTimeOut)
      .map(streamKafka => (streamKafka.streamId, streamKafka.kafkaUrl)).toMap
  }

  def getTopicsAllOffsets(streamId: Long): GetTopicsResponse = {
    val stream = Await.result(super.findById(streamId), minTimeOut).head
    val map = mutable.HashMap.empty[Long, String]
    map(streamId) = stream.name
    getStreamTopicsMap(Seq(streamId), map.toMap)(streamId)
  }

  def getStreamTopicsMap(streamId: Long, streamName: String): GetTopicsResponse = {
    val map = mutable.HashMap.empty[Long, String]
    map(streamId) = streamName
    getStreamTopicsMap(Seq(streamId), map.toMap)(streamId)
  }

  def getStreamTopicsMap(streamIds: Seq[Long], streamGroupIdMap: Map[Long, String]): Map[Long, GetTopicsResponse] = {
    val autoRegisteredTopics = streamInTopicDal.getAutoRegisteredTopics(streamIds)
    val udfTopics = streamUdfTopicDal.getUdfTopics(streamIds)
    val kafkaMap = getStreamKafkaMap(streamIds)
    streamIds.map(id => {
      val autoTopicsResponse = genAllOffsets(autoRegisteredTopics, kafkaMap, streamGroupIdMap)
      val udfTopicsResponse = genAllOffsets(udfTopics, kafkaMap, streamGroupIdMap)
      (id, GetTopicsResponse(autoTopicsResponse, udfTopicsResponse))
    }).toMap
  }

  //getStreamTopicsName for getSimpleStreamInfo
  def getStreamTopicsName(streamIds: Long): (Long, Seq[String]) = {
    val autoRegisteredTopics = streamInTopicDal.getAutoRegisteredTopics(streamIds)
    val udfTopics = streamUdfTopicDal.getUdfTopics(streamIds)
    val topics = autoRegisteredTopics.filter(_.streamId == streamIds) ++: udfTopics.filter(_.streamId == streamIds)
    val topicsName = topics.map(topics => topics.name)
    val topicInfo = (streamIds, topicsName)
    topicInfo
  }


  def genAllOffsets(topics: Seq[StreamTopicTemp], kafkaMap: Map[Long, String], streamGroupIdMap: Map[Long, String]): Seq[TopicAllOffsets] = {
    topics.map(topic => {
      val inputKafkaInstance = getKafkaDetailByStreamId(topic.streamId)
      val inputKafkaKerberos = InstanceUtils.getKafkaKerberosConfig(inputKafkaInstance._2.getOrElse(""), RiderConfig.kerberos.kafkaEnabled)
      val earliest = getEarliestOffset(kafkaMap(topic.streamId), topic.name, inputKafkaKerberos)
      val latest = getLatestOffset(kafkaMap(topic.streamId), topic.name, inputKafkaKerberos)
      val consumed = getConsumerOffset(kafkaMap(topic.streamId), streamGroupIdMap(topic.streamId), topic.name, latest.split(",").length, inputKafkaKerberos)
      TopicAllOffsets(topic.id, topic.name, topic.rate, consumed, earliest, latest)
    })
  }

  //  def getConsumedMaxOffset(streamId: Long, topics: Seq[StreamTopicTemp]): Map[String, String] = {
  //    try {
  //      val stream = Await.result(super.findById(streamId), minTimeOut).head
  //
  //      val topicFeedbackSeq = feedbackOffsetDal.getStreamTopicsFeedbackOffset(streamId, topics.size)
  //
  //      val topicOffsetMap = new mutable.HashMap[String, String]()
  //      topicFeedbackSeq.foreach(topic => {
  //        if (!topicOffsetMap.contains(topic.topicName)) {
  //          if (stream.startedTime.nonEmpty && stream.startedTime != null &&
  //            DateUtils.yyyyMMddHHmmss(topic.umsTs) > DateUtils.yyyyMMddHHmmss(stream.startedTime.get))
  //            topicOffsetMap(topic.topicName) = formatOffset(topic.partitionOffsets)
  //        }
  //      })
  //      topics.foreach(
  //        topic => {
  //          if (!topicOffsetMap.contains(topic.name)) topicOffsetMap(topic.name) = formatOffset(topic.partitionOffsets)
  //        }
  //      )
  //      topicOffsetMap.toMap
  //    } catch {
  //      case ex: Exception =>
  //        riderLogger.error(s"get stream consumed latest offset from feedback failed", ex)
  //        throw ex
  //    }
  //  }


}