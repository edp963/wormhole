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

import edp.rider.common.{DatabaseSearchException, RiderLogger}
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration._

class StreamInTopicDal(streamInTopicTable: TableQuery[StreamInTopicTable],
                       nsDatabaseTable: TableQuery[NsDatabaseTable],
                       feedbackOffsetTable: TableQuery[FeedbackOffsetTable]) extends BaseDalImpl[StreamInTopicTable, StreamInTopic](streamInTopicTable) with RiderLogger {

  def getStreamTopic(streamIds: Seq[Long]): Seq[StreamTopicTemp] = {
    try {
      val streamTopics = Await.result(db.run((streamInTopicTable.filter(_.streamId inSet streamIds) join nsDatabaseTable on (_.nsDatabaseId === _.id))
        .map {
          case (streamInTopic, nsDatabase) => (streamInTopic.nsDatabaseId, streamInTopic.streamId, nsDatabase.nsDatabase, streamInTopic.partitionOffsets, streamInTopic.rate) <> (StreamTopicTemp.tupled, StreamTopicTemp.unapply)
        }.result).mapTo[Seq[StreamTopicTemp]], Inf)
      getConsumedMaxOffset(streamTopics)
    } catch {
      case ex: Exception =>
        throw DatabaseSearchException(ex.getMessage, ex.getCause)
    }
  }

  def getConsumedMaxOffset(streamTopics: Seq[StreamTopicTemp]): Seq[StreamTopicTemp] = {
    try {
      val seq = new ListBuffer[StreamTopicTemp]
      streamTopics.map(_.streamId).distinct.foreach(
        streamId => {
          val topicSeq = streamTopics.filter(_.streamId == streamId)
          val topicFeedbackSeq = Await.result(db.run(feedbackOffsetTable.filter(_.streamId === streamId).sortBy(_.feedbackTime.desc).take(topicSeq.size + 1).result).mapTo[Seq[FeedbackOffset]], minTimeOut)
          val map = topicFeedbackSeq.map(topic => (topic.topicName, topic.partitionOffsets)).toMap
          topicSeq.foreach(
            topic => {
              if (map.contains(topic.name))
                seq += StreamTopicTemp(topic.id, topic.streamId, topic.name, map(topic.name), topic.rate)
              else seq += topic
            }
          )
        })
      seq
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get stream consumed latest offset from feedback failed", ex)
        throw ex
    }
  }

  def updateOffset(streamId: Long, topicId: Long, offset: String, rate: Int, userId: Long): Future[Int] = {
    db.run(streamInTopicTable.filter(topic => topic.streamId === streamId && topic.nsDatabaseId === topicId)
      .map(topic => (topic.partitionOffsets, topic.rate, topic.updateTime, topic.updateBy))
      .update(offset, rate, currentSec, userId)).mapTo[Int]
  }
}
