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

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration._

class StreamInTopicDal(streamInTopicTable: TableQuery[StreamInTopicTable],
                       nsDatabaseTable: TableQuery[NsDatabaseTable],
                       feedbackOffsetTable: TableQuery[FeedbackOffsetTable]) extends BaseDalImpl[StreamInTopicTable, StreamInTopic](streamInTopicTable) with RiderLogger {

  //  def getStreamTopic(streamIds: Seq[Long], latestOffset: Boolean = true): Seq[StreamTopicTemp] = {
  //    try {
  //      val streamTopics = Await.result(db.run((streamInTopicTable.filter(_.streamId inSet streamIds) join nsDatabaseTable on (_.nsDatabaseId === _.id))
  //        .map {
  //          case (streamInTopic, nsDatabase) => (streamInTopic.nsDatabaseId, streamInTopic.streamId, nsDatabase.nsDatabase, streamInTopic.partitionOffsets, streamInTopic.rate) <> (StreamTopicTemp.tupled, StreamTopicTemp.unapply)
  //        }.result).mapTo[Seq[StreamTopicTemp]], minTimeOut)
  //      //      if (latestOffset) getConsumedMaxOffset(streamTopics)
  //      //      else streamTopics
  //      streamTopics
  //    } catch {
  //      case ex: Exception =>
  //        throw DatabaseSearchException(ex.getMessage, ex.getCause)
  //    }
  //  }

  def getAutoRegisteredTopics(streamId: Long): Seq[StreamTopicTemp] = {
    Await.result(db.run((streamInTopicTable.filter(_.streamId === streamId) join nsDatabaseTable on (_.nsDatabaseId === _.id))
      .map {
        case (streamInTopic, nsDatabase) => (streamInTopic.id, streamInTopic.streamId, nsDatabase.nsDatabase, streamInTopic.partitionOffsets, streamInTopic.rate) <> (StreamTopicTemp.tupled, StreamTopicTemp.unapply)
      }.result).mapTo[Seq[StreamTopicTemp]], minTimeOut)
  }

  def getAutoRegisteredTopicNameMap(streamId: Long): Map[Long, String] = {
    getAutoRegisteredTopics(streamId).map(topic => (topic.id, topic.name)).toMap
  }

  def getAutoRegisteredTopics(streamIds: Seq[Long]): Seq[StreamTopicTemp] = {
    Await.result(db.run((streamInTopicTable.filter(_.streamId inSet streamIds) join nsDatabaseTable on (_.nsDatabaseId === _.id))
      .map {
        case (streamInTopic, nsDatabase) => (streamInTopic.id, streamInTopic.streamId, nsDatabase.nsDatabase, streamInTopic.partitionOffsets, streamInTopic.rate) <> (StreamTopicTemp.tupled, StreamTopicTemp.unapply)
      }.result).mapTo[Seq[StreamTopicTemp]], minTimeOut)
  }

  def checkAutoRegisteredTopicExists(streamId: Long, topic: String): Boolean = {
    var exist = false
    val topicSearch = Await.result(db.run(
      (nsDatabaseTable.filter(_.nsDatabase === topic) join streamInTopicTable.filter(_.streamId === streamId)
        on (_.id === _.nsDatabaseId)).map {
        case (db, _) => db
      }.result).mapTo[Seq[NsDatabase]], minTimeOut)
    if (topicSearch.nonEmpty) exist = true
    exist
  }

  def updateOffset(streamId: Long, topicId: Long, offset: String, rate: Int, userId: Long): Future[Int] = {
    db.run(streamInTopicTable.filter(topic => topic.streamId === streamId && topic.nsDatabaseId === topicId)
      .map(topic => (topic.partitionOffsets, topic.rate, topic.updateTime, topic.updateBy))
      .update(offset, rate, currentSec, userId)).mapTo[Int]
  }

  def updateOffset(topics: Seq[UpdateTopicOffset]): Seq[Int] = {
    topics.map(topic =>
      Await.result(db.run(streamInTopicTable.filter(_.id === topic.id).map(topic => (topic.partitionOffsets)).update(topic.offset)).mapTo[Int], minTimeOut))
  }

  def updateByStartOrRenew(streamId: Long, topics: Seq[PutTopicDirective], userId: Long): Boolean = {
    val topicMap = getAutoRegisteredTopics(streamId).map(topic => (topic.name, topic.id)).toMap
    topics.filter(_.action.getOrElse(1) == 1).map(
      topic => Await.result(
        db.run(streamInTopicTable.filter(_.id === topicMap(topic.name))
          .map(topic => (topic.partitionOffsets, topic.rate, topic.updateTime, topic.updateBy))
          .update(topic.partitionOffsets, topic.rate, currentSec, userId)), minTimeOut)
    )
    true
  }

  def updateOffsetAndRate(streamId: Long, topicId: Long, offset: String, rate: Int, userId: Long): Future[Int] = {
    db.run(streamInTopicTable.filter(topic => topic.streamId === streamId && topic.nsDatabaseId === topicId)
      .map(topic => (topic.partitionOffsets, topic.rate, topic.updateTime, topic.updateBy))
      .update(offset, rate, currentSec, userId)).mapTo[Int]
  }

}
