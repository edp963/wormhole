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

import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class FeedbackOffsetDal(feedbackOffsetTable: TableQuery[FeedbackOffsetTable]) extends BaseDalImpl[FeedbackOffsetTable, FeedbackOffset](feedbackOffsetTable) with RiderLogger{

  def getLatestOffset(streamId: Long, topic: String, partitionId: Int): Future[Option[Long]] = {
    super.findByFilter(str => str.streamId === streamId && str.topicName === topic && str.partitionNum === partitionId)
      .map[Option[Long]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.partitionOffset).max))
  }

  def getLatestTopicOffset(topics: Seq[StreamTopicPartition]): Seq[TopicOffset] = {
    val topicList: ListBuffer[TopicOffset] = new ListBuffer()
      topics.foreach{topic =>
        var pid = 0
        while (pid < topic.partitions.getOrElse(1)) {
          try {
            val offset: Long = Await.result(getLatestOffset(topic.streamId, topic.topicName, pid), Duration.Inf).getOrElse(0)
            if (offset >= 0) topicList.append(TopicOffset(topic.topicName, pid, offset))
          } catch {
            case e: Exception =>
              riderLogger.error(s"Failed to get latest offset", e)
          }
          pid += 1
        }
      }
    topicList
  }

  def deleteHistory( pastNdays : String ) = super.deleteByFilter(_.feedbackTime <= pastNdays )

}
