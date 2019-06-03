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
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.{currentSec, minTimeOut}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import edp.rider.RiderStarter.modules._

import scala.concurrent.{Await, Future}

class FlowInTopicDal(flowInTopicTable: TableQuery[FlowInTopicTable],
                     nsDatabaseTable: TableQuery[NsDatabaseTable],
                     feedbackOffsetTable: TableQuery[FeedbackOffsetTable]) extends BaseDalImpl[FlowInTopicTable, FlowInTopic](flowInTopicTable) with RiderLogger {


  def checkAutoRegisteredTopicExists(flowId: Long, topic: String): Boolean = {
    var exist = false
    val topicSearch = Await.result(db.run(
      (nsDatabaseTable.filter(_.nsDatabase === topic) join flowInTopicTable.filter(_.flowId === flowId)
        on (_.id === _.nsDatabaseId)).map {
        case (db, _) => db
      }.result).mapTo[Seq[NsDatabase]], minTimeOut)
    if (topicSearch.nonEmpty) exist = true
    exist
  }

  def getAutoRegisteredTopics(flowIds: Seq[Long]): Seq[FlowTopicTemp] = {
    Await.result(db.run((flowInTopicTable.filter(_.flowId inSet flowIds) join nsDatabaseTable on (_.nsDatabaseId === _.id))
      .map {
        case (flowInTopic, nsDatabase) => (flowInTopic.id, flowInTopic.flowId, nsDatabase.nsDatabase, flowInTopic.partitionOffsets, flowInTopic.rate) <> (FlowTopicTemp.tupled, FlowTopicTemp.unapply)
      }.result).mapTo[Seq[FlowTopicTemp]], minTimeOut)
  }


  def updateOffset(topics: Seq[UpdateTopicOffset]): Seq[Int] = {
    topics.map(topic =>
      Await.result(db.run(flowInTopicTable.filter(_.id === topic.id).map(topic => (topic.partitionOffsets)).update(topic.offset)).mapTo[Int], minTimeOut))
  }

  def updateByStart(flowId: Long, topics: Seq[PutFlowTopicDirective], userId: Long): Boolean = {
    val topicMap = getAutoRegisteredTopics(Seq(flowId)).map(topic => (topic.topicName, topic.id)).toMap
    topics.map(
      topic => Await.result(
        db.run(flowInTopicTable.filter(_.id === topicMap(topic.name))
          .map(topic => (topic.partitionOffsets,topic.updateTime, topic.updateBy))
          .update(topic.partitionOffsets, currentSec, userId)), minTimeOut)
    )
    true
  }

}
