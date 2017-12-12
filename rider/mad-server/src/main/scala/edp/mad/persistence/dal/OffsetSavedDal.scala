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


package edp.mad.persistence.dal

import edp.mad.module.{ConfigModuleImpl, DBDriverModuleImpl}
import edp.mad.persistence.base.BaseDalImpl
import edp.mad.persistence.entities._
import org.apache.log4j.Logger
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.{Await, Future}

class OffsetSavedDal(offsetSavedTableQ: TableQuery[OffsetSavedTable]) extends BaseDalImpl[OffsetSavedTable, OffsetTableEntity](offsetSavedTableQ) with ConfigModuleImpl with DBDriverModuleImpl {
  private val logger = Logger.getLogger(this.getClass)
  def updateOrInsert(rows: Seq[OffsetTableEntity]) = {
    rows.foreach { row =>
      logger.info(s"updateOrInsert ${row} ")
      try {
        if( Await.result(super.findByFilter(e=> (e.streamId === row.streamId && e.topicName === row.topicName && e.partitionNum === row.partitionNum)), FiniteDuration(180, SECONDS) ).isEmpty){
          db.run(offsetSavedTableQ += row).mapTo[Seq[OffsetTableEntity]]
          logger.info(s"insert row \n")
        } else {
          db.run(offsetSavedTableQ.filter(e=> (e.streamId === row.streamId && e.topicName === row.topicName && e.partitionNum === row.partitionNum)).update(row)).mapTo[Seq[OffsetTableEntity]]
          logger.info(s"update row  \n")
        }
      } catch {
        case e: Exception =>
          logger.error(s" Flow table query error ", e)
      }
    }
  }

  def getLatestOffset(streamId: Long, topic: String): Future[Option[OffsetTableEntity]] = {
    db.run(offsetSavedTableQ.filter(str => str.streamId === streamId && str.topicName === topic ).sortBy(_.feedbackTime.desc).result.headOption)
  }

//  def getDistinctStreamTopicList(streamId: Long): Future[Seq[StreamTopicPartitionId]] ={
//    db.run(offsetInfoTable.filter(str => str.streamId === streamId)
//      map{case(str)=>(str.streamId,str.topicName,str.partitionNum ) <> (StreamTopicPartitionId.tupled, StreamTopicPartitionId.unapply)
//      }.distinct.result).mapTo[Seq[StreamTopicPartitionId]]
//  }
}
