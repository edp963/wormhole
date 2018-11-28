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


package edp.rider.rest.persistence.entities


import edp.rider.rest.persistence.base.{BaseEntity, BaseTable}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}



case class MonitorInfo(
                        id: Long,
                        umsTs: String,
                        projectId: Long,
                        streamId: Long,
                        streamName: String,
                        flowId: Long,
                        flowNamespace: String,
                        rddCount: Int,
                        throughput: Long,
                        dataGeneratedTs: String,
                        rddTs: String,
                        directiveTs: String,
                        DataProcessTs: String,
                        swiftsTs: String,
                        sinkTs: String,
                        doneTs: String,
                        interval:Interval)extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

/*case class FeedbackFlowStats(
                              protocolType: String,
                              dataType: String,
                              umsTs: String,
                              streamId: Long,
                              stats_id: String,
                              sourceNamespace: String,
                              sinkNamespace: String,
                              rddCount: Int,
                              dataUmsTs: Long,
                              rddTs: Long,
                              directiveTs: Long,
                              DataProcessTs: Long,
                              swiftsTs: Long,
                              sinkTs: Long,
                              doneTs: Long)*/

case class StreamMonitorInfo(streamId: Long, flowNs: String)

case class Interval(intervalDataProcessToDataums:Long,
                    intervalDataProcessToRdd:Long,
                    intervalDataProcessToSwifts:Long,
                    intervalDataProcessToSink:Long,
                    intervalDataProcessToDone:Long,
                    intervalDataumsToDone:Long,
                    intervalRddToDone:Long,
                    intervalSwiftsToSink:Long,
                    intervalSinkToDone:Long)

class MonitorInfoTable(_tableTag: Tag) extends BaseTable[MonitorInfo](_tableTag, "feedback_flow_stats"){
  def * =(id,umsTs,projectId,streamId,streamName,flowId,flowNamespace,rddCount,
      throughput,dataGeneratedTs,rddTs,directiveTs,DataProcessTs,swiftsTs,sinkTs,doneTs,
      interval) <> ((MonitorInfo.apply _).tupled,MonitorInfo.unapply)
  def interval=(intervalDataProcessToDataums,intervalDataProcessToRdd,intervalDataProcessToSwifts,
    intervalDataProcessToSink,intervalDataProcessToDone,intervalDataumsToDone,intervalRddToDone,intervalSwiftsToSink,intervalSinkToDone) <> ((Interval.apply _).tupled,Interval.unapply)
  val umsTs:Rep[String]=column[String]("ums_ts")
  val projectId:Rep[Long]=column[Long]("project_id")
  val streamId:Rep[Long]=column[Long]("stream_id")
  val streamName:Rep[String]=column[String]("stream_name")
  val flowId:Rep[Long]=column[Long]("flow_id")
  val flowNamespace:Rep[String]=column[String]("flow_namespace")
  val rddCount:Rep[Int]=column[Int]("rdd_count")
  val throughput:Rep[Long]=column[Long]("throughput")
  val dataGeneratedTs:Rep[String]=column[String]("data_generated_ts")
  val rddTs:Rep[String]=column[String]("rdd_ts")
  val directiveTs:Rep[String]=column[String]("directive_ts")
  val DataProcessTs:Rep[String]=column[String]("data_process_ts")
  val swiftsTs:Rep[String]=column[String]("swifts_ts")
  val sinkTs:Rep[String]=column[String]("sink_ts")
  val doneTs:Rep[String]=column[String]("done_ts")

  val intervalDataProcessToDataums:Rep[Long]=column[Long]("interval_data_process_to_data_ums")
  val intervalDataProcessToRdd:Rep[Long]=column[Long]("interval_data_process_to_rdd")
  val intervalDataProcessToSwifts:Rep[Long]=column[Long]("interval_data_process_to_swifts")
  val intervalDataProcessToSink:Rep[Long]=column[Long]("interval_data_process_to_sink")
  val intervalDataProcessToDone:Rep[Long]=column[Long]("interval_data_process_to_done")
  val intervalDataumsToDone:Rep[Long]=column[Long]("interval_data_ums_done")
  val intervalRddToDone:Rep[Long]=column[Long]("interval_rdd_done")
  val intervalSwiftsToSink:Rep[Long]=column[Long]("interval_swifts_sink")
  val intervalSinkToDone:Rep[Long]=column[Long]("interval_sink_done")
}
