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

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.{Rep, Tag}

import scala.collection.mutable.ListBuffer


case class MonitorInfo(
                        id: Long,
                        projectId: Long,
                        batchId: String,
                        streamId: Long,
                        flowId: Long,
                        sourceNamespace: String,
                        sinkNamespace: String,
                        dataType: String,
                        rddCount: Int,
                        topics: String = "",
                        throughput: Long,
                        dataGeneratedTs: String,
                        rddTs: String,
                        DataProcessTs: String,
                        swiftsTs: String,
                        sinkTs: String,
                        doneTs: String,
                        interval: Interval,
                        feedbackTime: String) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
  lazy val flowNamespace=sourceNamespace+"_"+sinkNamespace
}

case class StreamMonitorInfo(streamId: Long, flowNs: String)

case class MonitorTimeSpan(startTime: Long, endTime: Long) extends SimpleBaseEntity

case class MonitorNumberWidget(count: Long, umsTs: String)

case class MonitorIntervalWidget(time: Long, umsTs: String)

case class MonitorOpsWidget(ops: Double, umsTs: String)

case class MonitorFlowInfo(flowName: String,
                           cols: String = "rddCountMetrics,throughPutMetrics,receivedDelays@preProcessDelays@swiftsDelays@sinkDelays@wormholeDelays@rddToSwiftsDelays",
                           rddCountMetrics: ListBuffer[MonitorNumberWidget] = new ListBuffer[MonitorNumberWidget](),
                           receivedDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           preProcessDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           rddToSwiftsDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           swiftsDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           sinkDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           wormholeDelays: ListBuffer[MonitorIntervalWidget] = new ListBuffer[MonitorIntervalWidget](),
                           throughPutMetrics: ListBuffer[MonitorOpsWidget] = new ListBuffer[MonitorOpsWidget]())

case class MonitorMetric(flowName: String = "",
                         rddCountMetric: MonitorNumberWidget = MonitorNumberWidget(0L, ""),
                         receivedDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         preProcessDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         rddToSwiftsDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         swiftsDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         sinkDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         wormholeDelay: MonitorIntervalWidget = MonitorIntervalWidget(0L, ""),
                         throughPutMetric: MonitorOpsWidget = MonitorOpsWidget(0.0, ""))

case class MonitorDashBoard(flowMetrics: Seq[MonitorFlowInfo])

case class Interval(intervalDataProcessToDataums: Long,
                    intervalDataProcessToRdd: Long,
                    intervalRddToSwifts: Long,
                    intervalDataProcessToDone: Long,
                    intervalSwiftsToSink: Long,
                    intervalSinkToDone: Long)

class MonitorInfoTable(_tableTag: Tag) extends BaseTable[MonitorInfo](_tableTag, "feedback_flow_stats") {
  def * = (id, projectId,batchId,streamId, flowId,sourceNamespace,sinkNamespace,dataType,rddCount,
    topics, throughput, dataGeneratedTs, rddTs, DataProcessTs, swiftsTs, sinkTs, doneTs,
    interval,feedbackTime) <> ((MonitorInfo.apply _).tupled, MonitorInfo.unapply)

  def interval = (intervalDataProcessToDataums, intervalDataProcessToRdd, intervalRddToSwifts,
    intervalDataProcessToDone, intervalSwiftsToSink, intervalSinkToDone) <> ((Interval.apply _).tupled, Interval.unapply)

  val projectId: Rep[Long] = column[Long]("project_id")
  val streamId: Rep[Long] = column[Long]("stream_id")
  val batchId: Rep[String] = column[String]("batch_id")
  val flowId: Rep[Long] = column[Long]("flow_id")
  val sourceNamespace: Rep[String] = column[String]("source_ns")
  val sinkNamespace: Rep[String] = column[String]("sink_ns")
  val dataType: Rep[String] = column[String]("data_type")
  val rddCount: Rep[Int] = column[Int]("rdd_count")
  val topics: Rep[String] = column[String]("topics")
  val throughput: Rep[Long] = column[Long]("throughput")
  val dataGeneratedTs: Rep[String] = column[String]("data_generated_ts")
  val rddTs: Rep[String] = column[String]("rdd_ts")
  val DataProcessTs: Rep[String] = column[String]("data_process_ts")
  val swiftsTs: Rep[String] = column[String]("swifts_ts")
  val sinkTs: Rep[String] = column[String]("sink_ts")
  val doneTs: Rep[String] = column[String]("done_ts")
  val feedbackTime: Rep[String] = column[String]("feedback_time")

  val intervalDataProcessToDataums: Rep[Long] = column[Long]("interval_data_process_to_data_ums")
  val intervalDataProcessToRdd: Rep[Long] = column[Long]("interval_data_process_to_rdd")
  val intervalRddToSwifts: Rep[Long]=  column[Long]("interval_rdd_to_swifts")
  val intervalDataProcessToDone: Rep[Long] = column[Long]("interval_data_process_to_done")
  val intervalSwiftsToSink: Rep[Long] = column[Long]("interval_swifts_to_sink")
  val intervalSinkToDone: Rep[Long] = column[Long]("interval_sink_to_done")

  lazy val flowNamespace=sourceNamespace+"_"+sinkNamespace
}
