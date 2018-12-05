package edp.rider.rest.persistence.dal

import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.{maxTimeOut, minTimeOut}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class MonitorInfoDal (monitorInfoTable: TableQuery[MonitorInfoTable],
                      streamDal: StreamDal,
                      flowDal: FlowDal) extends BaseDalImpl[MonitorInfoTable, MonitorInfo](monitorInfoTable){

  def queryESFlowLastestTs(projectId: Long, streamId: Long, flowId: Long): Future[Option[String]] = {
      super.findByFilter(str=>str.projectId===projectId && str.streamId===streamId && str.flowId ===flowId)
        .map[Option[String]](seq =>
        if (seq.isEmpty) None
        else Some(seq.map(_.dataGeneratedTs).max))
  }

  def queryESStreamLastestTs(projectId: Long, streamId: Long): Future[Option[String]] = {
    super.findByFilter(str=>str.projectId===projectId && str.streamId===streamId)
      .map[Option[String]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.dataGeneratedTs).max))
  }

  def deleteHistory(pastNdays: String)={
    val ignoreIds = new ListBuffer[String]
    val existSeq = Await.result(super.findAll, maxTimeOut).map(
      monitorInfo => StreamMonitorInfo(monitorInfo.streamId, monitorInfo.flowNamespace)
    ).distinct
    val streamIds = Await.result(streamDal.findAll, maxTimeOut).map(_.id)
    val sourceSinks = Await.result(flowDal.findAll, maxTimeOut).map(flow => flow.sourceNs + "_" + flow.sinkNs)
    existSeq.filter(monitorInfo => streamIds.contains(monitorInfo.streamId))
      .filter(monitorInfo => sourceSinks.contains(monitorInfo.flowNs))
      .map(monitorInfo => {
        val maxMonitorInfo = Await.result(
          db.run(monitorInfoTable
            .filter(table => table.streamId === monitorInfo.streamId &&
              table.flowNamespace === monitorInfo.flowNs)
            .sortBy(_.doneTs).take(1).result), minTimeOut)
        if (maxMonitorInfo.nonEmpty) ignoreIds += maxMonitorInfo.head.statsId
      })
    val deleteSeq =Await.result(db.run(monitorInfoTable.withFilter(_.doneTs<=pastNdays).map(_.statsId).result).mapTo[Seq[String]],maxTimeOut)
    if(!deleteSeq.isEmpty)Await.result(super.deleteByFilter(monitorInfo => monitorInfo.statsId <= deleteSeq.max && !monitorInfo.statsId.inSet(ignoreIds)), maxTimeOut)
  }
}
