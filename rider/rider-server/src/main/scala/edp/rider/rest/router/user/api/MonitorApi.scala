package edp.rider.rest.router.user.api

import java.text.SimpleDateFormat

import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.dal.{FlowDal, MonitorInfoDal, StreamDal}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.{JsonSerializer, ResponseJson, SessionClass}
import edp.rider.rest.util.AuthorizationProvider
import edp.rider.rest.util.ResponseUtils.{getHeader, setFailedResponse}
import edp.rider.rest.util.CommonUtils._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

class MonitorApi(flowDal: FlowDal, streamDal: StreamDal,monitorInfoDal: MonitorInfoDal) extends BaseUserApiImpl[MonitorInfoTable, MonitorInfo](monitorInfoDal) with RiderLogger with JsonSerializer{

  lazy val dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSZ")
  lazy val frontFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getMonitorInfoByFlowId(route: String):Route=path(route / "monitor" / LongNumber / "flow" / LongNumber){
    (projectId,flowId) =>
      post{
        entity(as[MonitorTimeSpan]){
          timeSpan=>
          authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
            session =>
              if (session.roleType != "user") {
                riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                complete(OK, setFailedResponse(session, "Insufficient permission"))
              }else{
                if (session.projectIdList.contains(projectId)) {
                  val monitorInfoOpt=if(RiderConfig.monitor.databaseType.trim.equalsIgnoreCase("es"))ElasticSearch.queryESFlowMonitor(projectId, flowId, dateFormat.format(frontFormat.parse(timeSpan.startTime)), dateFormat.format(frontFormat.parse(timeSpan.endTime)))._2.toSeq
                  else Await.result(monitorInfoDal.findByFilter(monitor=>monitor.projectId==projectId && monitor.flowId==flowId && dateFormat.parse(monitor.umsTs.toString).getTime>=dateFormat.parse(timeSpan.startTime).getTime && dateFormat.parse(monitor.umsTs.toString).getTime<=dateFormat.parse(timeSpan.endTime).getTime),minTimeOut)
                  Option(monitorInfoOpt) match {
                    case Some(monitorSeq)=>
                      val flowMonitorInfo=fillFlowMetricsByMonitor(monitorSeq.asInstanceOf[Seq[MonitorInfo]])
                      complete(OK, ResponseJson[MonitorDashBoard](getHeader(200, session),MonitorDashBoard(Seq(flowMonitorInfo))))
                    case None=>
                      complete(OK, ResponseJson[String](getHeader(200, session), ""))
                  }
                }else{
                  riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                  complete(OK, setFailedResponse(session, "Insufficient permission"))
                }
              }
          }
        }

    }
  }



  def fillFlowMetricsByMonitor(monitorSeq:Seq[MonitorInfo])={
    val flowSeq=monitorSeq.map(monitor=>
      MonitorMetric(monitor.flowNamespace,MonitorNumberWidget(monitor.rddCount,monitor.umsTs),MonitorIntervalWidget(monitor.interval.intervalDataProcessToDataums,monitor.umsTs),
        MonitorIntervalWidget(monitor.interval.intervalDataProcessToRdd,monitor.umsTs),MonitorIntervalWidget(monitor.interval.intervalSwiftsToSink,monitor.umsTs),MonitorIntervalWidget(monitor.interval.intervalSinkToDone,monitor.umsTs),
        MonitorIntervalWidget(monitor.interval.intervalDataProcessToDone,monitor.umsTs),MonitorOpsWidget(monitor.throughput,monitor.umsTs)))
    val flowName=flowSeq.headOption.getOrElse[MonitorMetric](MonitorMetric()).flowName
    val flowInfoMetric=MonitorFlowInfo(flowName)
    flowSeq.foreach(flowOpt=>{
      Option(flowOpt) match {
        case Some(flow)=>
          flowInfoMetric.receivedDelays += flow.receivedDelay
          flowInfoMetric.preProcessDelays += flow.preProcessDelay
          flowInfoMetric.rddCountMetrics += flow.rddCountMetric
          flowInfoMetric.swiftsDelays += flow.swiftsDelay
          flowInfoMetric.sinkDelays += flow.sinkDelay
          flowInfoMetric.wormholeDelays += flow.wormholeDelay
          flowInfoMetric.throughPutMetrics += flow.throughPutMetric
        case None=>
      }
    })
    flowInfoMetric
  }

  def getMonitorInfoByStreamId(route: String):Route=path(route  / "monitor"/ LongNumber / "stream" / LongNumber){
    (projectId,streamId) =>
      post{
        entity(as[MonitorTimeSpan]) {
          timeSpan =>
            authenticateOAuth2Async[SessionClass]("rider", AuthorizationProvider.authorize) {
              session =>
                if (session.roleType != "user") {
                  riderLogger.warn(s"user ${session.userId} has no permission to access it.")
                  complete(OK, setFailedResponse(session, "Insufficient permission"))
                } else {
                  if (session.projectIdList.contains(projectId)) {
                    val monitorInfoOpt = if(RiderConfig.monitor.databaseType.trim.equalsIgnoreCase("es")) ElasticSearch.queryESStreamMonitor(projectId,streamId,dateFormat.format(frontFormat.parse(timeSpan.startTime)),dateFormat.format(frontFormat.parse(timeSpan.endTime)))._2.toSeq
                    else Await.result(monitorInfoDal.findByFilter(monitor => monitor.projectId==projectId && monitor.streamId == streamId && dateFormat.parse(monitor.umsTs.toString).getTime>=dateFormat.parse(timeSpan.startTime).getTime && dateFormat.parse(monitor.umsTs.toString).getTime<=dateFormat.parse(timeSpan.endTime).getTime), minTimeOut)
                    Option(monitorInfoOpt) match {
                      case Some(monitorSeq) =>
                        val monitorMap =new mutable.HashMap[Long, ListBuffer[MonitorInfo]]()
                        val flowMonitorSeq=ListBuffer[MonitorFlowInfo]()
                        monitorSeq.asInstanceOf[Seq[MonitorInfo]].foreach(info =>
                          if(monitorMap.contains(info.flowId))monitorMap(info.flowId).append(info)
                          else monitorMap+=(info.flowId->ListBuffer[MonitorInfo](info)))

                        val iterator=monitorMap.keysIterator
                        while(iterator.hasNext){
                          val key=iterator.next()
                          flowMonitorSeq += fillFlowMetricsByMonitor(monitorMap(key).toSeq)
                        }
                        complete(OK, ResponseJson[MonitorDashBoard](getHeader(200, session), MonitorDashBoard(flowMonitorSeq)))
                      case None =>
                        complete(OK, ResponseJson[String](getHeader(200, session), ""))
                    }
                  } else {
                    riderLogger.error(s"user ${session.userId} doesn't have permission to access the project $projectId.")
                    complete(OK, setFailedResponse(session, "Insufficient permission"))
                  }
                }
            }
        }
    }
  }
}
