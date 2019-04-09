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


package edp.rider.schedule

import java.util.{Calendar, Date, GregorianCalendar}

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.module._
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.util.CommonUtils
import edp.rider.yarn.YarnStatusQuery
import edp.wormhole.util.{DateUtils, DtFormat}

object ScheduledTask extends RiderLogger {
  lazy val modules = new ConfigurationModuleImpl with ActorModuleImpl with PersistenceModuleImpl

  def deleteHistory = {
    try {
      val cal = new GregorianCalendar()

      cal.setTime(new java.util.Date())
      cal.add(Calendar.DAY_OF_MONTH, (-1) * RiderConfig.maintenance.mysqlRemain)
      var pastNdays: Date = cal.getTime()

      modules.feedbackErrDal.deleteHistory(DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_SEC))
      //      modules.feedbackHeartbeatDal.deleteHistory(DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_SEC))
      //      modules.feedbackStreamErrDal.deleteHistory(DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_SEC))
      //
      //      modules.feedbackOffsetDal.deleteHistory(DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_SEC))
      //      riderLogger.info(s"delete the feedback history past ${RiderConfig.maintenance.mysqlRemain} days")
      if (RiderConfig.monitor.databaseType.equalsIgnoreCase("es") && RiderConfig.es != null) {
        cal.setTime(new java.util.Date())
        cal.add(Calendar.DAY_OF_MONTH, (-1) * RiderConfig.maintenance.esRemain)
        pastNdays = cal.getTime()
        val fromDate = "2017-01-01 00:00:00.000000" + CommonUtils.getTimeZoneId
        ElasticSearch.deleteEsHistory(fromDate, DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_MICROSEC) + CommonUtils.getTimeZoneId)
        riderLogger.info(s"delete ES feedback history data from $fromDate to ${DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_MICROSEC)}")
      } else if (!RiderConfig.monitor.databaseType.equalsIgnoreCase("es")) {
        modules.monitorInfoDal.deleteHistory(DateUtils.dt2string(pastNdays, DtFormat.TS_DASH_SEC))
      }
    } catch {
      case e: Exception =>
        riderLogger.error(s"failed to delete feedback history data", e)
    }
  }

  def updateAllStatusByYarn: Unit = {
    try {
      YarnStatusQuery.updateStatusByYarn
    } catch {
      case e: Exception =>
        //Thread.sleep(RiderConfig.refreshInterval*1000)
        riderLogger.error(s"failed to update stream status by yarn app status", e)
    }
  }
}
