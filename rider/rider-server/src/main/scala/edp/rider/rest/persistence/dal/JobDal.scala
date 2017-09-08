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

import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.common.AppInfo
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await

class JobDal(jobTable: TableQuery[JobTable]) extends BaseDalImpl[JobTable, Job](jobTable) {

  def updateJobStatus(jobId: Long, appInfo: AppInfo) = {
    Await.result(db.run(jobTable.filter(_.id === jobId).map(c => (c.sparkAppid, c.status, c.startedTime, c.stoppedTime, c.updateTime))
      .update(Option(appInfo.appId), appInfo.appState, Option(appInfo.startedTime), Option(appInfo.startedTime), currentSec)), minTimeOut)
  }

  def updateJobStatus(jobId: Long, status: String) = {
    Await.result(db.run(jobTable.filter(_.id === jobId).map(c => (c.status,c.updateTime))
      .update(status, currentSec)), minTimeOut)
  }
}
