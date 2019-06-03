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

import akka.actor.Actor
import edp.rider.common.{HistoryDelete, RiderLogger, Stop, RefreshYarn}

class SchedulerActor extends Actor with RiderLogger {

  override def receive = {
    case HistoryDelete =>
      ScheduledTask.deleteHistory
      riderLogger.info(s"Rider delete feedback data timer scheduler ${new java.util.Date().toString} start")

    case RefreshYarn =>
      ScheduledTask.updateAllStatusByYarn
      //riderLogger.info(s"Rider update stream status by yarn app status ${new java.util.Date().toString} start")

    case Stop =>
      super.postStop()
  }
}


