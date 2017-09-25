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

import akka.actor.{Actor, ActorRef, Props}
import edp.rider.common.RiderLogger
import edp.rider.module.{ActorModuleImpl, ConfigurationModuleImpl, PersistenceModuleImpl}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object Scheduler extends ConfigurationModuleImpl with RiderLogger {
  lazy val modules = new ConfigurationModuleImpl with ActorModuleImpl with PersistenceModuleImpl

  def start: Unit = {
    val system = modules.system
    val schedulerActor: ActorRef = system.actorOf(Props[SchedulerActor])
    system.scheduler.schedule(30.seconds, 1.days, schedulerActor, "databaseMaintenance")

  }

  class SchedulerActor extends Actor {
    def receive = {
      case "databaseMaintenance" => {
        ScheduledTask.deleteHistory
        riderLogger.info(s"Rider delete feedback data timer scheduler ${new java.util.Date().toString} start")
      }
      case _ => {}
        riderLogger.info(s"timer ${new java.util.Date().toString}")
    }
  }

}
