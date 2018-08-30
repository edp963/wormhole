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


package edp.rider

import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.kafka.ConsumerManager
import edp.rider.module._
import edp.rider.monitor.ElasticSearch
import edp.rider.rest.persistence.entities.User
import edp.rider.rest.router.RoutesApi
import edp.rider.rest.util.CommonUtils._
import edp.rider.schedule.Scheduler
import edp.rider.service.util.CacheMap
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.util.{Failure, Success}

object RiderStarter extends App with RiderLogger {

  lazy val modules = new ConfigurationModuleImpl
    with ActorModuleImpl
    with PersistenceModuleImpl
    with BusinessModuleImpl
    with RoutesModuleImpl

  implicit val system = modules.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  DbModule.createSchema

  if (Await.result(modules.userDal.findByFilter(_.email === RiderConfig.riderServer.adminUser), minTimeOut).isEmpty)
    Await.result(modules.userDal.insert(User(0, RiderConfig.riderServer.adminUser, RiderConfig.riderServer.adminPwd, RiderConfig.riderServer.adminUser, "admin", RiderConfig.riderServer.defaultLanguage, active = true, currentSec, 1, currentSec, 1)), minTimeOut)

  val future = Http().bindAndHandle(new RoutesApi(modules).routes, RiderConfig.riderServer.host, RiderConfig.riderServer.port)

  future.onComplete {
    case Success(_) =>
      riderLogger.info(s"WormholeServer http://${RiderConfig.riderServer.host}:${RiderConfig.riderServer.port}/.")

      CacheMap.cacheMapInit

      ElasticSearch.initial(RiderConfig.es, RiderConfig.grafana)

      new ConsumerManager(modules)
      riderLogger.info(s"WormholeServer Consumer started")
      Scheduler.start
      riderLogger.info(s"Wormhole Scheduler started")

    case Failure(e) =>
      riderLogger.error(e.getMessage)
      system.terminate()
  }
}
