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

import java.util.Properties

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
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
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

      val props=new Properties()

      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // key反序列化方式
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer") // value反系列化方式
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RiderConfig.consumer.brokers) // 指定broker地址，来找到group的coordinator
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
      props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,"80000")
      if(RiderConfig.kerberos.enabled){
        props.put("security.protocol","SASL_PLAINTEXT")
        props.put("sasl.kerberos.service.name", "kafka")
      }
      val consumer:KafkaConsumer[String, String]=new KafkaConsumer[String, String](props)

      new ConsumerManager(modules,consumer)
      riderLogger.info(s"WormholeServer Consumer started")
      Scheduler.start
      riderLogger.info(s"Wormhole Scheduler started")

    case Failure(e) =>
      riderLogger.error(e.getMessage)
      system.terminate()
  }
}
