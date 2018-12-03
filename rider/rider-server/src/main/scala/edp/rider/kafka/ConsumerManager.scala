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


package edp.rider.kafka

import akka.actor.{ActorRef, Props}
import edp.rider.common.RiderLogger
import edp.rider.module.{ActorModuleImpl, ConfigurationModule, PersistenceModule}
import org.apache.kafka.clients.consumer.KafkaConsumer


class ConsumerManager(modules: ConfigurationModule with PersistenceModule with ActorModuleImpl) extends RiderLogger {
  implicit val system = modules.system

  val RiderConsumer: ActorRef = system.actorOf(Props(new RiderConsumer(modules)))
  //  val ConsumerManagerProxy = system.actorOf(AkkaRetry.props(retryTimes = 5, retryTimeOut = 3000.millis, retryInterval = 5000.millis, RiderConsumer))

  def stopManager = {
    //    system.stop(ConsumerManagerProxy)
    system.stop(RiderConsumer)
  }
}
