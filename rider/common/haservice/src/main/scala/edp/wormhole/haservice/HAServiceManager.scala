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
package edp.wormhole.haservice

import akka.actor.{ActorRef, Props,ActorSystem}
import scala.concurrent.duration.DurationInt

class HAServiceManager( system:ActorSystem, serviceProps: Props, actorName: String ) {
  val serviceActor: ActorRef = system.actorOf(serviceProps,actorName)
  val HAServiceManagerProxy = system.actorOf(AkkaRetry.props(retryTimes = 5, retryTimeOut = 3000.millis, retryInterval = 5000.millis, serviceActor), s"HAServiceFor$actorName")
  def stopManager ={
    println(s" =============== stopManager \n")
    system.stop(HAServiceManagerProxy)
    system.stop(serviceActor)
  }
}
