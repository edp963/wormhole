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

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import edp.rider.common.RiderLogger

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object AkkaRetry extends RiderLogger {

  private case class RetryEntry(originalSender: ActorRef, message: Any, times: Int)

  private case class ResponseEntry(originalSender: ActorRef, result: Any)

  def props(retryTimes: Int, retryTimeOut: FiniteDuration, retryInterval: FiniteDuration, actorHandler: ActorRef): Props = Props(new AkkaRetry(retryTimes: Int, retryTimeOut: FiniteDuration, retryInterval: FiniteDuration, actorHandler: ActorRef))
}

class AkkaRetry(val retryTimes: Int, retryTimeOut: FiniteDuration, retryInterval: FiniteDuration, actorHandler: ActorRef) extends Actor with RiderLogger {

  import AkkaRetry._
  import context.dispatcher

  def retryCheck: Receive = {
    case ResponseEntry(originalSender, result) =>
      originalSender ! result
      context stop self
    case RetryEntry(originalSender, message, triesLeft) =>
      (actorHandler ? message) (retryTimeOut) onComplete {
        case Success(result) =>
          self ! ResponseEntry(originalSender, result)
        case Failure(err) =>
          if (triesLeft == 1) {
            self ! ResponseEntry(originalSender, Failure(new Exception(s" akka retry exceeded")))
          } else {
            riderLogger.error(s" akka retry failed ${err.getMessage}")
          }
      }
      if (triesLeft > 1)
        context.system.scheduler.scheduleOnce(retryInterval, self, RetryEntry(originalSender, message, triesLeft - 1))
    case msg@_ => riderLogger.error(s"No handling defined for message: $msg")
  }

  def receive: Receive = {
    case message@_ =>
      self ! RetryEntry(sender, message, retryTimes)
      context.become(retryCheck, discardOld = false)
  }
}
