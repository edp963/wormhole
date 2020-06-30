package edp.rider.rest.router

import java.util.UUID

import akka.actor.ActorRef

/**
  * @author Suxy
  * @date 2020/6/12
  * @description file description
  */
object DebugRefProtocol {

  case class SignedMessage(uuid: UUID, msg: String)
  case class OpenConnection(actor: ActorRef, uuid: UUID)
  case class CloseConnection(uuid: UUID)

}
