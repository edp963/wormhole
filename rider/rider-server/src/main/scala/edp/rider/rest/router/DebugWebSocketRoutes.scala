package edp.rider.rest.router

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule}

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * @author Suxy
  * @date 2020/6/10
  * @description file description
  */
class DebugWebSocketRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule) extends Directives {

  lazy val routes: Route = debugWebSocketRoute

  def debugWebSocketRoute: Route = debugWebSocket()

  implicit val system: ActorSystem = ActorSystem()
  implicit val executor: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val debugRef = system.actorOf(Props[DebugRef], "debugRef")

  def debugWebSocket(): Route = path("debug" / "ws") {
    get {
      handleWebSocketMessages(debugFlow)
    }
  }

  def debugFlow: Flow[Message, Message, Any] =
    Flow[Message]
      .mapAsync(1) {
        case TextMessage.Strict(s) =>
          Future.successful(s)
        case TextMessage.Streamed(s) =>
          s.runFold("")(_ + _)
        case _: BinaryMessage =>
          throw new Exception("Binary message cannot be handled")
      }.via(debugActorFlow(UUID.randomUUID()))
      .map(TextMessage(_))

  def debugActorFlow(connectionId: UUID) : Flow[String, String, Any] = {
    val sink = Flow[String]
      .map(msg => DebugRefProtocol.SignedMessage(connectionId, msg))
      .to(Sink.actorRef(debugRef, DebugRefProtocol.CloseConnection(connectionId)))

    val source = Source.actorRef(16, OverflowStrategy.fail)
      .mapMaterializedValue {
        actor : ActorRef => {
          debugRef ! DebugRefProtocol.OpenConnection(actor, connectionId)
        }
      }

    Flow.fromSinkAndSource(sink, source)
  }

}
