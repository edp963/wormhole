package edp.rider.rest.router

import java.util.UUID

import akka.actor.{Actor, ActorRef, PoisonPill}
import edp.rider.common.{RiderConfig, WsRequest}
import edp.rider.yarn.ProcessKeeper
import edp.wormhole.util.JsonUtils

import scala.io.Source

/**
  * @author Suxy
  * @date 2020/6/12
  * @description file description
  */
class DebugRef extends Actor {

  private val maximumClients = 20
  private val lastLineNum = 2

  var clients: scala.collection.mutable.Map[UUID, ActorRef] = scala.collection.mutable.Map[UUID, ActorRef]()
  var reads: scala.collection.mutable.Map[UUID, String] = scala.collection.mutable.Map[UUID, String]()

  override def receive: Receive = withClients(clients)

  def withClients(clients: scala.collection.mutable.Map[UUID, ActorRef]): Receive = {
    case DebugRefProtocol.SignedMessage(_, msg) => clients.collect {
      case (id, _) =>
        val reqModel = JsonUtils.json2caseClass[WsRequest](msg)
        if (reqModel.action == "read") {
          reads += (id -> (reqModel.logPath.get + "$0"))
          ProcessKeeper.access(reqModel.logPath.get)
        }
    }
    case DebugRefProtocol.OpenConnection(ar, _) if clients.size == maximumClients =>
      ar ! PoisonPill
    case DebugRefProtocol.OpenConnection(ar, uuid) =>
      context.become(withClients(clients += (uuid -> ar)))
    case DebugRefProtocol.CloseConnection(uuid) =>
      /**
        * flink 直接关闭进程
        */
      if (reads(uuid).contains(s"${RiderConfig.flink.clientLogPath}/debug/")) {
        ProcessKeeper.shutdown(reads(uuid).split("\\$")(0))
      }
      reads -= uuid
      context.become(withClients(clients -= uuid))
  }

  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        try {
          if (reads.nonEmpty) {
            for (read <- reads) {
              val path = read._2.split("\\$")(0)
              val lineNum = read._2.split("\\$")(1).toInt
              val buf = new StringBuilder
              val lines = Source.fromFile(path, "UTF-8").getLines().toList
              var startPos = 0
              if (lines.length > lineNum) {
                if (lineNum > 0) {
                  startPos = lineNum
                } else if (lines.length > lastLineNum) {
                  startPos = lines.length - lastLineNum
                }
                var index = 0
                for (line <- lines) {
                  if (index >= startPos) {
                    buf ++= line
                    buf ++= "\n"
                  }
                  index += 1
                }
                clients(read._1) ! buf.toString
                reads(read._1) = path + "$" + index
              }
            }
            Thread.sleep(500)
          } else {
            Thread.sleep(3000)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
  }).start()

}
