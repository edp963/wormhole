package edp.rider.yarn

import java.util.{Date, Timer, TimerTask}

/**
  * @author Suxy
  * @date 2020/6/16
  * @description file description
  */
object ProcessKeeper {

  /**
    * 30 min
    */
  private val timeout: Long = 1800000

  private var processes: Map[String, Process] = Map()
  private var accesses: Map[String, Date] = Map()

  def push(logPath: String, process: Process): Unit = {
    processes += (logPath -> process)
    accesses += (logPath -> new Date)
  }

  def access(logPath: String): Unit = {
    accesses += (logPath -> new Date)
  }

  def shutdown(logPath: String): Unit = {
    if (processes.contains(logPath)) {
      processes(logPath).destroyForcibly()
      processes -= logPath
    }
    accesses -= logPath
  }

  new Timer().schedule(new TimerTask() {
    override def run(): Unit = {
      val now = new Date
      var expires: List[String] = List()
      for (elem <- accesses) {
        if ((now.getTime - elem._2.getTime) > timeout) {
          expires = expires :+ elem._1
        }
      }
      if (expires.nonEmpty) {
        expires.foreach(expire => shutdown(expire))
      }
    }
  }, 5000, 10000)

}
