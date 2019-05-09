package edp.rider.yarn

import java.io.File

import edp.rider.common.RiderLogger

import scala.collection.JavaConversions._

object ShellUtils extends RiderLogger with App {

  def runShellCommand(cmd: String, logPath: String): Boolean = {
    val processBuilder = new ProcessBuilder(List("/bin/sh", "-c", cmd))
    val logFile = new File(logPath)
    try {
      if (!logFile.exists())
        logFile.createNewFile()
      processBuilder.redirectError(logFile)
      val process = processBuilder.start()
      try {
        if (process.exitValue() == 0) {
          true
        } else {
          false
        }
      } catch {
        case _: Exception => true
      }

    } catch {
      case ex: Exception =>
        println(ex.getMessage)
        false
    }
  }

//  val result = runShellCommand("ll /", "/Users/swallow/Desktop/shell-test/1.log")
//  println(result)
}
