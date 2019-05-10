package edp.rider.yarn

import java.io.File

import edp.rider.common.RiderLogger

import scala.collection.JavaConversions._

object ShellUtils extends RiderLogger {

  def runShellCommand(cmd: String, logPath: String): Boolean = {
    val processBuilder = new ProcessBuilder(List("/bin/sh", "-c", cmd))
    val logFile = new File(logPath)
    try {
      if (!logFile.exists()) {
        logFile.mkdirs()
        logFile.createNewFile()
      }
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
        riderLogger.error(ex.getMessage)
        false
    }
  }
}
