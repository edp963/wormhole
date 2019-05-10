package edp.rider.yarn

import java.io.{BufferedWriter, File, FileWriter}

import edp.rider.common.RiderLogger

import scala.collection.JavaConversions._

object ShellUtils extends RiderLogger {

  def runShellCommand(cmd: String, logPath: String): Boolean = {
    val processBuilder = new ProcessBuilder(List("/bin/sh", "-c", cmd))
    val logFile = new File(logPath)
    try {
      if (!logFile.exists()) {
        logFile.getParentFile.mkdirs()
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
        val error = s"start command ${processBuilder.command().mkString(" ")} execute failed, ${ex.getMessage}"
        riderLogger.error(error)
        val fw = new FileWriter(logFile.getAbsoluteFile())
        val bw = new BufferedWriter(fw)
        bw.write(error)
        bw.close()
        fw.close()
        false
    }
  }
}
