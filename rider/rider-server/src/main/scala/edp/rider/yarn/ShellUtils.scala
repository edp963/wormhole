package edp.rider.yarn

import java.io.{BufferedWriter, File, FileWriter}

import edp.rider.common.RiderLogger

import scala.collection.JavaConversions._
import scala.sys.process.Process

object ShellUtils extends RiderLogger {

  def runShellCommand(cmd: String, logPath: String): (Boolean, Option[String])  = {
    val processBuilder = new ProcessBuilder(List("/bin/sh", "-c", cmd.replaceAll("\r", "")))
    val logFile = new File(logPath)
    try {
      if (!logFile.exists()) {
        logFile.getParentFile.mkdirs()
        logFile.createNewFile()
      }
      processBuilder.redirectError(logFile)
      val process = processBuilder.start()

      val f = process.getClass.getDeclaredField("pid")
      f.setAccessible(true)
      val pid = Some(f.get(process).toString)
      riderLogger.info(s"shell command is $cmd, pid is $pid")
      try {
        if (process.exitValue() == 0) {
          (true, pid)
        } else {
          (false, pid)
        }
      } catch {
        case _: Exception => (true, pid)
      }

    } catch {
      case ex: Exception =>
        val error = s"start command ${processBuilder.command().mkString(" ")} execute failed, ${ex.getMessage}"
        riderLogger.error(error)
        val fw = new FileWriter(logFile.getAbsoluteFile)
        val bw = new BufferedWriter(fw)
        bw.write(error)
        bw.close()
        fw.close()
        (false, None)
    }
  }

  def runDebugShellCommand(cmd: String, logPath: String): (Boolean, Option[String])  = {
    var command: List[String] = List()
    val os = System.getProperty("os.name").toLowerCase
    if (os.startsWith("windows")) {
      command = cmd.replaceAll("\\\"", "\\\\\"").split(" ").toList
    } else {
      command = cmd.split(" ").toList
    }
    val processBuilder = new ProcessBuilder(command)
    val logFile = new File(logPath)
    try {
      if (!logFile.exists()) {
        logFile.getParentFile.mkdirs()
        logFile.createNewFile()
      }

      processBuilder.redirectErrorStream(true)
      processBuilder.redirectOutput(logFile)
      val process = processBuilder.start()

      ProcessKeeper.push(logPath, process)

      riderLogger.info(s"shell command is $cmd")

      (true, None)
    } catch {
      case ex: Exception =>
        val error = s"start command ${processBuilder.command().mkString(" ")} execute failed, ${ex.getMessage}"
        riderLogger.error(error)
        val fw = new FileWriter(logFile.getAbsoluteFile)
        val bw = new BufferedWriter(fw)
        bw.write(error)
        bw.close()
        fw.close()
        (false, None)
    }
  }
}
