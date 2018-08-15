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


package edp.rider.yarn

import java.io.File

import edp.rider.common.YarnAppStatus._
import edp.rider.common.{RiderLogger, StreamStatus}
import edp.rider.yarn.SubmitYarnJob._

import scala.language.postfixOps
import scala.sys.process._

object YarnClientLog extends RiderLogger {

  def getLogByAppName(appName: String, logPath: String) = {
    assert(appName != "" || appName != null, "Refresh log, name couldn't be null or blank.")
//    val logPath = getLogPath(appName)
    if (new File(logPath).exists) {
      val command = s"tail -500 $logPath"
      try {
        command !!
      } catch {
        case runTimeEx: java.lang.RuntimeException =>
          riderLogger.warn(s"Refresh $appName client log command failed", runTimeEx)
          if (runTimeEx.getMessage.contains("Nonzero exit value: 1"))
            "The log file doesn't exist."
          else runTimeEx.getMessage
        case ex: Exception => ex.getMessage
      }
    } else {
      riderLogger.warn(s"$appName client log file $logPath doesn't exist.")
      "The log file doesn't exist."
    }

  }

  def getAppStatusByLog(appName: String, curStatus: String, logPath: String): (String, String) = {
    assert(appName != "" && appName != null, "Refresh Spark Application log, app name couldn't be null or blank.")
    val appIdPattern = "Application report for application_([0-9]){13}_([0-9]){4}".r
    try {
      val fileLines = getLogByAppName(appName, logPath).split("\\n")
      val appIdList = appIdPattern.findAllIn(fileLines.mkString("\\n")).toList
      val appId = if (appIdList.nonEmpty) appIdList.last.stripPrefix("Application report for").trim else ""
      val hasException = fileLines.count(s => s contains "Exception")
      val isRunning = fileLines.count(s => s contains s"(state: $RUNNING)")
      val isAccepted = fileLines.count(s => s contains s"(state: $ACCEPTED)")
      val isFinished = fileLines.count(s => s contains s"((state: $FINISHED))")

      val status =
        if (appId == "" && hasException == 0) curStatus
        else if (appId == "" && hasException > 0) StreamStatus.FAILED.toString
        else if (hasException == 0 && isRunning > 0) StreamStatus.RUNNING.toString
        else if (hasException > 0) StreamStatus.FAILED.toString
        else if (isAccepted > 0) StreamStatus.WAITING.toString
        else if (isFinished > 0) StreamStatus.FINISHED.toString
        else curStatus
      (appId, status)
    }
    catch {
      case ex: Exception =>
        riderLogger.warn(s"Refresh Spark Application status from client log failed", ex)
        ("", curStatus)
    }
  }
}
