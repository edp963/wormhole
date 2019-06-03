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

import edp.rider.common.{RiderLogger, StreamStatus}

import scala.language.postfixOps
import scala.sys.process._

object YarnClientLog extends RiderLogger {

  def getLogByAppName(appName: String, logPath: String) = {
    assert(appName != "" || appName != null, "Refresh log, name couldn't be null or blank.")
    if (new File(logPath).exists) {
      val command = s"tail -500 $logPath"
      try {
        command !!
      } catch {
        case runTimeEx: java.lang.RuntimeException =>
          riderLogger.warn(s"Refresh $appName client log command failed", runTimeEx.getMessage)
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

  def getAppStatusByLog(appName: String, curStatus: String, logPath: String, curAppId: String): (String, String) = {
    assert(appName != "" && appName != null, "Refresh Spark Application log, app name couldn't be null or blank.")
    val appIdPattern = "Application report for application_([0-9]){13}_([0-9]){4}".r

    try {
      val fileLines = getLogByAppName(appName, logPath).split("\\n")
      val appIdList = appIdPattern.findAllIn(fileLines.mkString("\\n")).toList
      val appId = if (appIdList.nonEmpty) appIdList.last.stripPrefix("Application report for").trim else curAppId
      val hasException = fileLines.count(s => s.toLowerCase contains "exception in thread")
      val noFileExists = fileLines.count(s => s.toLowerCase contains "no such file")
      val lostClass=fileLines.count(s => s.toLowerCase contains "could not find")
//      val isRunning = fileLines.count(s => s contains s"(state: $RUNNING)")
//      val isAccepted = fileLines.count(s => s contains s"(state: $ACCEPTED)")
//      val isFinished = fileLines.count(s => s contains s"((state: $FINISHED))")

      val status =
        if (noFileExists > 0 ||hasException > 0 ||lostClass > 0) StreamStatus.FAILED.toString
        else curStatus
      (appId, status)
    }
    catch {
      case ex: Exception =>
        riderLogger.warn(s"Refresh Spark Application status from client log failed", ex)
        ("", curStatus)
    }
  }

  def getFlinkAppStatusByLog(appName: String, curStatus: String, logPath: String, curAppId: String) : (String,String)={
    assert(appName != "" && appName != null, "Refresh Flink Application log, app name couldn't be null or blank.")
    val appIdPattern = "Submitted application application_([0-9]){13}_([0-9]){4}".r
    try {
      val fileLines = getLogByAppName(appName, logPath).split("\\n")
      val appIdList = appIdPattern.findAllIn(fileLines.mkString("\\n")).toList
      val appId = if (appIdList.nonEmpty) appIdList.last.stripPrefix("Submitted application").trim else curAppId
      val isFailed = fileLines.count(s => s.toLowerCase contains s"the flink yarn cluster has failed")
      val isSubmitFailed=fileLines.count(s => s.toLowerCase contains s"the program finished with the following exception")
      val noFileExists = fileLines.count(s => s.toLowerCase contains "no such file")
      val lostClass=fileLines.count(s => s.toLowerCase contains "could not find")
//      val isRunning = fileLines.count(s => s contains s"Flink JobManager is now running on")
//      val isAccepted = fileLines.count(s => s contains s"YARN application has been deployed successfully")
//      val isFinished = if(appId == "") 0 else fileLines.count(s => s contains s"Application $appId finished")

      val status =
        if (noFileExists > 0 ||isFailed > 0 || isSubmitFailed > 0 || lostClass > 0) StreamStatus.FAILED.toString
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
