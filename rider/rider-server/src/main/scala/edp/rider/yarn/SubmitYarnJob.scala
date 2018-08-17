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

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.entities.{FlinkResourceConfig, StartConfig, Stream}
import edp.rider.rest.util.StreamUtils.getLogPath
import edp.wormhole.util.JsonUtils

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.sys.process.{Process, _}

object SubmitYarnJob extends App with RiderLogger {

  //  def startSparkSubmit(args: String, streamID: Long, brokers: String, streamName: String, startConfig: StartConfig, launchConfig: LaunchConfig, sparkConfig: String, streamType: String): Unit = {
  //    val args = getConfig(streamID, streamName, brokers, launchConfig)
  //    val commandSh = generateStreamStartSh(args, streamName, startConfig, sparkConfig, streamType)
  //    runShellCommand(commandSh)
  //  }
  //
  //  def stopApp(appID: String): Unit = {
  //    val cmdStr = s"yarn application -kill $appID"
  //    riderLogger.info(s"stop spark-streaming command: $cmdStr")
  //    runShellCommand(cmdStr)
  //  }


  def runShellCommand(command: String) = {
    //    val remoteCommand = "ssh -p%s %s@%s %s ".format(sshPort, username, hostname, command)
    assert(!command.trim.isEmpty, "start or stop spark application command can't be empty")
    val array = command.split(";")
    if (array.length == 2) {
      val process1 = Future(Process(array(0)).run())
      process1.map(p => {
        if (p.exitValue() != 0) {
          val msg = array(1).split("\\|")
          Future(msg(0).trim #| msg(1).trim !)
        }
      })
    }
    else Process(command).run()
  }

  //  def commandGetJobInfo(streamName: String) = {
  ////    val logPath = logRootPath + streamName
  //    val command = s"vim ${getLogPath(streamName)}"
  ////    val remoteCommand = "ssh -p%s %s@%s %s ".format(sshPort, username, hostname, command)
  //    riderLogger.info(s"refresh stream $streamName log command: $command")
  //    assert(!command.trim.isEmpty, s"refresh stream $streamName log command can't be empty")
  //    Process(command).run()
  //  }


  def generateSparkStreamStartSh(args: String, streamName: String, logPath: String, startConfig: StartConfig, sparkConfig: String, functionType: String, local: Boolean = false): String = {
    val submitPre = s"ssh -p${RiderConfig.spark.sshPort} ${RiderConfig.spark.user}@${RiderConfig.riderServer.host} " + RiderConfig.spark.sparkHome
    val executorsNum = startConfig.executorNums
    val driverMemory = startConfig.driverMemory
    val executorMemory = startConfig.perExecutorMemory
    val executorCores = startConfig.perExecutorCores
    val realJarPath =
      if (RiderConfig.spark.kafka08StreamNames.nonEmpty && RiderConfig.spark.kafka08StreamNames.contains(streamName))
        RiderConfig.spark.kafka08JarPath
      else RiderConfig.spark.jarPath

    val confList: Seq[String] = {
      val conf = new ListBuffer[String]
      if (sparkConfig != "") {
        val riderConf = sparkConfig.split(",") :+ s"spark.yarn.tags=${RiderConfig.spark.appTags}"
        conf ++= riderConf
      }
      else conf ++= Array(s"spark.yarn.tags=${RiderConfig.spark.appTags}")
      if (RiderConfig.spark.metricsConfPath != "") {
        conf += s"spark.metrics.conf=metrics.properties"
        conf += s"spark.metrics.namespace=$streamName"
      }
      conf
    }

    val files =
      if (RiderConfig.spark.metricsConfPath != "")
        s"${RiderConfig.spark.sparkLog4jPath},${RiderConfig.spark.metricsConfPath}"
      else RiderConfig.spark.sparkLog4jPath

    runShellCommand(s"mkdir -p ${RiderConfig.spark.clientLogRootPath}")
    val startShell =
      if (local)
        RiderConfig.spark.startShell.split("\\n").filterNot(line => line.contains("master") || line.contains("deploy-mode"))
      else
        RiderConfig.spark.startShell.split("\\n")

    //    val startShell = Source.fromFile(s"${RiderConfig.riderConfPath}/bin/startStream.sh").getLines()
    val startCommand = startShell.map(l => {
      if (l.startsWith("--num-exe")) s" --num-executors " + executorsNum + " "
      else if (l.startsWith("--driver-mem")) s" --driver-memory " + driverMemory + s"g "
      else if (l.startsWith("--files")) s" --files " + files + s" "
      else if (l.startsWith("--queue")) s" --queue " + RiderConfig.spark.queueName + s" "
      else if (l.startsWith("--executor-mem")) s"  --executor-memory " + executorMemory + s"g "
      else if (l.startsWith("--executor-cores")) s"  --executor-cores " + executorCores + s" "
      else if (l.startsWith("--name")) s"  --name " + streamName + " "
//      else if (l.startsWith("--jars")) s"  --jars " + RiderConfig.spark.sparkxInterfaceJarPath + " "
      else if (l.startsWith("--conf")) {
        confList.toList.map(conf => " --conf \"" + conf + "\" ").mkString("")
      }
      else if (l.startsWith("--class")) {
        functionType match {
          case "default" => s"  --class edp.wormhole.sparkx.batchflow.BatchflowStarter  "
          case "hdfslog" => s"  --class edp.wormhole.sparkx.hdfslog.HdfsLogStarter  "
          case "routing" => s"  --class edp.wormhole.sparkx.router.RouterStarter  "
          case "job" => s"  --class edp.wormhole.sparkx.batchjob.BatchJobStarter  "
        }
      }
      else l
    }).mkString("").stripMargin.replace("\\", "  ") +
//      realJarPath + " " + args + " 1> " + logPath + " 2>&1"
      realJarPath + " " + args + " > " + logPath + " 2>&1 "

    val finalCommand =
      if (RiderConfig.spark.alert)
        s"$startCommand;echo '$streamName is dead' | mail -s 'ERROR-$streamName-is-dead' ${RiderConfig.spark.alertEmails}"
      else startCommand
    //    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //    println("final:" + submitPre + "/bin/spark-submit " + startCommand + realJarPath + " " + args + " 1> " + logPath + " 2>&1")
    //    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    submitPre + "/bin/spark-submit " + finalCommand
  }

  // ./bin/yarn-session.sh -n 2 -tm 1024 -s 4 -jm 1024 -nm flinktest

  def generateFlinkStreamStartSh(stream: Stream): String = {
    val resourceConfig = JsonUtils.json2caseClass[FlinkResourceConfig](stream.startConfig)
    val logPath = getLogPath(stream.name)
    s"""
       |${RiderConfig.flink.homePath}/bin/yarn-session.sh
       |-n ${resourceConfig.taskManagersNumber}
       |-tm ${resourceConfig.perTaskManagerMemoryGB * 1024}
       |-s ${resourceConfig.perTaskManagerSlots}
       |-jm ${resourceConfig.jobManagerMemoryGB * 1024}
       |-qu ${RiderConfig.flink.yarnQueueName}
       |-nm ${stream.name}
       |> $logPath 2>&1
     """.stripMargin.replaceAll("\n", " ").trim
  }

}
