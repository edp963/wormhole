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
import edp.rider.rest.util.StreamProcessLogger
import edp.rider.rest.util.StreamUtils.getLogPath
import edp.wormhole.util.JsonUtils

import scala.language.postfixOps
import scala.sys.process.{Process, _}

object SubmitYarnJob extends App with RiderLogger {

  def runShellCommandBlock(command: String) = {
    val commandRe = Process(command).!
    riderLogger.info(s"run shell command is: $command, result is $commandRe")
    commandRe
  }

  def runYarnKillCommand(command: String): Boolean = {
    try {
      var killSuccess = runYarnKillCommandOnce(command)
      //      if (!killSuccess) {
      //        if (RiderConfig.kerberos.enabled) {
      //          val killCommand = s"kinit -kt ${RiderConfig.kerberos.sparkKeyTab} ${RiderConfig.kerberos.sparkPrincipal}"
      //          runShellCommandBlock(killCommand)
      //        }
      //        killSuccess = runYarnKillCommandOnce(command)
      //      }
      riderLogger.info(s"yarn kill command is: $command, result is $killSuccess")
      killSuccess
    } catch {
      case ex: Exception => {
        riderLogger.error(s"run shell command $command failed,", ex)
        false
      }
    }
  }

  def runYarnKillCommandOnce(command: String): Boolean = {
    val process = Runtime.getRuntime.exec(command)
    val killSuccess =
    //      if (RiderConfig.kerberos.enabled)
      new StreamProcessLogger(process.getErrorStream).parseKillErrorStream()
    //    else true
    riderLogger.info(s"run shell command is: $command, result is $killSuccess")
    if (!killSuccess) {
      try {
        process.destroy()
      } catch {
        case ex: Exception => {
          riderLogger.warn(s"kill yarn app process destroy failed, $ex")
        }
      }
    }
    killSuccess
  }

  //  def commandGetJobInfo(streamName: String) = {
  ////    val logPath = logRootPath + streamName
  //    val command = s"vim ${getLogPath(streamName)}"
  ////    val remoteCommand = "ssh -p%s %s@%s %s ".format(sshPort, username, hostname, command)
  //    riderLogger.info(s"refresh stream $streamName log command: $command")
  //    assert(!command.trim.isEmpty, s"refresh stream $streamName log command can't be empty")
  //    Process(command).run()
  //  }


  def generateSparkStreamStartSh(args: String, streamName: String, logPath: String, startConfig: StartConfig, jvmDriverConfig: String, jvmExecutorConfig: String, othersConfig: String, functionType: String, local: Boolean = false): String = {
    val submitPre = RiderConfig.spark.sparkHome
    val executorsNum = startConfig.executorNums
    val driverMemory = startConfig.driverMemory
    val executorMemory = startConfig.perExecutorMemory
    val executorCores = startConfig.perExecutorCores
    val realJarPath =
      if (RiderConfig.spark.kafka08StreamNames.nonEmpty && RiderConfig.spark.kafka08StreamNames.contains(streamName))
        RiderConfig.spark.kafka08JarPath
      else RiderConfig.spark.jarPath

    //    val confList: Seq[String] = {
    //      val conf = new ListBuffer[String]
    //      val relativePath = RiderConfig.kerberos.jaasYarnConfig.contains("/") match {
    //        case true => RiderConfig.kerberos.jaasYarnConfig.substring(RiderConfig.kerberos.jaasYarnConfig.lastIndexOf('/') + 1)
    //        case _ => RiderConfig.kerberos.jaasYarnConfig
    //      }
    //
    //      val krb5Param = s" -Djava.security.auth.login.config=./${relativePath}"
    //
    //      if (RiderConfig.kerberos.enabled) {
    //        if (jvmDriverConfig != "")
    //          conf ++= Array(jvmDriverConfig.concat(krb5Param))
    //        else
    //          conf ++= Array(s"spark.driver.extraJavaOptions=$krb5Param")
    //
    //        if (jvmExecutorConfig != "")
    //          conf ++= Array(jvmExecutorConfig + krb5Param)
    //        else
    //          conf ++= Array(s"spark.executor.extraJavaOptions=$krb5Param")
    //
    //        conf ++= Array("spark.hadoop.fs.hdfs.impl.disable.cache=true")
    //      } else {
    //        if (jvmDriverConfig != "") conf ++= Array(jvmDriverConfig)
    //        if (jvmExecutorConfig != "") conf ++= Array(jvmExecutorConfig)
    //      }
    //
    //      if (othersConfig != "") conf ++= othersConfig.split(",")
    //      conf ++= Array(s"spark.yarn.tags=${RiderConfig.spark.appTags}")
    //      if (RiderConfig.spark.metricsConfPath != "") {
    //        conf += s"spark.metrics.conf=metrics.properties"
    //        conf += s"spark.metrics.namespace=$streamName"
    //      }
    //      conf
    //    }

    val (finalDriverJvmConf, finalExecutorJvmConf) =
      if (RiderConfig.kerberos.kafkaEnabled) {
        val javaAuthJvmConf = s"-Djava.security.auth.login.config=./${RiderConfig.kerberos.sparkJavaAuthConf.split("/").last}"
        val javaKrb5JvmConf = s"-Djava.security.krb5.conf=./${RiderConfig.kerberos.javaKrb5Conf.split("/").last}"
        (jvmDriverConfig + " " + javaAuthJvmConf + " " + javaKrb5JvmConf,
          jvmExecutorConfig + " " + javaAuthJvmConf + " " + javaKrb5JvmConf)
      } else {
        (jvmDriverConfig, jvmExecutorConfig)
      }

    val confBuffer = othersConfig.split(",").toBuffer
    confBuffer.append(finalDriverJvmConf)
    confBuffer.append(finalExecutorJvmConf)
    confBuffer.append("spark.yarn.maxAppAttempts=4")
    confBuffer.append("spark.yarn.am.attemptFailuresValidityInterval=3h")
    confBuffer.append("spark.yarn.submit.waitAppCompletion=false")

    val files =
      if (RiderConfig.spark.metricsConfPath != "")
        s"${RiderConfig.spark.sparkLog4jPath},${RiderConfig.spark.metricsConfPath}"
      else RiderConfig.spark.sparkLog4jPath

    val startShell =
      if (local)
        RiderConfig.spark.startShell.split("\\n").filterNot(line => line.contains("master") || line.contains("deploy-mode"))
      else
        RiderConfig.spark.startShell.split("\\n")

    val startCommand = startShell.map(l => {
      if (l.startsWith("--num-exe")) s" --num-executors " + executorsNum + " "
      else if (l.startsWith("--driver-mem")) s" --driver-memory " + driverMemory + s"g "
      else if (l.startsWith("--files")) {
        if (RiderConfig.kerberos.kafkaEnabled)
          s" --files " + files + "," + RiderConfig.kerberos.sparkJavaAuthConf + "," +
            RiderConfig.kerberos.javaKrb5Conf + "," +
            RiderConfig.kerberos.keytab + s" "
        else
          s" --files " + files + s" "
      } else if (l.startsWith("--queue")) s" --queue " + RiderConfig.spark.queueName + s" "
      else if (l.startsWith("--executor-mem")) s"  --executor-memory " + executorMemory + s"g "
      else if (l.startsWith("--executor-cores")) s"  --executor-cores " + executorCores + s" "
      else if (l.startsWith("--name")) s"  --name " + streamName + " "
      else if (l.startsWith("--conf")) {
        confBuffer.toList.map(conf => " --conf \"" + conf + "\" ").mkString("")
      }
      else if (l.startsWith("--class")) {
        functionType match {
          case "default" => s"  --class edp.wormhole.sparkx.batchflow.BatchflowStarter  "
          case "hdfslog" => s"  --class edp.wormhole.sparkx.hdfs.hdfslog.HdfsLogStarter  "
          case "hdfscsv" => s"  --class edp.wormhole.sparkx.hdfs.hdfscsv.HdfsCsvStarter  "
          case "routing" => s"  --class edp.wormhole.sparkx.router.RouterStarter  "
          case "job" => s"  --class edp.wormhole.sparkx.batchjob.BatchJobStarter  "
        }
      }
      else l
    }).mkString("").stripMargin.replace("\\", "  ") +
      realJarPath + " " + args + " > " + logPath + " 2>&1 "

    //    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //    println("final:" + submitPre + "/bin/spark-submit " + startCommand + realJarPath + " " + args + " 1> " + logPath + " 2>&1")
    //    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //    if (RiderConfig.kerberos.enabled)
    //      submitPre + s"/bin/spark-submit --principal ${RiderConfig.kerberos.sparkPrincipal} --keytab  ${RiderConfig.kerberos.sparkKeyTab} " + startCommand
    //    else
    submitPre + "/bin/spark-submit " + startCommand
  }

  //ssh -p22 user@host ./bin/yarn-session.sh -n 2 -tm 1024 -s 4 -jm 1024 -nm flinktest
  def generateFlinkStreamStartSh(stream: Stream): String = {
    val resourceConfig = JsonUtils.json2caseClass[FlinkResourceConfig](stream.startConfig)
    val logPath = getLogPath(stream.name)
    s"""
       |${RiderConfig.flink.homePath}/bin/yarn-session.sh
       |-d
       |-n ${resourceConfig.taskManagersNumber}
       |-tm ${resourceConfig.perTaskManagerMemoryGB * 1024}
       |-s ${resourceConfig.perTaskManagerSlots}
       |-jm ${resourceConfig.jobManagerMemoryGB * 1024}
       |-qu ${RiderConfig.flink.yarnQueueName}
       |-nm ${stream.name}
       |> $logPath 2>&1
     """.stripMargin.replaceAll("\n", " ").trim
  }


  def killPidCommand(pidOrg: Option[String], name: String) = {
    try {
      pidOrg match {
        case Some(pid) =>
          if (pid != null && pid.trim.nonEmpty) {
            ("ps -ef" #| s"grep $pid" #| "grep -v grep" #| Seq("awk", "{print $2}") #| "xargs kill -9").run()
            riderLogger.info(s"the stream [$name] submit cilent is killed, need to yarn to view log")
          }
        case None =>
      }
    } catch {
      case ex: Exception =>
        riderLogger.warn(s"kill pid $pidOrg failed")
    }
  }

}
