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

package edp.rider.spark

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.entities.{LaunchConfig, StartConfig}

import scala.sys.process.Process

object SubmitSparkJob extends App with RiderLogger {

  def getLogPath(appName: String) = s"${RiderConfig.spark.clientLogRootPath}$appName.log"

  def getConfig(streamId: Long, streamName: String, brokers: String, launchConfig: LaunchConfig) = {

    val maxRecords = launchConfig.maxRecords
    val durations = launchConfig.durations
    val partitions = launchConfig.partitions
    val baseDecoder = new sun.misc.BASE64Decoder
    val kafkaInput = new String(baseDecoder.decodeBuffer("JycneyJrYWZrYV9pbnB1dCI6eyJtYXgucGFydGl0aW9uLmZldGNoLmJ5dGVzIjoxMDQ4NTc2MCwia2V5LmRlc2VyaWFsaXplciI6Im9yZy5hcGFjaGUua2Fma2EuY29tbW9uLnNlcmlhbGl6YXRpb24uU3RyaW5nRGVzZXJpYWxpemVyIiwidmFsdWUuZGVzZXJpYWxpemVyIjoib3JnLmFwYWNoZS5rYWZrYS5jb21tb24uc2VyaWFsaXphdGlvbi5TdHJpbmdEZXNlcmlhbGl6ZXIiLCJzZXNzaW9uLnRpbWVvdXQubXMiOjMwMDAwLCJtYXgucG9sbC5yZWNvcmRzIjo=")) + maxRecords + new String(baseDecoder.decodeBuffer("LCJncm91cF9pZCI6Ig==")) + streamName + new String(baseDecoder.decodeBuffer("IiwiYnJva2VycyI6Ig=="))
    val brokersBase64 = brokers + new String(baseDecoder.decodeBuffer("IiwiYmF0Y2hfZHVyYXRpb25fc2Vjb25kcyI6")) + durations + "},"
    val kafkaOutput = new String(baseDecoder.decodeBuffer("ImthZmthX291dHB1dCI6eyJmZWVkYmFja190b3BpY19uYW1lIjoi")) + RiderConfig.consumer.topic + new String(baseDecoder.decodeBuffer("IiwiYnJva2VycyI6Ig==")) + RiderConfig.consumer.brokers + new String(baseDecoder.decodeBuffer("In0s"))
    val sparkConfig = new String(baseDecoder.decodeBuffer("InNwYXJrX2NvbmZpZyI6eyJzdHJlYW1faWQiOg==")) + streamId.toString + new String(baseDecoder.decodeBuffer("LCJzdHJlYW1fbmFtZSI6Ig==")) + streamName + new String(baseDecoder.decodeBuffer("IiwibWFzdGVyIjoieWFybi1jbHVzdGVyIiwic3Bhcmsuc3FsLnNodWZmbGUucGFydGl0aW9ucyI6")) + partitions + "},"
    val hdfsConfig = new String(baseDecoder.decodeBuffer("InJkZF9wYXJ0aXRpb25fbnVtYmVyIjo=")) + partitions + new String(baseDecoder.decodeBuffer("LCJ6b29rZWVwZXJfcGF0aCI6Ig==")) + RiderConfig.zk + new String(baseDecoder.decodeBuffer("Iiwia2Fma2FfcGVyc2lzdGVuY2VfY29uZmlnX2lzdmFsaWQiOmZhbHNlLCJzdHJlYW1faGRmc19hZGRyZXNzIjoi")) + RiderConfig.spark.hdfs_root + "\"" + "}'''"
    kafkaInput + brokersBase64 + kafkaOutput + sparkConfig + hdfsConfig
  }

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
    Process(command).run()
  }

  //  def commandGetJobInfo(streamName: String) = {
  ////    val logPath = logRootPath + streamName
  //    val command = s"vim ${getLogPath(streamName)}"
  ////    val remoteCommand = "ssh -p%s %s@%s %s ".format(sshPort, username, hostname, command)
  //    riderLogger.info(s"refresh stream $streamName log command: $command")
  //    assert(!command.trim.isEmpty, s"refresh stream $streamName log command can't be empty")
  //    Process(command).run()
  //  }


  def generateStreamStartSh(args: String, streamName: String, startConfig: StartConfig, sparkConfig: String, streamType: String): String = {
    val submitPre = s"ssh -p${RiderConfig.spark.sshPort} ${RiderConfig.spark.user}@${RiderConfig.riderServer.host} " + RiderConfig.spark.spark_home
    val executorsNum = startConfig.executorNums
    val driverMemory = startConfig.driverMemory
    val executorMemory = startConfig.perExecutorMemory
    val executorCores = startConfig.perExecutorCores
    val realJarPath =
      if (RiderConfig.spark.kafka08StreamNames.nonEmpty && RiderConfig.spark.kafka08StreamNames.contains(streamName))
        RiderConfig.spark.kafka08JarPath
      else RiderConfig.spark.jarPath

    val confList: Array[String] = sparkConfig.split(",") :+ s"spark.yarn.tags=${RiderConfig.spark.app_tags}"
    val logPath = getLogPath(streamName)
    val startShell = RiderConfig.spark.startShell.split("\\n")
    //    val startShell = Source.fromFile(s"${RiderConfig.riderConfPath}/bin/startStream.sh").getLines()
    val startCommand = startShell.map(l => {
      if (l.startsWith("--num-exe")) s" --num-executors " + executorsNum + " "
      else if (l.startsWith("--driver-mem")) s" --driver-memory " + driverMemory + s"g "
      else if (l.startsWith("--files")) s" --files " + RiderConfig.spark.sparkLog4jPath + s" "
      else if (l.startsWith("--queue")) s" --queue " + RiderConfig.spark.queue_name + s" "
      else if (l.startsWith("--executor-mem")) s"  --executor-memory " + executorMemory + s"g "
      else if (l.startsWith("--executor-cores")) s"  --executor-cores " + executorCores + s" "
      else if (l.startsWith("--name")) s"  --name " + streamName + " "
      else if (l.startsWith("--conf")) {
        confList.toList.map(conf => " --conf \"" + conf + "\" ").mkString("")
      }
      else if (l.startsWith("--class")) {
        streamType match {
          case "default" => s"  --class edp.wormhole.batchflow.BatchflowStarter  "
          case "hdfslog" => s"  --class edp.wormhole.hdfslog.HdfsLogStarter  "
          case "routing" => s"  --class edp.wormhole.router.RouterStarter  "
          case "job" => s"  --class edp.wormhole.batchjob.BatchJobStarter  "
        }
      }
      else l
    }).mkString("").stripMargin.replace("\\", "  ")
    submitPre + "/bin/spark-submit " + startCommand + realJarPath + " " + args + " 1> " + logPath + " 2>&1"

  }

}
