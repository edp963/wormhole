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

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.common.{AppResult, RiderConfig, RiderLogger, StreamType}
import edp.rider.rest.persistence.entities.{Directive, FlinkDefaultConfig, FlinkResourceConfig, Flow, FlowDirective, LaunchConfig, SourceSchema, StartConfig, Stream}
import edp.rider.rest.util.CommonUtils.{currentMicroSec, currentSec, minTimeOut}
import edp.rider.rest.util.StreamUtils.getLogPath

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.sys.process.{Process, _}
import edp.wormhole.common.util.JsonUtils._
import edp.rider.wormhole.{BatchFlowConfig, KafkaInputBaseConfig, KafkaOutputConfig, SparkConfig}
import spray.json.JsonParser
import edp.rider.RiderStarter.modules._
import edp.rider.rest.util.FlowUtils._
import edp.rider.spark.SparkJobClientLog.riderLogger
import edp.rider.wormhole._
import edp.wormhole.common.util.CommonUtils.base64byte2s
import edp.wormhole.common.util.DateUtils.{dt2date, dt2string}
import edp.wormhole.common.util.DtFormat
import edp.wormhole.ums.UmsProtocolType.DIRECTIVE_FLOW_START

import scalaj.http.{Http, HttpResponse}

object SubmitSparkJob extends App with RiderLogger {

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
          case "default" => s"  --class edp.wormhole.batchflow.BatchflowStarter  "
          case "hdfslog" => s"  --class edp.wormhole.hdfslog.HdfsLogStarter  "
          case "routing" => s"  --class edp.wormhole.router.RouterStarter  "
          case "job" => s"  --class edp.wormhole.batchjob.BatchJobStarter  "
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
    val logPath = getLogPath(stream.name)
    val resourceConfig = json2caseClass[FlinkResourceConfig](stream.startConfig)
    s"""
       |${RiderConfig.flink.homePath}/bin/yarn-session.sh
       |-n ${resourceConfig.taskManagersNumber}
       |-tm ${resourceConfig.perTaskManagerMemoryGB * 1024}
       |-s ${resourceConfig.perTaskManagerSlots}
       |-jm ${resourceConfig.jobManagerMemoryGB * 1024}
       |-qu ${RiderConfig.flink.yarnQueueName}
       |-nm ${stream.name}
       |-d
       |> $logPath 2>&1
     """.stripMargin.replaceAll("\n", " ").trim
  }

  def startFlinkFlow(stream: Stream, flow: Flow, flowDirectiveOpt: FlowDirective) = {
        val commandSh = SubmitSparkJob.generateFlinkFlowStartSh(stream, flow, flowDirectiveOpt)
        riderLogger.info(s"start flow ${flow.id} command: $commandSh")
        runShellCommand(commandSh)

  }

  def generateFlinkFlowStartSh(stream: Stream, flow: Flow, flowDirectiveOpt: FlowDirective): String = {
    val logPath = getLogPath(stream.name)
    val address = getAddressOnYarn(stream.sparkAppid.get).split("\\:").head
    riderLogger.info(address)
    val port = getPortOnYarn(stream.sparkAppid.get)
    val config1 = getWhConfig(stream, flowDirectiveOpt, flow)
    val config2 = getUms(flow, stream)
    s"""
       |${RiderConfig.flink.homePath}/bin/flink
       |-m ${address}:${port} wormhole-ums_1.3-flinkx_1.4.2-0.4.1-SNAPSHOTS-jar-with-dependencies.jar ${config1} ${config2}
       |-d
       |> $logPath 2>&1
     """.stripMargin.replaceAll("\n", " ").trim
  }


  def getAddressOnYarn(sparkAppid: String): String = {
    var address: String = null
    val url =s"http://hdp2:8088/ws/v1/cluster/apps/${sparkAppid}"
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      riderLogger.info(response.toString)
      val json = JsonParser.apply(response.body).toString()
       val app = JSON.parseObject(json).getString("app")
      address = JSON.parseObject(app).getString("amHostHttpAddress")
    } catch {
      case e: Exception =>
        riderLogger.error(s"Flink Application refresh yarn rest url $url failed", e)
    }
    riderLogger.info(address)
    address
  }


  def getPortOnYarn(sparkAppid: String): String = {
    var port :String = null
    val url =s"http://hdp2:8088/proxy/${sparkAppid}/jobmanager/config"
    try {
      val response: HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
      val json = JsonParser.apply(response.body).toString()
      port= JSON.parseObject(json).getString("jobmanager.rpc.port")
    } catch {
      case e: Exception =>
        riderLogger.error(s"Flink Application refresh yarn rest url $url failed", e)
    }
    port
  }


  def getWhConfig(stream: Stream, flowDirectiveOpt: FlowDirective, flow: Flow) = {
    val kafkaUrl = getKafkaByStreamId(stream.id)
    val baseConfig = KafkaBaseConfig(flow.sourceNs+"-"+flow.sinkNs, kafkaUrl,RiderConfig.spark.kafkaSessionTimeOut, RiderConfig.spark.kafkaGroupMaxSessionTimeOut)
    val outputConfig = KafkaOutputConfig(RiderConfig.consumer.feedbackTopic, RiderConfig.consumer.brokers)
//    var autoRegisteredpoc = Seq[KafkaFlinkTopic](null, null)
//    var userDefinedpoc = Seq[KafkaFlinkTopic](null, null)
    val flinkTopic =flowDirectiveOpt.topicInfo match {
      case Some(topic) =>
//    val autoRegisteredOffset = flowDirectiveOpt.topicInfo.get.autoRegisteredTopics
//    val userDefinedOffset = flowDirectiveOpt.topicInfo.get.userDefinedTopics
        val autoRegisteredOffset = topic.autoRegisteredTopics
           val userDefinedOffset = topic.userDefinedTopics
    val autoRegisteredpoc = autoRegisteredOffset.map(offset => KafkaFlinkTopic(offset.name, offset.partitionOffsets.split(",").map(tp => {
      val tpo = tp.split(":")
      PartitionOffsetConfig(tpo(0).toInt, tpo(1).toLong)
    })))
   val userDefinedpoc = userDefinedOffset.map(offset => KafkaFlinkTopic(offset.name, offset.partitionOffsets.split(",").map(tp => {
      val tpo = tp.split(":")
      PartitionOffsetConfig(tpo(0).toInt, tpo(1).toLong)
    })))
       autoRegisteredpoc ++ userDefinedpoc
      case  None => Seq[KafkaFlinkTopic](null, null)++Seq[KafkaFlinkTopic](null, null)
    }

    //val flinkTopic = autoRegisteredpoc ++ userDefinedpoc

    val config = whConfig(kafka_input(baseConfig, flinkTopic), outputConfig, "", 1, RiderConfig.zk)
    caseClass2json[whConfig](config)

  }
  def getKafkaByStreamId(id: Long): String = {
    val kakfaId = Await.result(streamDal.findById(id), minTimeOut).get.instanceId
    Await.result(instanceDal.findById(kakfaId), minTimeOut).get.connUrl
  }

  def getUms(flow: Flow, stream: Stream) ={
    val consumedProtocolSet = getConsumptionType(flow.consumedProtocol)
    val sinkConfigSet = getSinkConfig(flow.sinkNs, flow.sinkConfig.get)
    val tranConfigFinal = getTranConfig(flow.tranConfig.getOrElse(""))

    val sourceNsObj = modules.namespaceDal.getNamespaceByNs(flow.sourceNs).get
    val umsInfoOpt =
      if (sourceNsObj.sourceSchema.nonEmpty)
        json2caseClass[Option[SourceSchema]](modules.namespaceDal.getNamespaceByNs(flow.sourceNs).get.sourceSchema.get)
      else None
    val umsType = umsInfoOpt match {
      case Some(umsInfo) => umsInfo.umsType.getOrElse("ums")
      case None => "ums"
    }
    val umsSchema = umsInfoOpt match {
      case Some(umsInfo) => umsInfo.umsSchema match {
        case Some(schema) => caseClass2json[Object](schema)
        case None => ""
      }
      case None => ""
    }

    val base64Tuple = Seq(stream.id, flow.id, currentMicroSec, umsType, base64byte2s(umsSchema.toString.trim.getBytes), flow.sinkNs, base64byte2s(consumedProtocolSet.trim.getBytes),
      base64byte2s(sinkConfigSet.trim.getBytes), base64byte2s(tranConfigFinal.trim.getBytes))
    val directive = Await.result(modules.directiveDal.insert(Directive(0, DIRECTIVE_FLOW_START.toString, stream.id, flow.id, "", RiderConfig.zk, currentSec, flow.updateBy)), minTimeOut)
    //        riderLogger.info(s"user ${directive.createBy} insert ${DIRECTIVE_FLOW_START.toString} success.")
    val flow_start_ums =
      s"""
         |{
         |"protocol": {
         |"type": "${
        DIRECTIVE_FLOW_START.toString
      }"
         |},
         |"schema": {
         |"namespace": "${flow.sourceNs}",
         |"fields": [
         |{
         |"name": "directive_id",
         |"type": "long",
         |"nullable": false
         |},
         |{
         |"name": "stream_id",
         |"type": "long",
         |"nullable": false
         |},
         |{
         |"name": "job_id",
         |"type": "long",
         |"nullable": false
         |},
         |{
         |"name": "ums_ts_",
         |"type": "datetime",
         |"nullable": false
         |},
         |{
         |"name": "data_type",
         |"type": "string",
         |"nullable": false
         |},
         |{
         |"name": "data_parse",
         |"type": "string",
         |"nullable": true
         |},
         |{
         |"name": "sink_namespace",
         |"type": "string",
         |"nullable": false
         |},
         |{
         |"name": "consumption_protocol",
         |"type": "string",
         |"nullable": false
         |},
         |{
         |"name": "sinks",
         |"type": "string",
         |"nullable": false
         |},
         |{
         |"name": "swifts",
         |"type": "string",
         |"nullable": true
         |}
         |]
         |},
         |"payload": [
         |{
         |"tuple": [${
        directive.id
      }, ${
        base64Tuple.head
      }, "${
        base64Tuple(1)
      }", "${
        base64Tuple(2)
      }", "${
        base64Tuple(3)
      }", "${
        base64Tuple(4)
      }", "${
        base64Tuple(5)
      }", "${
        base64Tuple(6)
      }", "${
        base64Tuple(7)
      }", "${
        base64Tuple(8)
      }"]
         |}
         |]
         |}
        """.stripMargin.replaceAll("\n", "")
  }
}
