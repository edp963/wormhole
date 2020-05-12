/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole.flinkx.eventflow

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.util.{Properties, TimeZone}

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.common.json.FieldInfo
import edp.wormhole.flinkx.common.ExceptionProcessMethod.ExceptionProcessMethod
import edp.wormhole.flinkx.common.{ExceptionConfig, _}
import edp.wormhole.flinkx.deserialization.WormholeDeserializationStringSchema
import edp.wormhole.flinkx.sink.SinkProcess
import edp.wormhole.flinkx.swifts.{FlinkxTimeCharacteristicConstants, ParseSwiftsSql, SwiftsProcess}
import edp.wormhole.flinkx.udaf.{AdjacentSub, FirstValue, LastValue}
import edp.wormhole.flinkx.udf.{UdafRegister, UdfRegister, WhMapToString}
import edp.wormhole.flinkx.util.FlinkSchemaUtils._
import edp.wormhole.flinkx.util.{FlinkxTimestampExtractor, UmsFlowStartUtils, WormholeFlinkxConfigUtils}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import edp.wormhole.util.DateUtils
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.types.Row
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map


class WormholeFlinkMainProcess(config: WormholeFlinkxConfig, umsFlowStart: Ums) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)
  private val flowStartFields = umsFlowStart.schema.fields_get
  private val flowStartPayload = umsFlowStart.payload_get.head
  private val swiftsString: String = UmsFlowStartUtils.extractSwifts(flowStartFields, flowStartPayload)
  logger.info(swiftsString + "------swifts string")
  private val swifts: fastjson.JSONObject = JSON.parseObject(swiftsString)
  private val timeCharacteristic: String = UmsFlowStartUtils.extractTimeCharacteristic(swifts)
  private val sinkNamespace = UmsFlowStartUtils.extractSinkNamespace(flowStartFields, flowStartPayload)
  private val sourceNamespace: String = UmsFlowStartUtils.extractSourceNamespace(umsFlowStart)
  private val streamId = UmsFlowStartUtils.extractStreamId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head).toLong
  //  private val directiveId = UmsFlowStartUtils.extractDirectiveId(umsFlowStart.schema.fields_get, umsFlowStart.payload_get.head).toLong
  private val flowId = UmsFlowStartUtils.extractFlowId(flowStartFields, flowStartPayload)
  val swiftsSpecialConfig: JSONObject = UmsFlowStartUtils.extractSwiftsSpecialConfig(swifts)

  private val exceptionProcessMethod: ExceptionProcessMethod = ExceptionProcessMethod.exceptionProcessMethod(UmsFlowStartUtils.extractExceptionProcess(swiftsSpecialConfig))
  private val latenessSeconds: Int = UmsFlowStartUtils.latenessSecondsGet(swiftsSpecialConfig)
  private val exceptionConfig = ExceptionConfig(streamId, flowId, sourceNamespace, sinkNamespace, exceptionProcessMethod)

  private val kafkaDataTag = OutputTag[String]("kafkaDataException")

  def process(): JobExecutionResult = {

    val initialTs = System.currentTimeMillis
    val swiftsSql = getSwiftsSql(swiftsString, UmsFlowStartUtils.extractDataType(flowStartFields, flowStartPayload))
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val flowConfigString = UmsFlowStartUtils.extractConfig(flowStartFields, flowStartPayload)
    val flowConfig = JSON.parseObject(flowConfigString)
    val parallelism = UmsFlowStartUtils.extractParallelism(flowConfig)
    env.setParallelism(parallelism)
    manageCheckpoint(env, UmsFlowStartUtils.extractCheckpointConfig(config.commonConfig, flowConfig))
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    tableEnv.config.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    udfRegister(tableEnv)
    assignTimeCharacteristic(env)

    val inputStream: DataStream[Row] = createKafkaStream(env, umsFlowStart.schema.namespace.toLowerCase, initialTs)
    val watermarkStream = assignTimestamp(inputStream, immutableSourceSchemaMap)

    val swiftsTs = System.currentTimeMillis
    val (stream, schemaMap) = new SwiftsProcess(watermarkStream, exceptionConfig, tableEnv, swiftsSql, swiftsSpecialConfig, timeCharacteristic, config).process()
    SinkProcess.doProcess(stream, umsFlowStart, schemaMap, config, initialTs, swiftsTs, exceptionConfig)
    //      WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority1, feedbackDirective(DateUtils.currentDateTime, directiveId, UmsFeedbackStatus.SUCCESS, streamId, ""), Some(UmsProtocolType.FEEDBACK_DIRECTIVE+"."+streamId), config.kafka_output.brokers)
    env.execute(config.flow_name)
  }


  private def udfRegister(tableEnv: StreamTableEnvironment): Unit = {

    tableEnv.registerFunction(BuiltInFunctions.ADJACENTSUB.toString, new AdjacentSub())
    tableEnv.registerFunction(BuiltInFunctions.FIRSTVALUE.toString, new FirstValue())
    tableEnv.registerFunction(BuiltInFunctions.LASTVALUE.toString, new LastValue())
    tableEnv.registerFunction(BuiltInFunctions.MAPTOSTRING.toString, new WhMapToString())

    config.udf_config.foreach(udf => {
      val udfName = udf.functionName
      val udfClassFullName = udf.fullClassName
      val mapOrAgg = udf.mapOrAgg
      try {
        mapOrAgg match {
          case "udaf" =>
            UdafRegister.register(udfName, udfClassFullName, tableEnv)
          case "udf" =>
            UdfRegister.register(udfName, udfClassFullName, tableEnv)
          case _ =>
            UdfRegister.register(udfName, udfClassFullName, tableEnv)
        }

      } catch {
        case e: Throwable =>
          logger.error(mapOrAgg + ":" + udfName + " register fail", e)
      }
    })
  }

  private def createKafkaStream(env: StreamExecutionEnvironment, flowNamespace: String, initialTs: Long): DataStream[Row] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.kafka_input.kafka_base_config.brokers)
    properties.setProperty("zookeeper.connect", config.zookeeper_address)
    properties.setProperty("group.id", config.kafka_input.groupId)
    properties.setProperty("session.timeout.ms", config.kafka_input.sessionTimeout)
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", 5000.toString)
    //config.kafka_input.kafka_base_config.`max.partition.fetch.bytes`.toString
    properties.setProperty("max.partition.fetch.bytes", 10485760.toString)
    if (config.kafka_input.kafka_base_config.kerberos) {
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.kerberos.service.name", "kafka")
    }
    val flinkxConfigUtils = new WormholeFlinkxConfigUtils(config)
    val topics = flinkxConfigUtils.getKafkaTopicList
    val myConsumer = new FlinkKafkaConsumer010[(String, String, String, Int, Long)](topics, new WormholeDeserializationStringSchema, properties)

    val specificStartOffsets = flinkxConfigUtils.getTopicPartitionOffsetMap
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
    val consumeProtocolMap = UmsFlowStartUtils.extractConsumeProtocol(flowStartFields, flowStartPayload)
    val initialStream: DataStream[(String, String, String, Int, Long)] = env.addSource(myConsumer)
      .map(event => (UmsCommonUtils.checkAndGetKey(event._1, event._2), event._2, event._3, event._4, event._5))

    val processStream: DataStream[(String, String, String, Int, Long)] =
      initialStream.filter(event => {
        val (umsProtocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(event._1)
        //        if (checkOtherData(umsProtocolType.toString) && matchNamespace(namespace, flowNamespace))
        //          doOtherData(event._2)
        consumeProtocolMap.contains(umsProtocolType) && consumeProtocolMap(umsProtocolType) && matchNamespace(namespace, flowNamespace)
      })
    val jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])] = ConfMemoryStorage.getAllSourceParseMap

    val inputStream = processStream.process(new UmsProcessElement(sourceSchemaMap.toMap, config, exceptionConfig, jsonSourceParseMap, kafkaDataTag, assignMetricConfig))(Types.ROW(sourceFieldNameArray, sourceFlinkTypeArray))

    val exceptionStream = inputStream.getSideOutput(kafkaDataTag)
    exceptionStream.map(new ExceptionProcess(exceptionProcessMethod, config, exceptionConfig))
    inputStream
  }

  private def manageCheckpoint(env: StreamExecutionEnvironment, flinkCheckpoint: FlinkCheckpoint): Unit = {
    if (flinkCheckpoint.isEnable) {
      env.setStateBackend(new FsStateBackend(flinkCheckpoint.stateBackend).asInstanceOf[StateBackend])
      env.enableCheckpointing(flinkCheckpoint `checkpointInterval.ms`)
      val checkpointConfig = env.getCheckpointConfig
      checkpointConfig.setMinPauseBetweenCheckpoints(500)
      checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    } else env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)))
  }

  private def assignMetricConfig(): Configuration = {
    val mConfig = new Configuration()
    mConfig.setString("metrics.reporters", "feedbackState")
    mConfig.setString("metrics.reporter.feedbackState.class", "edp.wormhole.reporter.FeedbackMetricsReporter")
    mConfig.setString("metrics.reporter.feedbackState.interval", config.feedback_interval + " SECONDS")
    mConfig.setString("metrics.reporter.feedbackState.scope.delimiter", ".")
    mConfig.setString("metrics.reporter.feedbackState.sourceNamespace", sourceNamespace)
    mConfig.setString("metrics.reporter.feedbackState.sinkNamespace", sinkNamespace)
    mConfig.setString("metrics.reporter.feedbackState.streamId", streamId.toString)
    mConfig.setString("metrics.reporter.feedbackState.flowId", flowId.toString)
    mConfig.setString("metrics.reporter.feedbackState.topic", config.kafka_output.feedback_topic_name)
    mConfig.setString("metrics.reporter.feedbackState.kerberos", config.kafka_output.kerberos.toString)
    mConfig.setString("metrics.reporter.feedbackState.brokers", config.kafka_output.brokers)
    mConfig.setInteger("metrics.reporter.feedbackState.feedbackCount", config.feedback_state_count)
    mConfig
  }

  private def assignTimeCharacteristic(env: StreamExecutionEnvironment): Unit = {
    if (timeCharacteristic == FlinkxTimeCharacteristicConstants.PROCESSING_TIME)
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    else
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }

  private def assignTimestamp(inputStream: DataStream[Row], sourceSchemaMap: Map[String, (TypeInformation[_], Int)]) = {
    if (timeCharacteristic == FlinkxTimeCharacteristicConstants.PROCESSING_TIME) {
      inputStream
    }
    else if (latenessSeconds <= 0) {
      inputStream.assignTimestampsAndWatermarks(new FlinkxTimestampExtractor(sourceSchemaMap))
    } else {
      inputStream.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Row](Time.seconds(latenessSeconds)) {
          override def extractTimestamp(element: Row): Long = {
            val umsTs = element.getField(sourceSchemaMap(UmsSysField.TS.toString)._2)
            logger.debug(s"latenessSeconds is $latenessSeconds, umsTs in assignTimestamp $umsTs")
            val umsTsInLong = DateUtils.dt2long(umsTs.asInstanceOf[Timestamp])
            logger.debug("umsTsInLong " + umsTsInLong)
            umsTsInLong
          }
        }
      )
    }
  }

  private def getSwiftsSql(swiftsString: String, dataType: String): Option[Array[SwiftsSql]] = {
    val action: String = if (swifts.containsKey("action") && swifts.getString("action").trim.nonEmpty) new String(new sun.misc.BASE64Decoder().decodeBuffer(swifts.getString("action").trim)) else null
    if (null != action) {
      logger.info(s"action in getSwiftsSql $action")
      val parser = new ParseSwiftsSql(action, sourceNamespace, sinkNamespace)
      parser.registerConnections(swifts)
      parser.parse(dataType, immutableSourceSchemaMap.keySet)
    } else None
  }

  private def doOtherData(row: String): Unit = {
    WormholeKafkaProducer.initWithoutAcksAll(config.kafka_output.brokers, config.kafka_output.config, config.kafka_output.kerberos)
    val ums = UmsCommonUtils.json2Ums(row)
    if (ums.payload_get.nonEmpty) {
      try {
        val umsTsIndex = ums.schema.fields.get.zipWithIndex.filter(_._1.name == UmsSysField.TS.toString).head._2
        val namespace = ums.schema.namespace
        val umsts = ums.payload_get.head.tuple(umsTsIndex)
        ums.protocol.`type` match {
          case UmsProtocolType.DATA_BATCH_TERMINATION =>
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              WormholeUms.feedbackDataBatchTermination(namespace, umsts, streamId), Some(UmsProtocolType.FEEDBACK_DATA_BATCH_TERMINATION + "." + streamId), config.kafka_output.brokers)
          case UmsProtocolType.DATA_INCREMENT_TERMINATION =>
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              WormholeUms.feedbackDataIncrementTermination(namespace, umsts, streamId), Some(UmsProtocolType.FEEDBACK_DATA_INCREMENT_TERMINATION + "." + streamId), config.kafka_output.brokers)
          case UmsProtocolType.DATA_INCREMENT_HEARTBEAT =>
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.feedbackPriority,
              WormholeUms.feedbackDataIncrementHeartbeat(namespace, umsts, streamId), Some(UmsProtocolType.FEEDBACK_DATA_INCREMENT_HEARTBEAT + "." + streamId), config.kafka_output.brokers)
          case _ => logger.warn(ums.protocol.`type`.toString + " is not supported")
        }
      } catch {
        case e: Throwable => logger.error("in doOtherData ", e)
      }
    }
  }
}
