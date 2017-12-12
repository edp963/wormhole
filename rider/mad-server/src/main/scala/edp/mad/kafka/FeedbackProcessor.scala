package edp.mad.kafka



import akka.kafka.ConsumerMessage.CommittableMessage
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch.MadES._
import edp.mad.elasticsearch._
import edp.mad.module._
import edp.mad.util._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.{DtFormat, _}
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.{Ums, UmsFieldType, _}
import org.apache.log4j.Logger
import edp.mad.cache._

import scala.concurrent.Future

object FeedbackProcessor{
  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

    def getProtocolFromKey(key: String): UmsProtocolType = {
      val protocolTypeStr: String = key.substring(0, key.indexOf(".") - 1)
      UmsProtocolType.umsProtocolType(protocolTypeStr)
    }

    private def json2Ums(json: String): Ums = {
      try {
        UmsSchemaUtils.toUms(json)
      } catch {
        case e: Throwable =>
          logger.error(s"feedback $json convert to case class failed", e)
          Ums(UmsProtocol(UmsProtocolType.FEEDBACK_DIRECTIVE), UmsSchema("defaultNamespace"))
      }
    }


  def processMessage (msg: CommittableMessage[Array[Byte], String] ): Future[CommittableMessage[Array[Byte], String]] = {
    if (msg.record.key() != null)
      logger.info(s"Consumed key: ${msg.record.key().toString}")

    //val curTs = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    logger.info(s"Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    //println(s"Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    modules.offsetMap.set(OffsetMapkey(modules.feedbackConsumerStreamId, msg.record.topic, msg.record.partition), OffsetMapValue( msg.record.offset))

    if (msg.record.value() == null || msg.record.value() == "") {
      logger.error(s"feedback message value is null: ${msg.toString}")
    } else {
      try {
        val ums: Ums = json2Ums(msg.record.value())
        logger.debug(s"Consumed protocol: ${ums.protocol.`type`.toString}")
        ums.protocol.`type` match {
          case FEEDBACK_FLOW_ERROR =>
             doFeedbackFlowError(ums)
          case FEEDBACK_FLOW_STATS =>
              doFeedbackFlowStats(ums)
          case FEEDBACK_STREAM_BATCH_ERROR =>
              doFeedbackStreamBatchError(ums)
          case FEEDBACK_STREAM_TOPIC_OFFSET =>
            doFeedbackStreamTopicOffset(ums)
          case _ => logger.info(s"don't proccess the protocol ${ums.protocol.`type`.toString}")
        }
      } catch {
        case e: Exception =>
          logger.error(s"parse protocol error key: ${msg.record.key()} value: ${msg.record.value()}", e)
      }
    }

    Future.successful (msg)

  }


  def doFeedbackFlowError(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val curTs = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    logger.debug(s"start process protocol: ${protocolType}")
    try {
      message.payload_get.foreach(tuple => {
        logger.debug(s"$tuple")
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val topicNameValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name")
        val partitionOffsetValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets")
        if(umsTsValue != null && streamIdValue != null && topicNameValue != null && partitionOffsetValue != null) {
          /*
          val partitionOffset = partitionOffsetValue.toString
          val partitionNum: Int = FeedbackOffsetUtil.getPartitionNumber(partitionOffset)
          val future = modules.feedbackOffsetDal.insert(FeedbackOffset(1, protocolType.toString, umsTsValue.toString, streamIdValue.toString.toLong,
            topicNameValue.toString, partitionNum, partitionOffset, curTs))
          val result = Await.ready(future, Duration.Inf).value.get
          result match {
            case Failure(e) =>
              riderLogger.error(s"FeedbackStreamTopicOffset inserted ${tuple.toString} failed", e)
            case Success(t) => riderLogger.debug("FeedbackStreamTopicOffset inserted success.")
          }
          */
        }else { logger.error(s"FeedbackStreamTopicOffset can't found the value")}

      })
    } catch {
      case e: Exception =>
        logger.error(s"Failed to process FeedbackStreamTopicOffset feedback message ${message}", e)
    }
  }

  def namespaceRiderString(ns: String): String = {
    val array = ns.split("\\.")
    List(array(0), array(1), array(2), array(3), "*", "*", "*").mkString(".")
  }

  def doFeedbackFlowStats(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val srcNamespace = message.schema.namespace.toLowerCase
    val riderNamespace = namespaceRiderString(srcNamespace)
    val fields = message.schema.fields_get
    var throughput: Long = 0
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    logger.debug(s"start process FeedbackFlowStats feedback ${message.payload_get}")
    try {
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val statsIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stats_id")
        val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace")
        val rddCountValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_count")
        val dataOriginalTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_genereated_ts")
        val rddTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "rdd_generated_ts")
        val directiveTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "directive_process_start_ts")
        val mainProcessTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "data_process_start_ts")
        val swiftsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "swifts_start_ts")
        val sinkTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_start_ts")
        val doneTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "done_ts")
        logger.debug(s"\n")
        if(umsTsValue != null && streamIdValue != null && statsIdValue != null && sinkNamespaceValue != null && rddCountValue != null && dataOriginalTsValue != null && rddTsValue  != null &&
          directiveTsValue != null && mainProcessTsValue != null && swiftsTsValue != null && sinkTsValue != null && doneTsValue != null) {
          logger.debug(s"\n")
          val umsTs = umsTsValue.toString
          val streamId = streamIdValue.toString.toLong
          val statsId = statsIdValue.toString
          val sinkNamespace = sinkNamespaceValue.toString
          val rddCount = rddCountValue.toString.toInt
          val dataOriginalTs = dataOriginalTsValue.toString.toLong
          val rddTs = rddTsValue.toString.toLong
          val directiveTs = directiveTsValue.toString.toLong
          val mainProcessTs = mainProcessTsValue.toString.toLong
          val swiftsTs = swiftsTsValue.toString.toLong
          val sinkTs = sinkTsValue.toString.toLong
          val doneTs = doneTsValue.toString.toLong
          logger.debug(s"\n")
          val riderSinkNamespace = if (sinkNamespaceValue.toString == "") riderNamespace else namespaceRiderString(sinkNamespaceValue.toString)
          val sourceNs = riderNamespace.split("\\.")
          val sinkNs = riderSinkNamespace.split("\\.")
          val flowName = s"${riderNamespace}_${riderSinkNamespace}"
          logger.debug(s"\n")
          val intervalMainProcessToDataOriginal = (mainProcessTsValue.toString.toLong - dataOriginalTsValue.toString.toLong) / 1000
          val intervalMainProcessToDone = (mainProcessTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000
          val intervalMainProcessToSwifts = (mainProcessTsValue.toString.toLong - swiftsTsValue.toString.toLong) / 1000
          val intervalMainProcessToSink = (mainProcessTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000
          val intervalSwiftsToSink = (swiftsTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000
          val intervalSinkToDone = (sinkTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000
          val intervalRddToDone = (doneTsValue.toString.toLong - mainProcessTsValue.toString.toLong)/1000
          logger.debug(s"\n")
          if (intervalRddToDone == 0L) {
            throughput = rddCountValue.toString.toInt
          } else throughput = rddCountValue.toString.toInt /intervalRddToDone

          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          logger.debug(s"  streamMapV ${streamMapV} \n")
          if(  null != streamMapV ){
            val infos = streamMapV.get
            modules.streamFeedbackMap.updateHitCount(streamId, 1,  DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))
            logger.debug(s" infos: ${infos}")
            val flowList = infos.listCacheFlowInfo
            logger.debug(s" flowList: ${flowList}")
            val flowInfos = flowList.filter(cacheFlowInfo => cacheFlowInfo.flowNamespace == flowName)
            logger.debug(s" flowInfos: ${flowInfos}")
            if(flowInfos != null && flowInfos != List()) {
              val flowInfo = flowInfos.head
              logger.debug(s" flowInfo: ${flowInfo}")
              val topicV = modules.namespaceMap.get(NamespaceMapkey(riderNamespace))
              val topicName = if ( null != topicV )  topicV.get.topicName else ""
              val esMadFlows = EsMadFlows(
                DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
                DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC),
                      infos.cacheProjectInfo.id,
                      infos.cacheProjectInfo.name,
                      // stream
                      infos.cacheStreamInfo.id,
                      infos.cacheStreamInfo.name,
                      infos.cacheStreamInfo.appId,
                      infos.cacheStreamInfo.status,
                      DateUtils.dt2string(DateUtils.dt2dateTime( infos.cacheStreamInfo.startedTime) ,DtFormat.TS_DASH_SEC),
                      infos.cacheStreamInfo.consumerDuration,
                      infos.cacheStreamInfo.consumerMaxRecords,
                      infos.cacheStreamInfo.processRepartition,
                      infos.cacheStreamInfo.driverCores,
                      infos.cacheStreamInfo.driverMemory,
                      infos.cacheStreamInfo.perExecuterCores,
                      infos.cacheStreamInfo.perExecuterMemory,
                      infos.cacheStreamInfo.kafkaConnection,
              // Flow 相关配置和静态信息
                      topicName,
                      0,
                      flowName,
                      riderNamespace,
                      sourceNs(0),
                      sourceNs(1),
                      sourceNs(2),
                      sourceNs(3),
                      riderSinkNamespace,
                      sinkNs(0),
                      sinkNs(1),
                      sinkNs(2),
                      sinkNs(3),
                      flowInfo.flowStatus,
                      DateUtils.dt2string(DateUtils.dt2dateTime(flowInfo.flowStartedTime), DtFormat.TS_DASH_SEC) ,
                      DateUtils.dt2string(DateUtils.dt2dateTime(flowInfo.updateTime), DtFormat.TS_DASH_SEC),
                      flowInfo.consumedProtocol,
                      flowInfo.sinkSpecificConfig, //每种sink都不一样，无法拆分出有效字段
                      flowInfo.tranConfig,
                      flowInfo.tranActionCustomClass,
                      flowInfo.transPushdownNamespaces,
                      // Flow反馈的错误信息
                      DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
                      DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
                      0,
                      "",
                      // Flow反馈的统计信息
                      statsId,
                      rddCount,
                      throughput,
                      DateUtils.dt2string(dataOriginalTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(rddTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(directiveTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(mainProcessTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(swiftsTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(sinkTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      DateUtils.dt2string(doneTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC),
                      intervalMainProcessToDataOriginal,
                      intervalMainProcessToDone,
                      intervalMainProcessToSwifts,
                      intervalMainProcessToSink,
                      intervalSwiftsToSink,
                      intervalSinkToDone,
                      intervalRddToDone)
              logger.debug(s" EsMadFlows: ${esMadFlows}")
              val postBody: String = JsonUtils.caseClass2json(esMadFlows)
              val rc =   madES.insertEs(postBody,INDEXFLOWS.toString)
              logger.debug(s" EsMadFlows: response ${rc}")
              //    asyncToES(postBody, url, HttpMethods.POST)
            // ElasticSearch.insertFlowStatToES(monitorInfo)
            }else{ logger.error(s" can't found the flow ${flowName} from Stream Map \n")}
          }else{
            modules.streamFeedbackMap.updateMissedCount(streamId, 1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))
            logger.info(s" can't found the stream id in streamMap ${streamIdValue} \n ")}
        }else {logger.error(s"Failed to get value from FeedbackFlowStats ${tuple}")}
      })
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to parse FeedbackFlowStats feedback message",ex)
    }

  }

  def doFeedbackStreamBatchError(message: Ums) = {
    logger.info("start process")
  }

  def doFeedbackStreamTopicOffset(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    logger.debug(s"start process StreamTopicOffset feedback ${message.payload_get}")
    try {
      message.payload_get.foreach(tuple => {
        logger.debug(s"\n")
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val topicNameValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name")
        val partitionOffsetValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets")
        logger.debug(s"\n")
        if (umsTsValue != null && streamIdValue != null && topicNameValue != null && partitionOffsetValue != null) {
          val umsTs = umsTsValue.toString
          val streamId = streamIdValue.toString.toLong
          val topicName = topicNameValue.toString
          val partitionOffsets = partitionOffsetValue.toString
          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          if (streamMapV != null) {
            modules.streamFeedbackMap.updateHitCount(streamId, 1,  DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))
            logger.debug(s" streamMapV: ${streamMapV}")
            val infos = streamMapV.get
            val projectList = infos.cacheProjectInfo
            val streamList = infos.cacheStreamInfo
            val partitionNum: Int = OffsetUtils.getPartitionNumber(partitionOffsets)
            val topicList = streamList.topicList
            logger.debug(s" topicList: ${topicList}")
            val topicInfos = topicList.filter(CacheTopicInfo => CacheTopicInfo.topicName == topicName)
            logger.debug(s" topicInfos: ${topicInfos}")

            if (topicInfos != null && topicInfos != List()) {
              val topicInfo = topicInfos.head
              var pid = 0
              while (pid < partitionNum) {
                var consumeredOffset = -1L
                var latestOffset = -1L
                  consumeredOffset = OffsetUtils.getOffsetFromPartitionOffsets(partitionOffsets, pid)
                  latestOffset = OffsetUtils.getOffsetFromPartitionOffsets(topicInfo.latestPartitionOffsets, pid)
                  val esMadStream = EsMadStreams(
                    DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
                    projectList.id,
                    projectList.name,
                    projectList.resourceCores,
                    projectList.resourceMemory,
                    DateUtils.dt2string(DateUtils.dt2dateTime(projectList.createdTime), DtFormat.TS_DASH_SEC),
                    DateUtils.dt2string(DateUtils.dt2dateTime(projectList.updatedTime), DtFormat.TS_DASH_SEC),
                    // Stream 相关配置和静态信息
                    streamId,
                    streamList.name,
                    streamList.appId,
                    streamList.status,
                    DateUtils.dt2string(DateUtils.dt2dateTime(streamList.startedTime), DtFormat.TS_DASH_SEC),
                    streamList.sparkConfig,
                    streamList.consumerDuration,
                    streamList.consumerMaxRecords,
                    streamList.processRepartition,
                    streamList.driverCores,
                    streamList.driverMemory,
                    streamList.perExecuterCores,
                    streamList.perExecuterMemory,
                    streamList.kafkaConnection,
                    DateUtils.dt2string(DateUtils.dt2dateTime(umsTs), DtFormat.TS_DASH_SEC),
                    "",
                    topicName,
                    partitionNum,
                    pid,
                    latestOffset,
                    consumeredOffset
                  )
                try{
                  logger.info(s" EsMadStreams: ${esMadStream}")
                  val postBody: String = JsonUtils.caseClass2json(esMadStream)
                  val rc =   madES.insertEs(postBody,INDEXSTREAMS.toString)
                  logger.info(s" EsMadStreams: response ${rc}")
                } catch {
                  case e: Exception =>
                    logger.error(s"Failed to insert mad streams to ES", e)
                }
                pid += 1
              }
            }
          }else{
            modules.streamFeedbackMap.updateMissedCount(streamId, 1,  DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))
            logger.info(s" can't found the streamId in stream Map ${streamId}" )
          }
        }
      })
    } catch {
      case e: Exception =>
        logger.error(s"Failed to process FeedbackFlowStats ${message} \n ",e)
    }
  }
}

