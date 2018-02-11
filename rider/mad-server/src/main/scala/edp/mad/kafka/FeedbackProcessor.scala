package edp.mad.kafka

import akka.kafka.ConsumerMessage.CommittableMessage
import com.alibaba.fastjson.JSONObject
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch.MadES._
import edp.mad.module._
import edp.mad.util._
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.{DtFormat, _}
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.ums.{Ums, UmsFieldType, _}
import org.apache.log4j.Logger
import edp.mad.cache._

import scala.collection.mutable.ListBuffer
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
    if (msg.record.key() != null) logger.info(s"Consumed key: ${msg.record.key().toString}")

    modules.offsetMap.set(OffsetMapkey(modules.feedbackConsumerStreamId, msg.record.topic, msg.record.partition), OffsetMapValue( msg.record.offset))

    if (msg.record.value() == null || msg.record.value() == "") {
      logger.error(s"feedback message value is null: ${msg.toString}")
    } else {
      try {
        val ums: Ums = json2Ums(msg.record.value())
        logger.info(s"ConsumedProtocol: ${ums.protocol.`type`.toString} [topic,partition,offset]  [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] ")
        ums.protocol.`type` match {
          case FEEDBACK_FLOW_ERROR =>
             doFeedbackFlowError(ums)
          case FEEDBACK_FLOW_STATS =>
              doFeedbackFlowStats(ums)
          case FEEDBACK_STREAM_BATCH_ERROR =>
              doFeedbackStreamBatchError(ums)
          case FEEDBACK_STREAM_TOPIC_OFFSET =>
            doFeedbackStreamTopicOffset(ums)
          //case feedback_data_increment_heartbeat =>
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
    val srcNamespace = message.schema.namespace.toLowerCase
    val riderNamespace = namespaceRiderString(srcNamespace)
    val fields = message.schema.fields_get
    logger.info(s"start process feedback_flow_error ${protocolType} feedback ${message.payload_get}")
    val esSchemaMap = madES.getSchemaMap(INDEXFLOWERROR.toString)
    try{
      val esBulkList = new ListBuffer[String]
      val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
      message.payload_get.foreach(tuple => {
        logger.debug(s"$tuple")
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val sinkNamespaceValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "sink_namespace")

        if(umsTsValue != null && streamIdValue != null && sinkNamespaceValue != null) {
          val streamId = streamIdValue.toString.toLong
          val riderSinkNamespace = if (sinkNamespaceValue.toString == "") riderNamespace else namespaceRiderString(sinkNamespaceValue.toString)
          val flowName = s"${riderNamespace}_${riderSinkNamespace}"
          var flowId : Long = 0
          var streamName = ""

          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          if(  null != streamMapV && streamMapV != None) {
            streamName = streamMapV.get.cacheStreamInfo.name
            val flowInfos = streamMapV.get.listCacheFlowInfo.filter(cacheFlowInfo => cacheFlowInfo.flowNamespace == flowName)
            if ( flowInfos != null && flowInfos != List() ){ flowId = flowInfos.head.id }
          }
          logger.info(s" get streamName: ${streamName}, flowId: ${flowId}  from stream map ${streamMapV} by steam id ${streamId} \n")

          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
            e._1 match{
              case  "madProcessTime" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case   "feedbackTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTsValue.toString) ,DtFormat.TS_DASH_SEC))
              case   "streamId" => flattenJson.put( e._1, streamId )
              case   "streamName" => flattenJson.put( e._1, streamName )
              case   "flowId" => flattenJson.put( e._1, flowId )
              case   "flowNamespace" => flattenJson.put( e._1, flowName )
              case   "flowErrorMaxWaterMarkTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_max_watermark_ts").toString) ,DtFormat.TS_DASH_SEC))
              case   "flowErrorMinWaterMarkTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_min_watermark_ts").toString) ,DtFormat.TS_DASH_SEC))
              case   "flowErrorCount" => flattenJson.put( e._1, UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_count").toString.toInt)
              case   "flowErrorMessage" => flattenJson.put( e._1, UmsFieldType.umsFieldValue(tuple.tuple, fields, "error_info").toString)
            }
          }
          esBulkList.append(flattenJson.toJSONString)
        }else { logger.error(s"FeedbackStreamTopicOffset can't found the value")}
      })

      if(esBulkList.nonEmpty){ madES.bulkIndex2Es( esBulkList.toList, INDEXFLOWERROR.toString) }else { logger.error(s" bulkIndex empty list \n") }

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
    val esBulkList = new ListBuffer[String]
    val esSchemaMap = madES.getSchemaMap(INDEXFLOWFEEDBACK.toString)
    logger.debug(s"start process FeedbackFlowStats feedback ${protocolType} ${message.payload_get}")
    try {
      val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

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
        val topicV = modules.namespaceMap.get(NamespaceMapkey(riderNamespace))
        val topicName = if(  null != topicV && topicV != None) topicV.get.topicName else ""
        var flowId : Long = 0
        var streamName = ""

        if(umsTsValue != null && streamIdValue != null && statsIdValue != null && sinkNamespaceValue != null && rddCountValue != null && dataOriginalTsValue != null && rddTsValue  != null &&
          directiveTsValue != null && mainProcessTsValue != null && swiftsTsValue != null && sinkTsValue != null && doneTsValue != null) {
          val umsTs = umsTsValue.toString
          val streamId = streamIdValue.toString.toLong
          val riderSinkNamespace = if (sinkNamespaceValue.toString == "") riderNamespace else namespaceRiderString(sinkNamespaceValue.toString)
          val flowName = s"${riderNamespace}_${riderSinkNamespace}"

          modules.streamFeedbackMap.updateHitCount(streamId, 1,  DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))

          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          if(  null != streamMapV && streamMapV != None) {
            streamName = streamMapV.get.cacheStreamInfo.name
            val flowInfos = streamMapV.get.listCacheFlowInfo.filter(cacheFlowInfo => cacheFlowInfo.flowNamespace == flowName)
            if (flowInfos != null && flowInfos != List()) { flowId = flowInfos.head.id }
          }
          logger.debug(s" get streamName:  ${streamName} flowId:${flowId} from stream map by steam id ${streamId} \n")

          val intervalRddToDone = (doneTsValue.toString.toLong - mainProcessTsValue.toString.toLong)/1000

          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
            //logger.info(s" = = = = 0 ${e._1}  ${e._2}")
            e._1 match{
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case  "feedbackTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTsValue.toString) ,DtFormat.TS_DASH_SEC) )
              case  "streamId" => flattenJson.put( e._1, streamId )
              case  "streamName" => flattenJson.put( e._1, streamName )
              case  "flowId" => flattenJson.put( e._1, flowId )
              case  "flowNamespace" => flattenJson.put( e._1, flowName )
              case  "statsId" => flattenJson.put( e._1, statsIdValue.toString )
              case  "rddCount" => flattenJson.put( e._1, rddCountValue.toString.toInt)
              case  "throughput" => flattenJson.put( e._1, if (intervalRddToDone == 0L){ rddCountValue.toString.toInt } else rddCountValue.toString.toInt /intervalRddToDone)
              case  "originalDataTs " => flattenJson.put( e._1, DateUtils.dt2string(dataOriginalTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "rddTransformStartTs" => flattenJson.put( e._1, DateUtils.dt2string(rddTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "directiveProcessStartTs" => flattenJson.put( e._1, DateUtils.dt2string(directiveTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "mainProcessStartTs" => flattenJson.put( e._1, DateUtils.dt2string(mainProcessTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "swiftsProcessStartTs" => flattenJson.put( e._1, DateUtils.dt2string(swiftsTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "sinkWriteStartTs" => flattenJson.put( e._1, DateUtils.dt2string(sinkTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "processDoneTs" => flattenJson.put( e._1, DateUtils.dt2string(doneTsValue.toString.toLong * 1000, DtFormat.TS_DASH_MICROSEC))
              case  "intervalMainProcessToDataOriginalTs" => flattenJson.put( e._1, (mainProcessTsValue.toString.toLong - dataOriginalTsValue.toString.toLong) / 1000)
              case  "intervalMainProcessToDone " => flattenJson.put( e._1, (mainProcessTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000)
              case  "intervalMainProcessToSwifts" => flattenJson.put( e._1, (mainProcessTsValue.toString.toLong - swiftsTsValue.toString.toLong) / 1000)
              case  "intervalMainProcessToSink" => flattenJson.put( e._1, (mainProcessTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000)
              case  "intervalSwiftsToSink" => flattenJson.put( e._1, (swiftsTsValue.toString.toLong - sinkTsValue.toString.toLong) / 1000)
              case  "intervalSinkToDone" => flattenJson.put( e._1, (sinkTsValue.toString.toLong - doneTsValue.toString.toLong) / 1000)
              case  "intervalRddToDone" => flattenJson.put( e._1,  (doneTsValue.toString.toLong - mainProcessTsValue.toString.toLong)/1000)
            }
          }
          esBulkList.append(flattenJson.toJSONString)
        }else{
          modules.streamFeedbackMap.updateMissedCount(streamIdValue.toString.toLong, 1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTsValue.toString) ,DtFormat.TS_DASH_SEC))
            logger.info(s" can't found the stream id in streamMap ${streamIdValue} \n ")
        }
      })

      if(esBulkList.nonEmpty){ madES.bulkIndex2Es( esBulkList.toList, INDEXFLOWFEEDBACK.toString) }else { logger.error(s" bulkIndex empty list \n") }

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to parse FeedbackFlowStats feedback message",ex)
    }
  }

  def doFeedbackStreamBatchError(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    val esSchemaMap = madES.getSchemaMap(INDEXSTREAMERROR.toString)
    logger.debug(s"start process ${protocolType} feedback ${message.payload_get}")
    try {
      val esBulkList = new ListBuffer[String]
      val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
      message.payload_get.foreach(tuple => {
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val statusValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "status")
        val resultValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "result_desc")
        if (umsTsValue != null && streamIdValue != null && statusValue != null && resultValue != null) {
          val umsTs = umsTsValue.toString
          val streamId = streamIdValue.toString.toLong
          var pId: Long = 0
          var pName: String = ""
          var sName: String = ""
          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          if (streamMapV != null && streamMapV != None) {
            val streamList = streamMapV.get.cacheStreamInfo
            pId = streamList.projectId
            pName = streamList.projectName
            sName = streamList.name
          }
          logger.debug(s" streamMapV: ${streamMapV}")

          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
            logger.info(s" = = = = 0 ${e._1}  ${e._2}")
            e._1 match{
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case "feedbackTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTsValue.toString) ,DtFormat.TS_DASH_SEC) )
              case  "projectId" => flattenJson.put( e._1, pId )
              case  "projectName" => flattenJson.put( e._1, pName )
              case  "streamId" => flattenJson.put( e._1, streamId )
              case  "streamName" => flattenJson.put( e._1, sName )
              case  "status" => flattenJson.put( e._1, statusValue.toString )
              case  "errorMessage" => flattenJson.put( e._1, resultValue.toString.replaceAll("\n","\t") )
            }
          }
          esBulkList.append(flattenJson.toJSONString)
        }else{
          logger.info(s" can't found the streamId in stream Map ${streamIdValue}" )
        }
      })

      if(esBulkList.nonEmpty){ madES.bulkIndex2Es( esBulkList.toList, INDEXSTREAMERROR.toString) }else { logger.error(s" bulkIndex empty list \n") }

    } catch {
      case e: Exception =>
        logger.error(s"Failed to process FeedbackFlowStats ${message} \n ",e)
    }
  }

  def doFeedbackStreamTopicOffset(message: Ums) = {
    val protocolType = message.protocol.`type`.toString
    val fields = message.schema.fields_get
    logger.debug(s"start process StreamTopicOffset feedback ${message.payload_get}")
    val esSchemaMap = madES.getSchemaMap(INDEXSTREAMSFEEDBACK.toString)
      try {
      val esBulkList = new ListBuffer[String]
      val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
      message.payload_get.foreach(tuple => {
        logger.debug(s"\n")
        val umsTsValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "ums_ts_")
        val streamIdValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "stream_id")
        val topicNameValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "topic_name")
        val partitionOffsetValue = UmsFieldType.umsFieldValue(tuple.tuple, fields, "partition_offsets")

        if (umsTsValue != null && streamIdValue != null && topicNameValue != null && partitionOffsetValue != null) {
          val umsTs = umsTsValue.toString
          val streamId = streamIdValue.toString.toLong
          val topicName = topicNameValue.toString
          val partitionOffsets = partitionOffsetValue.toString
          val partitionNum: Int = OffsetUtils.getPartitionNumber(partitionOffsets)
          var topicLatestOffsetStr =""
          var pId: Long = 0
          var pName: String = ""
          var sName: String = ""
          val streamMapV = modules.streamMap.get(StreamMapKey(streamId))
          if (streamMapV != null && streamMapV != None) {
            val streamList = streamMapV.get.cacheStreamInfo
            pId = streamList.projectId
            pName = streamList.projectName
            sName = streamList.name

            val topicInfos = streamList.topicList.filter(CacheTopicInfo => CacheTopicInfo.topicName == topicName)
            if (topicInfos != null && topicInfos != List()) { topicLatestOffsetStr = topicInfos.head.latestPartitionOffsets }
           // logger.info(s" topicLatestOffsetStr: ${topicLatestOffsetStr}")
            modules.streamFeedbackMap.updateHitCount(streamId, 1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTs), DtFormat.TS_DASH_SEC))
          }else{
            modules.streamFeedbackMap.updateMissedCount(streamId, 1,  DateUtils.dt2string(DateUtils.dt2dateTime(umsTs) ,DtFormat.TS_DASH_SEC))
          }

          //logger.info(s" streamMapV: ${streamMapV}")
          var pid = 0
          while (pid < partitionNum) {
            val consumeredOffset = OffsetUtils.getOffsetFromPartitionOffsets(partitionOffsets, pid)
            val latestOffset = OffsetUtils.getOffsetFromPartitionOffsets(topicLatestOffsetStr, pid)
           // logger.info(s" consumerStr: ${partitionOffsets} offset:  ${consumeredOffset}   latestSt： ${topicLatestOffsetStr}  offset：${latestOffset}")

            val flattenJson = new JSONObject
            esSchemaMap.foreach{e=>
              e._1 match{
                case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
                case "feedbackTs" => flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(umsTsValue.toString) ,DtFormat.TS_DASH_SEC) )
                case  "projectId" => flattenJson.put( e._1, pId )
                case  "projectName" => flattenJson.put( e._1, pName )
                case  "streamId" => flattenJson.put( e._1, streamId )
                case  "streamName" => flattenJson.put( e._1, sName )
                case "topicName" => flattenJson.put( e._1, topicName)
                case "partitionNum" => flattenJson.put( e._1, partitionNum )
                case "partitionId" => flattenJson.put( e._1, pid )
                case "latestOffset" => flattenJson.put( e._1, latestOffset )
                case "feedbackOffset" => flattenJson.put( e._1, consumeredOffset )
              }
            }
            esBulkList.append(flattenJson.toJSONString)
            pid += 1
          }
        }else{ logger.info(s" can't found the streamId in stream Map ${streamIdValue}" )  }
      })

      if(esBulkList.nonEmpty){ madES.bulkIndex2Es( esBulkList.toList, INDEXSTREAMSFEEDBACK.toString) }else { logger.error(s" bulkIndex empty list \n") }

    } catch {
      case e: Exception =>
        logger.error(s"Failed to process FeedbackFlowStats ${message} \n ",e)
    }
  }
}

