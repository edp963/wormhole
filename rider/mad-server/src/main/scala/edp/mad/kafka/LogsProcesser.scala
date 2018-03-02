package edp.mad.kafka


import akka.kafka.ConsumerMessage.CommittableMessage
import com.alibaba.fastjson.JSONObject
import edp.mad.cache._
import edp.mad.elasticsearch.MadES._
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch._
import edp.mad.module.ModuleObj
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.{DateUtils, DtFormat, JsonUtils}
import org.apache.log4j.Logger
import org.json4s.JsonAST.JNothing

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object LogsProcesser{
  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

  def processMessage (msg: CommittableMessage[Array[Byte], String] ): Future[CommittableMessage[Array[Byte], String]] = {
    logger.info(s"Logs Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    //println(s"Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    if (msg.record.key() != null) logger.info(s"Consumed key: ${msg.record.key().toString}")

    modules.offsetMap.set(OffsetMapkey(modules.logsConsumerStreamId, msg.record.topic, msg.record.partition), OffsetMapValue( msg.record.offset))
    val esSchemaMap = madES.getSchemaMap(INDEXAPPLOGS.toString)
    val esBulkList = new ListBuffer[String]

    if (msg.record.value() == null || msg.record.value() == "") {
      logger.error(s"Logs message value is null: ${msg.toString}")
    } else {
      logger.info(s" Logs message value : ${msg.toString}\n")
       try {
         val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
         val msgJsonStr = msg.record.value()
         val msgObj = JsonUtils.json2jValue(msgJsonStr)
         val appId = JsonUtils.getString(msgObj, s"appId")
         val host = JsonUtils.getString(msgObj, s"host")
         //val logTime = JsonUtils.getString(msgObj, s"ums_ts_")
         val logstashTime = JsonUtils.getString(msgObj, s"@timestamp")
         val loglevel = JsonUtils.getString(msgObj, s"loglevel")
         val path = JsonUtils.getString(msgObj, s"path")
         val className = JsonUtils.getString(msgObj, s"className")
         val container = JsonUtils.getString(msgObj, s"container")
         val message = JsonUtils.getString(msgObj, s"message")
         var streamName = ""
         var projectId = -1L
         var projectName = ""
         var streamId = -1L
         val appMapV = modules.applicationMap.get(ApplicationMapKey(appId))
         logger.info(s" Logs appMapV : ${appMapV}\n")
         if(null != appMapV && appMapV != None){
           streamName = appMapV.get.streamName
           val streamInfo = modules.streamNameMap.get(StreamNameMapKey(streamName))
           logger.info(s" Logs streamInfo : ${streamInfo}\n")
           if(streamInfo != null && streamInfo != None) {
             projectId = streamInfo.get.projectId.toLong
             projectName = streamInfo.get.projectName
             streamId = streamInfo.get.streamId.toLong
            }
         }

         val flattenJson = new JSONObject
         esSchemaMap.foreach{e=>
           e._1 match{
             case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
             case "projectId" =>  flattenJson.put( e._1,projectId)
             case "projectName" =>  flattenJson.put( e._1, projectName)
             case "streamId" =>  flattenJson.put( e._1, streamId)
             case "streamName" =>  flattenJson.put( e._1, streamName)
             case "sparkAppId" =>  flattenJson.put( e._1, appId)
             case "logsOrder" =>  flattenJson.put( e._1, msg.record.offset.toString)
             case "hostName" =>  flattenJson.put( e._1, host )
             case "logTime" =>  flattenJson.put( e._1,logstashTime )
             case "logLevel" =>  flattenJson.put( e._1, loglevel)
             case "logPath" =>  flattenJson.put( e._1, path )
             case "className" =>  flattenJson.put( e._1, className )
             case "message" =>  flattenJson.put( e._1, message)
           }
         }
         esBulkList.append(flattenJson.toJSONString)

         if(esBulkList.nonEmpty){ madES.bulkIndex2Es( esBulkList.toList, INDEXAPPLOGS.toString) }else { logger.error(s" bulkIndex empty list \n") }

       }catch{
         case e: Exception =>
           logger.error(s" Failed to process message  ",e)
       }
    }

    Future.successful(msg)

  }





}

