package edp.mad.kafka


import akka.kafka.ConsumerMessage.CommittableMessage
import edp.mad.cache._
import edp.mad.elasticsearch.MadES._
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch._
import edp.mad.module.ModuleObj
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.{DateUtils, DtFormat, JsonUtils}
import org.apache.log4j.Logger

import scala.concurrent.Future

object LogsProcesser{
  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

  def processMessage (msg: CommittableMessage[Array[Byte], String] ): Future[CommittableMessage[Array[Byte], String]] = {
    logger.info(s"Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    //println(s"Consumed: [topic,partition,offset] \n [${msg.record.topic}, ${msg.record.partition}, ${msg.record.offset} ] \n ")
    if (msg.record.key() != null) logger.info(s"Consumed key: ${msg.record.key().toString}")

    modules.offsetMap.set(OffsetMapkey(modules.logsConsumerStreamId, msg.record.topic, msg.record.partition), OffsetMapValue( msg.record.offset))

    if (msg.record.value() == null || msg.record.value() == "") {
      logger.error(s"Logs message value is null: ${msg.toString}")
    } else {
      logger.info(s" message value : ${msg.toString}\n")
       try {
         val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
         val msgJsonStr = msg.record.value()
         val msgObj = JsonUtils.json2jValue(msgJsonStr)
         val appId = JsonUtils.getString(msgObj, s"appId")

         var projectId = -1L
         var projectName = ""
         var streamId = -1L
         var streamStatus = ""
         var streamName = ""
         val appMapV = modules.applicationMap.get(ApplicationMapKey(appId))
         logger.info(s" applicationMap : ${modules.applicationMap.mapPrint}\n")
         logger.info(s" appMapV : ${appMapV}\n")
         if(null != appMapV ){
           streamName = appMapV.get.streamName
           val streamInfo = modules.streamNameMap.get(StreamNameMapKey(streamName))
           logger.info(s" streamNameMap : ${modules.streamNameMap.mapPrint}\n")
           logger.info(s" streamInfo : ${streamInfo}\n")
           if(streamInfo != null) {
             projectId = streamInfo.get.projectId.toLong
             projectName = streamInfo.get.projectName
             streamId = streamInfo.get.streamId.toLong
             streamStatus = streamInfo.get.streamStatus
           }
         }

         val host = JsonUtils.getString(msgObj, s"host")
         val logTime = JsonUtils.getString(msgObj, s"ums_ts_")
         //val logstashTime =  DateUtils.dt2string(DateUtils.dt2dateTime(JsonUtils.getString(msgObj, s"@timestamp")) ,DtFormat.TS_DASH_SEC)
         val logstashTime = JsonUtils.getString(msgObj, s"@timestamp")
         val madTime = DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC)
         val loglevel = JsonUtils.getString(msgObj, s"loglevel")
         val path = JsonUtils.getString(msgObj, s"path")
         val className = JsonUtils.getString(msgObj, s"className")
         val container = JsonUtils.getString(msgObj, s"container")
         val message = JsonUtils.getString(msgObj, s"message")


         val esMadlogs = EsMadLogs( projectId, projectName, streamId, streamName, streamStatus,
           msg.record.offset.toString, appId, host, logTime,logstashTime, madTime,
           loglevel, path, className, container, message )
         logger.info(s" esMadlogs ${esMadlogs} \n")
         val postBody: String = JsonUtils.caseClass2json(esMadlogs)
         val rc =   madES.insertEs(postBody,INDEXLOGS.toString)
         logger.info(s" EsMadStreams: response ${rc}")

      }catch{
         case e: Exception =>
           logger.error(s" Failed to process message  ",e)
       }
    }

    Future.successful(msg)

  }





}

