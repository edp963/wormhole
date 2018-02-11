package edp.mad.alert

import edp.mad.elasticsearch.MadES.madES
import edp.mad.elasticsearch.MadIndex.INDEXSTREAMALERT
import edp.mad.elasticsearch.StreamAlert
import edp.mad.module.ModuleObj
import edp.mad.rest.response.YarnRMResponse
import edp.wormhole.common.util.DateUtils.{currentyyyyMMddHHmmss, yyyyMMddHHmmssToString}
import edp.wormhole.common.util.{DateUtils, DtFormat, JsonUtils}
import org.apache.log4j.Logger

object StreamStatus extends Enumeration {
  type StreamStatus = Value

  val NEW = Value("new")
  val STARTING = Value("starting")
  val WAITING = Value("waiting")
  val RUNNING = Value("running")
  val STOPPING = Value("stopping")
  val STOPPED = Value("stopped")
  val FAILED = Value("failed")
  val FINISHED = Value("finished")
  val DONE = Value("done")

  def streamStatus(s: String) = StreamStatus.withName(s.toLowerCase)
}

object SparkAppStatus extends Enumeration {
  type SparkAppStatus = Value

  val STARTING = Value("STARTING")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val SUCCEEDED = Value("SUCCEEDED")
  val KILLED = Value("KILLED")
  val FINISHED = Value("FINISHED")

  def sparkAppStatus(s: String) = StreamStatus.withName(s.toUpperCase)
}

object AlertLevel extends Enumeration {
  type AlertLevel = Value

  val OK = Value("OK")
  val WARN = Value("warn")
  val ERROR = Value("error")

  def alertLevel(s: String) = AlertLevel.withName(s.toUpperCase)
}


object StreamDiagnosis{
  private val logger = Logger.getLogger(this.getClass)

  lazy val modules = ModuleObj.getModule

  def diagnosisStreamStatusAndAppState( streamStatus:String , appState: String ):String={
    var alertLevel = AlertLevel.ERROR.toString
    if(streamStatus == StreamStatus.RUNNING.toString &&  appState == SparkAppStatus.RUNNING.toString )
      alertLevel = AlertLevel.OK.toString
    else if( ( streamStatus == StreamStatus.RUNNING.toString ) &&  (appState != SparkAppStatus.RUNNING.toString))
      alertLevel = AlertLevel.ERROR.toString
    else
      alertLevel = AlertLevel.WARN.toString
    alertLevel
  }

  def streamStatusDiagnosis() ={
    val mergeMap = new scala.collection.mutable.HashMap[String, StreamAlert]
    logger.info(s" enter diagnosis \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val appList = YarnRMResponse.getAllAppsInfo()
    val streamList = modules.streamMap.getMapHandle()
    logger.info(s" diagnosis \n ")
    //logger.info(s"  ${appList}\n   ${streamList} \n ")
    try{
      appList.foreach{ app=>
        //logger.info(s" diagnosis \n ")
        val streamName = app.streamName
        //logger.info(s" stream name ${streamName} \n ")
        val res = streamList.filter(e=> e._2 == streamName)
        val streamInfo = if( res.nonEmpty) res.head else null
        //logger.info(s" stream name ${streamName} \n ")
        if(streamInfo != null ){
          logger.info(s" found app name ${streamName} in stream map \n ")
          val streamAlert = StreamAlert(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
            streamInfo._3.projectId, streamInfo._3.projectName,
            streamInfo._1, streamInfo._2,
            streamInfo._3.status,
            app.appId, app.state, app.finalStatus, diagnosisStreamStatusAndAppState(streamInfo._3.status,app.finalStatus))
          if( mergeMap.contains(streamName) == false )
            mergeMap.put(streamName,streamAlert)
        }else{
          logger.info(s" not found app name ${streamName} in stream map \n ")
        }
      }
    }catch{
      case e:Exception =>
        logger.error(s" failed to search in app list \n  ",e)
    }

    logger.info(s" enter diagnosis step 2  ${mergeMap}\n ")

    try {
      streamList.foreach { stream =>
        logger.info(s" diagnosis \n ")
        val streamName = stream._2
        val streamId = stream._1
        val streamInfo = stream._3
        logger.info(s" stream name ${streamName} \n ")
        val res = appList.filter { e => e.streamName == streamName }
        val appInfo = if(res.nonEmpty) res.head else null
        if (appInfo != null) {
          val streamAlert = StreamAlert(madProcessTime, streamInfo.projectId, streamInfo.projectName,
            streamId, streamName,
            streamInfo.status,
            appInfo.appId, appInfo.state, appInfo.finalStatus, diagnosisStreamStatusAndAppState(streamInfo.status, appInfo.finalStatus))
          if (mergeMap.contains(streamName) == false)
            mergeMap.put(streamName, streamAlert)
        }
      }
    }catch{
      case e:Exception =>
      logger.error(s" failed to search in stream list \n  ",e)
    }

    logger.info(s" merge map \n")
    try {
      mergeMap.foreach { e =>
        val postBody: String = JsonUtils.caseClass2json(e._2)
        val rc = madES.insertEs(postBody, INDEXSTREAMALERT.toString)
        logger.info(s" stream alert ${e._2} response ${rc} \n")
      }
    }catch{
      case e: Exception =>
        logger.error(s" failed to insert alert into es \n",e)
    }

  }
}