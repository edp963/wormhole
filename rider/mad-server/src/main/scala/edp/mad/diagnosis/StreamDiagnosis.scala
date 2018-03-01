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

  val INFO = Value("info")
  val OK = Value("OK")
  val WARN = Value("warn")
  val ERROR = Value("error")
  val FLOWERR = Value("flow_error")
  val STREAMERR = Value("stream_error")

  def alertLevel(s: String) = AlertLevel.withName(s.toUpperCase)
}


object StreamDiagnosis{
  private val logger = Logger.getLogger(this.getClass)

  lazy val modules = ModuleObj.getModule

  def diagnosisStreamStatusAndAppState( streamStatus:String , appState: String , curAlertLevel: String ):String= {
    var alertLevel = AlertLevel.INFO.toString
    if (streamStatus == StreamStatus.RUNNING.toString && appState == SparkAppStatus.RUNNING.toString) {
      if (curAlertLevel == AlertLevel.INFO.toString)
        alertLevel = AlertLevel.OK.toString
      else
        alertLevel = curAlertLevel
    } else if ((streamStatus == StreamStatus.RUNNING.toString) && (appState != SparkAppStatus.RUNNING.toString)) {
      alertLevel = AlertLevel.ERROR.toString
    } else{
      if(curAlertLevel != AlertLevel.INFO.toString)
      alertLevel = curAlertLevel
    }
    alertLevel
  }

  def streamStatusDiagnosis() = {
    val mergeMap = new scala.collection.mutable.HashMap[String, StreamAlert]
    val allStreamMap = new scala.collection.mutable.HashMap[String, StreamAlert]
    logger.info(s" enter diagnosis \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

    logger.info(s" diagnosis \n ")

    // get all stream info form app list
    try {
      YarnRMResponse.getAllAppsInfo().foreach { app =>
        //logger.info(s" diagnosis \n ")
        val streamName = app.streamName
        //logger.info(s" stream name ${streamName} \n ")
        if (allStreamMap.contains(streamName) == false) {
          val streamAlert = StreamAlert(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
            0, "", 0, streamName, "",
            app.appId, app.state, app.finalStatus, AlertLevel.INFO.toString, "")
          allStreamMap.put(streamName, streamAlert)
        } else {
          val opt = allStreamMap.get(streamName)
          opt match {
            case Some(opt) =>
              val streamAlert = StreamAlert(
                DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
                opt.projectId, opt.projectName, opt.streamId, streamName, opt.streamStatus,
                app.appId, app.state, app.finalStatus, opt.alertLevel, opt.alertInfo)
              allStreamMap.put(streamName, streamAlert)
            case None =>
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s" failed to search in stream list \n  ", e)
    }
    logger.info(s" enter diagnosis step 2  ${mergeMap}\n ")

    // get all stream Info from  stream Map
    try {
      modules.streamMap.getMapHandle().foreach { stream =>
        logger.info(s" diagnosis \n ")
        val streamName = stream._2
        val streamId = stream._1
        val streamInfo = stream._3
        logger.info(s" stream name ${streamName} \n ")

        if (allStreamMap.contains(streamName) == false) {
          val streamAlert = StreamAlert(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
            streamInfo.projectId, streamInfo.projectName, streamId, streamName, streamInfo.status,
            "", "", "", AlertLevel.INFO.toString, "")
          allStreamMap.put(streamName, streamAlert)
        } else {
          val opt = allStreamMap.get(streamName)
          opt match {
            case Some(opt) =>
              val streamAlert = StreamAlert(
                DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
                streamInfo.projectId, streamInfo.projectName, streamId, streamName, streamInfo.status,
                opt.appId, opt.state, opt.finalStatus, opt.alertLevel, opt.alertInfo )
              allStreamMap.put(streamName, streamAlert)
            case None =>
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s" failed to search in stream list \n  ", e)
    }


    // get info from alert Map
    try {
      modules.alertMap.getMapHandle.foreach { aInfo =>
        logger.info(s" diagnosis \n ")
        val streamId = aInfo._1
        val streamName = aInfo._2
        val alertType = aInfo._3
        val alertMsg = aInfo._5

        logger.info(s" stream id ${streamId} \n ")

        if (allStreamMap.contains(streamName) == false) {
          val streamAlert = StreamAlert(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
            0, "", 0, "", "",
            "", "", "", alertType, alertMsg)
          allStreamMap.put(streamName, streamAlert)
        } else {
          val opt = allStreamMap.get(streamName)
          opt match {
            case Some(opt) =>
              val streamAlert = StreamAlert(
                DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime), DtFormat.TS_DASH_SEC),
                opt.projectId, opt.projectName, streamId, streamName, opt.streamStatus,
                opt.appId, opt.state, opt.finalStatus, alertType, s"${opt.alertInfo} ${alertMsg}")
              allStreamMap.put(streamName, streamAlert)
            case None =>
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s" failed to search in stream list \n  ", e)
    }

    allStreamMap.foreach { e =>
      mergeMap.put(e._1, StreamAlert(e._2.madProcessTime, e._2.projectId, e._2.projectName, e._2.streamId, e._2.streamName, e._2.streamStatus,
        e._2.appId, e._2.state, e._2.finalStatus, diagnosisStreamStatusAndAppState(e._2.streamStatus, e._2.state, e._2.alertLevel), e._2.alertInfo))
    }

    logger.info(s" merge map \n")
    try {
      mergeMap.foreach { e =>
        val postBody: String = JsonUtils.caseClass2json(e._2)
        val rc = madES.insertEs(postBody, INDEXSTREAMALERT.toString)
        logger.info(s" stream alert ${e._2} response ${rc} \n")
      }
    } catch {
      case e: Exception =>
        logger.error(s" failed to insert alert into es \n", e)
    }

  }
}