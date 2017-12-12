package edp.mad.elasticsearch

import java.util.{Calendar, GregorianCalendar}

import edp.mad.elasticsearch.MadCreateIndexInterval._
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch.MadIndexPattern._
import edp.mad.module.ModuleObj
import edp.wormhole.common.util.DateUtils
import edp.wormhole.common.util.DtFormat._
import org.apache.log4j.Logger

object MadES{ val madES = new MadES }

class MadES extends ESIndexModule[String,IndexEntity]{
  private val logger = Logger.getLogger(this.getClass)

  lazy val modules = ModuleObj.getModule
  lazy val conUrl = modules.madEs.url
  lazy val esUser = modules.madEs.user
  lazy val esPwd = modules.madEs.pwd
  lazy val esToken = ""

  val madFlowMapping = """{
                         |  "mappings":{
                         |    "flows":{
                         |      "properties":{
                         |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "feedbackTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "projectId": { "type":"long", "index":"not_analyzed" },
                         |        "projectName": { "type":"keyword", "index":"not_analyzed" },
                         |        "streamId":{ "type":"long", "index":"not_analyzed" },
                         |        "streamName":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sparkAppId":{ "type":"keyword", "index":"not_analyzed" },
                         |        "streamStatus":{ "type":"keyword", "index":"not_analyzed" },
                         |        "streamStartedTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "streamConsumerDuration":{ "type":"integer", "index":"not_analyzed" },
                         |        "streamConsumerMaxRecords": { "type":"integer", "index":"not_analyzed" },
                         |        "streamProcessRepartition": { "type":"integer", "index":"not_analyzed" },
                         |        "streamDriverCores": { "type":"integer", "index":"not_analyzed" },
                         |        "streamDriverMemory": { "type":"integer", "index":"not_analyzed" },
                         |        "streamPerExecuterCores": { "type":"integer", "index":"not_analyzed" },
                         |        "streamPerExecuterMemory": { "type":"integer", "index":"not_analyzed" },
                         |        "kafkaConnection":{ "type":"keyword", "index":"not_analyzed" },
                         |        "topicName":{ "type":"keyword", "index":"not_analyzed" },
                         |        "flowId":{ "type":"long", "index":"not_analyzed" },
                         |        "flowNamespace":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sourceNamespace":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sourceDataSystem":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sourceInstance":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sourceDatabase":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sourceTable":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkNamespace":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkDataSystem":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkInstance":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkDatabase":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkTable":{ "type":"keyword", "index":"not_analyzed" },
                         |        "flowStatus":{ "type":"keyword", "index":"not_analyzed" },
                         |        "flowStartedTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "flowUpdateTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "consumedProtocol":{ "type":"keyword", "index":"not_analyzed" },
                         |        "sinkSpecificConfig":{ "type":"text"},
                         |        "tranConfig":{ "type":"keyword", "index":"not_analyzed" },
                         |        "tranActionCustomClass":{ "type":"keyword", "index":"not_analyzed" },
                         |        "transPushdownNamespaces":{ "type":"text" },
                         |        "flowErrorMaxWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "flowErrorMinWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |        "flowErrorCount": { "type":"integer", "index":"not_analyzed" },
                         |        "flowErrorMessage":{ "type":"text" },
                         |        "statsId": { "type":"keyword", "index":"not_analyzed" },
                         |        "rddCount":{ "type":"long", "index":"not_analyzed" },
                         |        "throughput":{ "type":"long", "index":"not_analyzed" },
                         |        "originalDataTs ":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "rddTransformStartTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "directiveProcessStartTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "mainProcessStartTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "swiftsProcessStartTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "sinkWriteStartTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "processDoneTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss.SSSSSS", "index":"not_analyzed" },
                         |        "intervalMainProcessToDataOriginalTs":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalMainProcessToDone ":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalMainProcessToSwifts":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalMainProcessToSink":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalSwiftsToSink":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalSinkToDone":{ "type":"long", "index":"not_analyzed" },
                         |        "intervalRddToDone":{ "type":"long", "index":"not_analyzed" }
                         |      }
                         |    }
                         |  }
                         |}
                         |""".stripMargin

  val madStreamMapping = """{
                           |"mappings":{
                           | "streams":{
                           |            "properties":{
                           |                "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |                "projectId": { "type":"long", "index":"not_analyzed" },
                           |                "projectName": { "type":"text", "index":"not_analyzed" },
                           |                "projectResourceCores": { "type":"integer", "index":"not_analyzed" },
                           |                "projectResourceMemory": { "type":"integer", "index":"not_analyzed" },
                           |                "projectResourceMemory": { "type":"integer", "index":"not_analyzed" },
                           |                "projectCreatedTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |                "projectUpdatedTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |                "streamId":{ "type":"long", "index":"not_analyzed" },
                           |                "streamName": { "type":"text", "index":"not_analyzed" },
                           |                "sparkAppId": { "type":"text", "index":"not_analyzed" },
                           |                "streamStatus": { "type":"text", "index":"not_analyzed" },
                           |                "streamStartedTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |                "sparkConfig":{ "type":"text" },
                           |                "streamConsumerDuration": { "type":"integer", "index":"not_analyzed" },
                           |                "streamConsumerMaxRecords": { "type":"integer", "index":"not_analyzed" },
                           |                "streamProcessRepartition": { "type":"integer", "index":"not_analyzed" },
                           |                "streamDriverCores": { "type":"integer", "index":"not_analyzed" },
                           |                "streamDriverMemory": { "type":"integer", "index":"not_analyzed" },
                           |                "streamPerExecuterCores": { "type":"integer", "index":"not_analyzed" },
                           |                "streamPerExecuterMemory": { "type":"integer", "index":"not_analyzed" },
                           |                "kafkaConnection":{ "type":"text", "index":"not_analyzed" },
                           |                "streamStatTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |                "errorMessage":{ "type":"text"},
                           |                "topicName":{ "type":"text", "index":"not_analyzed" },
                           |                "partitionNum": { "type":"integer", "index":"not_analyzed" },
                           |                "partitionId": { "type":"integer", "index":"not_analyzed" },
                           |                "latestOffset":{"type": "long", "index": "not_analyzed"},
                           |                "feedbackOffset":{"type": "long", "index": "not_analyzed"}
                           |           }
                           |        }
                           |}
                           |}""".stripMargin

  val madLogsMapping = """{
                         |"mappings":{
                         | "logs":{
                         |            "properties":{
                         |                "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                         |                "projectId": { "type":"long", "index":"not_analyzed" },
                         |                "projectName": { "type":"text", "index":"not_analyzed" },
                         |                "streamId":{ "type":"long", "index":"not_analyzed" },
                         |                "streamName": { "type":"text", "index":"not_analyzed" },
                         |                "sparkAppId": { "type":"text", "index":"not_analyzed" },
                         |                "streamStatus": { "type":"text", "index":"not_analyzed" },
                         |                "logsOrder":{ "type":"keyword", "index":"not_analyzed" },
                         |                "hostName":{"type": "keyword","index": "not_analyzed" },
                         |        	      "logTime":{"type": "date", "format": "yyyy-MM-dd HH:mm:ss", "index": "not_analyzed" },
                         |                "logLevel":{"type": "keyword","index": "not_analyzed" },
                         |                "logPath":{"type": "text" },
                         |                "className":{"type": "text" },
                         |                "message":{"type": "text" }
                         |           }
                         |        }
                         |}
                         |}""".stripMargin

  def initial() = {
    logger.info(s"  ES initial\n")
    try {
      setIndexMap(INDEXFLOWS.toString, IndexEntity(INDEXFLOWS.toString, "flows", madFlowMapping, YYYYMMDD.toString,EVERYDAY.toString,  "7"))
      setIndexMap(INDEXSTREAMS.toString, IndexEntity(INDEXSTREAMS.toString, "streams", madStreamMapping, YYYYMM.toString, EVERYMONTH.toString,  "3"))
      setIndexMap(INDEXLOGS.toString, IndexEntity(INDEXLOGS.toString, "logs", madLogsMapping, YYYYMMDD.toString, EVERYDAY.toString,  "7"))

      autoCreateIndexByPattern
    }catch{
      case ex:Exception =>
        logger.info(s" failed to initial the ES \n",ex)
    }
  }

  def insertEs( body: String, indexKey: String):Boolean = {
    var rc = false
    try {
      getEntity(indexKey) match {
        case Some(t) => {
          val indexName = getCurrentIndex(indexKey)
          val url = s"${conUrl}/${indexName}/${t.typeName}"
          rc = insertDoc(body, url, esUser, esPwd, esToken)
          if (rc == false) {
            createIndexByPattern(indexKey)
            rc = insertDoc(body, url, esUser, esPwd, esToken)
          }

        }
        case None => logger.info(s" can't found the setting for index ${indexKey.toString}\n")
      }
    }catch{
      case ex: Exception =>
        logger.error(s" failed to index document to Elasticsearch \n", ex)
    }
    rc
  }

  def deleteHistoryIndex= {
    val nDays = modules.madMaintenance.esRemain
    val nMonth = 3
    val cal = new GregorianCalendar()
    var i :Int = nDays
    var j : Int = nMonth
    val range = 2
    try {
      getIndexMapKeySet().foreach{indexType =>
         getEntity(indexType) match {
          case Some(t) => {
            if( t.createIndexInterval == EVERYDAY.toString) {
              while( i < range*nDays){
                cal.setTime(new java.util.Date())
                cal.add(Calendar.DAY_OF_MONTH,(-1) * i)
                val dateStr = DateUtils.dt2string(cal.getTime(), DATE_DASH)
                val indexName = s"${indexType.toString}_${dateStr}"
                val url = s"${conUrl}/${indexName}"
                deleteIndex("", url, esUser, esPwd, esToken)
                logger.info(s" delete Index ${url} \n")
                i=i+1
              }
            }else if(t.createIndexInterval == EVERYMONTH.toString ){
              while( j < range* nMonth){
                cal.setTime(new java.util.Date())
                cal.add(Calendar.MONTH,(-1) * j)
                val dateStr = DateUtils.dt2string(cal.getTime(), DATE_DASH).substring(0,7)
                val indexName = s"${indexType.toString}_${dateStr}"
                val url = s"${conUrl}/${indexName}"
                deleteIndex("", url, esUser, esPwd, esToken)
                logger.info(s" delete Index ${url} \n")
                j=j+1
              }
            }
          }
          case None => logger.info(s" can't found the setting for index ${indexType}\n")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to delete feedback history data", e)
    }
  }

  def autoCreateIndexByPattern= {
    try {
      getIndexMapKeySet().foreach{indexType =>
        logger.info(s"  index key ${indexType} \n")
        createIndexByPattern(indexType)
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to auto create index by pattern", e)
    }
  }

  def createIndexByPattern(indexType: String)= {
    try {
      logger.info(s" ==  ${indexType} \n")
      getEntity(indexType) match {
        case Some(t) => {
            logger.info(s" ${t}\n")
          logger.info(s" createIndexInterval ${t.createIndexInterval}   ${EVERYDAY.toString} \n")
            if( t.createIndexInterval == EVERYDAY.toString) {
              val dateStr = DateUtils.dt2string(new java.util.Date(), DATE_DASH)
              val indexName = s"${indexType.toString}_${dateStr}"
              val url = s"${conUrl}/${indexName}"
              createIndex(t.createIndexJson, url, esUser, esPwd, esToken)
              logger.info(s" create Index ${url} \n")
            }else if(t.createIndexInterval == EVERYMONTH.toString ){
              val dateStr = DateUtils.dt2string(new java.util.Date(), DATE_DASH).substring(0,7)
              val indexName = s"${indexType.toString}_${dateStr}"
              val url = s"${conUrl}/${indexName}"
              createIndex("", url, esUser, esPwd, esToken)
              logger.info(s" create Index ${url} \n")
            }else{
              logger.info(s" createIndexInterval ${t.createIndexInterval}  \n")
            }
          }
        case None => logger.info(s" can't found the setting for index ${indexType}\n")
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to auto create index by pattern", e)
    }
  }

  def getCurrentIndex( indexKey: String): String ={
    var indexName = indexKey.toString
    try {
        getEntity(indexKey) match {
          case Some(t) => {
            if( t.createIndexInterval == EVERYDAY.toString) {
              val dateStr = DateUtils.dt2string(new java.util.Date(), DATE_DASH)
              indexName = s"${indexKey.toString}_${dateStr}"
            }else if(t.createIndexInterval == EVERYMONTH.toString ){
              val dateStr = DateUtils.dt2string(new java.util.Date(), DATE_DASH).substring(0,7)
              indexName = s"${indexKey.toString}_${dateStr}"
            }
          }
          case None => logger.info(s" can't found the setting for index ${indexKey}\n")
        }

    } catch {
      case e: Exception =>
        logger.error(s"failed to auto create index by pattern", e)
    }
    indexName
  }



  def indexMaintenance={
    deleteHistoryIndex
  }
}
