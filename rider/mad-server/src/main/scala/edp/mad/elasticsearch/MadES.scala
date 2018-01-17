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

  val madProjectInfosMapping = """{
                                 |"mappings":{
                                 | "projects":{
                                 |            "properties":{
                                 |                "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |                "projectId": { "type":"long", "index":"not_analyzed" },
                                 |                "projectName": { "type":"keyword" },
                                 |                "projectResourceCores": { "type":"integer", "index":"not_analyzed" },
                                 |                "projectResourceMemory": { "type":"integer", "index":"not_analyzed" },
                                 |                "projectResourceMemory": { "type":"integer", "index":"not_analyzed" },
                                 |                "projectCreatedTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |                "projectUpdatedTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" }
                                 |           }
                                 |        }
                                 |}
                                 |}""".stripMargin

  val madStreamInfosMapping = """{
                               |"mappings":{
                               | "streams":{
                               |            "properties":{
                               |                "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                               |                "projectId": { "type":"long", "index":"not_analyzed" },
                               |                "projectName": { "type":"keyword" },
                               |                "streamId":{ "type":"long", "index":"not_analyzed" },
                               |                "streamName": { "type":"keyword" },
                               |                "sparkAppId": { "type":"keyword"},
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
                               |                "executorNums": { "type":"integer", "index":"not_analyzed" },
                               |                "useCores": { "type":"integer", "index":"not_analyzed" },
                               |                "useMemoryG": { "type":"integer", "index":"not_analyzed" },
                               |                "kafkaConnection":{ "type":"text", "index":"not_analyzed" },
                               |                "topicName":{ "type":"keyword" }
                               |           }
                               |        }
                               |}
                               |}""".stripMargin

  val madFlowInfosMapping = """{
                             |  "mappings":{
                             |    "flows":{
                             |      "properties":{
                             |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                             |       "projectId": { "type":"long", "index":"not_analyzed" },
                             |        "projectName": { "type":"keyword" },
                             |        "streamId":{ "type":"long", "index":"not_analyzed" },
                             |        "streamName":{ "type":"keyword" },
                             |        "flowId":{ "type":"long", "index":"not_analyzed" },
                             |        "flowNamespace":{ "type":"keyword" },
                             |        "sourceNamespace":{ "type":"keyword" },
                             |        "sourceDataSystem":{ "type":"keyword"},
                             |        "sourceInstance":{ "type":"keyword"},
                             |        "sourceDatabase":{ "type":"keyword" },
                             |        "sourceTable":{ "type":"keyword"},
                             |        "sinkNamespace":{ "type":"keyword" },
                             |        "sinkDataSystem":{ "type":"keyword" },
                             |        "sinkInstance":{ "type":"keyword" },
                             |        "sinkDatabase":{ "type":"keyword"},
                             |        "sinkTable":{ "type":"keyword"},
                             |        "flowStatus":{ "type":"keyword"},
                             |        "flowStartedTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                             |        "flowUpdateTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                             |        "consumedProtocol":{ "type":"keyword" },
                             |        "sinkSpecificConfig":{ "type":"text"},
                             |        "tranConfig":{ "type":"keyword" },
                             |        "tranActionCustomClass":{ "type":"keyword" },
                             |        "transPushdownNamespaces":{ "type":"text" }
                             |      }
                             |    }
                             |  }
                             |}
                             |""".stripMargin

  val madAppInfosMapping = """{
                              |  "mappings":{
                              |    "flows":{
                              |      "properties":{
                              |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                              |        "appId": { "type":"keyword" },
                              |        "streamName":{ "type":"keyword" },
                              |        "state":{ "type":"keyword" },
                              |        "finalStatus":{ "type":"keyword" },
                              |        "user":{ "type":"keyword"},
                              |        "queue":{ "type":"keyword"},
                              |        "startedTime":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" }
                              |      }
                              |    }
                              |  }
                              |}
                              |""".stripMargin

  val madNamespaceInfosMapping = """{
                              |  "mappings":{
                              |    "flows":{
                              |      "properties":{
                              |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                              |        "namespace": { "type":"keyword" },
                              |        "nsSys":{ "type":"keyword" },
                              |        "nsInstance":{ "type":"keyword" },
                              |        "nsDatabase":{ "type":"keyword" },
                              |        "nsTable":{ "type":"keyword"},
                              |        "topic":{ "type":"keyword"}
                              |      }
                              |    }
                              |  }
                              |}
                              |""".stripMargin


  val madFlowFeedbackMapping = """{
                                |  "mappings":{
                                |    "flows":{
                                |      "properties":{
                                |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                |        "feedbackTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                |        "projectId": { "type":"long", "index":"not_analyzed" },
                                |        "projectName": { "type":"keyword" },
                                |        "streamId":{ "type":"long", "index":"not_analyzed" },
                                |        "streamName":{ "type":"keyword" },
                                |        "flowId":{ "type":"long", "index":"not_analyzed" },
                                |        "flowNamespace":{ "type":"keyword" },
                                |        "flowErrorMaxWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                |        "flowErrorMinWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                |        "flowErrorCount": { "type":"integer", "index":"not_analyzed" },
                                |        "flowErrorMessage":{ "type":"text" },
                                |        "statsId": { "type":"keyword" },
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

  val madStreamFeedbackMapping = """{
                                  |"mappings":{
                                  | "streams":{
                                  |   "properties":{
                                  |     "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                  |     "projectId": { "type":"long", "index":"not_analyzed" },
                                  |     "projectName": { "type":"keyword" },
                                  |     "streamId":{ "type":"long", "index":"not_analyzed" },
                                  |     "streamName": { "type":"keyword" },
                                  |     "streamStatTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                  |     "errorMessage":{ "type":"text"},
                                  |     "topicName":{ "type":"keyword" },
                                  |     "partitionNum": { "type":"integer", "index":"not_analyzed" },
                                  |     "partitionId": { "type":"integer", "index":"not_analyzed" },
                                  |     "latestOffset":{"type": "long", "index": "not_analyzed"},
                                  |     "feedbackOffset":{"type": "long", "index": "not_analyzed"}
                                  |    }
                                  |  }
                                  |}
                                  |}""".stripMargin

  val madAppLogsMapping = """{
                           | "mappings":{
                           |  "logs":{
                           |  "properties":{
                           |    "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                           |    "projectId": { "type":"long", "index":"not_analyzed" },
                           |    "projectName": { "type":"text", "index":"not_analyzed" },
                           |    "streamId":{ "type":"long", "index":"not_analyzed" },
                           |    "streamName": { "type":"text", "index":"not_analyzed" },
                           |    "sparkAppId": { "type":"text", "index":"not_analyzed" },
                           |    "streamStatus": { "type":"text", "index":"not_analyzed" },
                           |    "logsOrder":{ "type":"keyword", "index":"not_analyzed" },
                           |    "hostName":{"type": "keyword","index": "not_analyzed" },
                           |    "logTime":{"type": "date", "format": "yyyy-MM-dd HH:mm:ss", "index": "not_analyzed" },
                           |    "logLevel":{"type": "keyword","index": "not_analyzed" },
                           |    "logPath":{"type": "text" },
                           |    "className":{"type": "text" },
                           |    "message":{"type": "text" }
                           |  }
                           |  }
                           |}
                           |}""".stripMargin

  val madStreamAlertMapping = """{
                                   |"mappings":{
                                   | "alert":{
                                   |   "properties":{
                                   |     "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                   |     "projectId": { "type":"long", "index":"not_analyzed" },
                                   |     "projectName": { "type":"keyword" },
                                   |     "streamId":{ "type":"long", "index":"not_analyzed" },
                                   |     "streamName": { "type":"keyword" },
                                   |     "streamStatus":{ "type":"keyword" },
                                   |     "appId":{ "type":"keyword" },
                                   |     "state":{ "type":"keyword" },
                                   |     "finalStatus":{ "type":"keyword" },
                                   |     "alertLevel":{ "type":"keyword" }
                                   |    }
                                   |  }
                                   |}
                                   |}""".stripMargin


  def initial() = {
    logger.info(s"  ES initial\n")
    try {
      setIndexMap(INDEXPROJECTINFOS.toString, IndexEntity(INDEXPROJECTINFOS.toString, "projects", madProjectInfosMapping, NONEPARTITION.toString, NENVER.toString,  "365"))
      setIndexMap(INDEXSTREAMINFOS.toString, IndexEntity(INDEXSTREAMINFOS.toString, "streams", madStreamInfosMapping, NONEPARTITION.toString, NENVER.toString,  "365"))
      setIndexMap(INDEXFLOWINFOS.toString, IndexEntity(INDEXFLOWINFOS.toString, "flows", madFlowInfosMapping, NONEPARTITION.toString, NENVER.toString,  "365"))
      setIndexMap(INDEXAPPINFOS.toString, IndexEntity(INDEXAPPINFOS.toString, "apps", madAppInfosMapping, NONEPARTITION.toString, NENVER.toString,  "365"))
      setIndexMap(INDEXNAMESPACEINFOS.toString, IndexEntity(INDEXNAMESPACEINFOS.toString, "namespaces", madNamespaceInfosMapping, NONEPARTITION.toString, NENVER.toString,  "365"))

      setIndexMap(INDEXSTREAMSFEEDBACK.toString, IndexEntity(INDEXSTREAMSFEEDBACK.toString, "streams", madStreamFeedbackMapping, YYYYMM.toString, EVERYMONTH.toString,  "90"))
      setIndexMap(INDEXFLOWFEEDBACK.toString, IndexEntity(INDEXFLOWFEEDBACK.toString, "flows", madFlowFeedbackMapping, YYYYMMDD.toString, EVERYDAY.toString,  "7"))
      setIndexMap(INDEXAPPLOGS.toString, IndexEntity(INDEXAPPLOGS.toString, "logs", madAppLogsMapping, YYYYMMDD.toString, EVERYDAY.toString,  "7"))

      setIndexMap(INDEXSTREAMALERT.toString, IndexEntity(INDEXSTREAMALERT.toString, "alert", madStreamAlertMapping, NONEPARTITION.toString, NENVER.toString,  "365"))

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
          logger.info(s" createIndexInterval ${t.createIndexInterval}   ${t.retainIndexDays.toString} \n")
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
              createIndex(t.createIndexJson, url, esUser, esPwd, esToken)
              logger.info(s" create Index ${url} \n")
            }else if(t.createIndexInterval == NENVER.toString ){
              val indexName = s"${indexType.toString}"
              val url = s"${conUrl}/${indexName}"
              createIndex(t.createIndexJson, url, esUser, esPwd, esToken)
              logger.info(s" create Index ${url} \n")
            }
            else{
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
            }else{
              indexName = s"${indexKey.toString}"
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
