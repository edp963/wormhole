package edp.mad.elasticsearch

import java.util.{Calendar, GregorianCalendar}

import edp.mad.elasticsearch.MadCreateIndexInterval._
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch.MadIndexPattern._
import edp.mad.module.ModuleObj
import edp.wormhole.common.util.{DateUtils, JsonUtils}
import edp.wormhole.common.util.DtFormat._
import org.apache.log4j.Logger
import org.json4s.JsonAST.{JNothing, JValue}

import scala.xml.Null

object MadES{ val madES = new MadES }

class MadES extends ESIndexModule[String,IndexEntity]{
  private val logger = Logger.getLogger(this.getClass)

  lazy val modules = ModuleObj.getModule
  lazy val conUrl = modules.madEs.url
  lazy val esUser = modules.madEs.user
  lazy val esPwd = modules.madEs.pwd
  lazy val esToken = ""

  implicit val formats = org.json4s.DefaultFormats
  val madProjectInfosMapping = """{
                                 |"mappings":{
                                 | "projects":{
                                 |            "properties":{
                                 |                "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |                "projectId": { "type":"long", "index":"not_analyzed" },
                                 |                "projectName": { "type":"keyword" },
                                 |                "projectResourceCores": { "type":"integer", "index":"not_analyzed" },
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
                              |    "apps":{
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
                              |    "namespaces":{
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
                                |        "streamId":{ "type":"long", "index":"not_analyzed" },
                                |        "streamName":{ "type":"keyword" },
                                |        "flowId":{ "type":"long", "index":"not_analyzed" },
                                |        "flowNamespace":{ "type":"keyword" },
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


  val madFlowErrorMapping = """{
                                 |  "mappings":{
                                 |    "flows":{
                                 |      "properties":{
                                 |       "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |        "feedbackTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |        "streamId":{ "type":"long", "index":"not_analyzed" },
                                 |        "streamName":{ "type":"keyword" },
                                 |        "flowId":{ "type":"long", "index":"not_analyzed" },
                                 |        "flowNamespace":{ "type":"keyword" },
                                 |        "flowErrorMaxWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |        "flowErrorMinWaterMarkTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                 |        "flowErrorCount": { "type":"integer", "index":"not_analyzed" },
                                 |        "flowErrorMessage":{ "type":"text" }
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
                                  |     "feedbackTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                  |     "projectId": { "type":"long", "index":"not_analyzed" },
                                  |     "projectName": { "type":"keyword" },
                                  |     "streamId":{ "type":"long", "index":"not_analyzed" },
                                  |     "streamName": { "type":"keyword" },
                                  |     "topicName":{ "type":"keyword" },
                                  |     "partitionNum": { "type":"integer", "index":"not_analyzed" },
                                  |     "partitionId": { "type":"integer", "index":"not_analyzed" },
                                  |     "latestOffset":{"type": "long", "index": "not_analyzed"},
                                  |     "feedbackOffset":{"type": "long", "index": "not_analyzed"}
                                  |    }
                                  |  }
                                  |}
                                  |}""".stripMargin

  val madStreamErrorMapping = """{
                                   |"mappings":{
                                   | "streams":{
                                   |   "properties":{
                                   |     "madProcessTime": { "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                   |     "feedbackTs":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss", "index":"not_analyzed" },
                                   |     "projectId": { "type":"long", "index":"not_analyzed" },
                                   |     "projectName": { "type":"keyword" },
                                   |     "streamId":{ "type":"long", "index":"not_analyzed" },
                                   |     "streamName": { "type":"keyword" },
                                   |     "status":{ "type":"keyword"},
                                   |     "errorMessage":{ "type":"text"}
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
                           |    "logsOrder":{ "type":"keyword", "index":"not_analyzed" },
                           |    "hostName":{"type": "keyword","index": "not_analyzed" },
                           |    "logTime":{"type": "keyword", "index": "not_analyzed" },
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
      setIndexMap(INDEXPROJECTINFOS.toString, IndexEntity(INDEXPROJECTINFOS.toString, "projects", madProjectInfosMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXSTREAMINFOS.toString, IndexEntity(INDEXSTREAMINFOS.toString, "streams", madStreamInfosMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXFLOWINFOS.toString, IndexEntity(INDEXFLOWINFOS.toString, "flows", madFlowInfosMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXAPPINFOS.toString, IndexEntity(INDEXAPPINFOS.toString, "apps", madAppInfosMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXNAMESPACEINFOS.toString, IndexEntity(INDEXNAMESPACEINFOS.toString, "namespaces", madNamespaceInfosMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))

      setIndexMap(INDEXSTREAMSFEEDBACK.toString, IndexEntity(INDEXSTREAMSFEEDBACK.toString, "streams", madStreamFeedbackMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXSTREAMERROR.toString, IndexEntity(INDEXSTREAMERROR.toString, "streams", madStreamErrorMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))
      setIndexMap(INDEXFLOWFEEDBACK.toString, IndexEntity(INDEXFLOWFEEDBACK.toString, "flows", madFlowFeedbackMapping, YYYYMMDD.toString, EVERYDAY.toString,  "7",null))
      setIndexMap(INDEXFLOWERROR.toString, IndexEntity(INDEXFLOWERROR.toString, "flows", madFlowErrorMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))

      setIndexMap(INDEXAPPLOGS.toString, IndexEntity(INDEXAPPLOGS.toString, "logs", madAppLogsMapping, YYYYMMDD.toString, EVERYDAY.toString,  "7",null))

      setIndexMap(INDEXSTREAMALERT.toString, IndexEntity(INDEXSTREAMALERT.toString, "alert", madStreamAlertMapping, YYYYMM.toString, EVERYMONTH.toString,  "90",null))

      autoCreateIndexByPattern

      updateIndexMappingSchema
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

  def bulkIndex2Es( body: List[String], indexKey: String ):Boolean = {
    var rc = false
    try {
      getEntity(indexKey) match {
        case Some(t) => {
          val indexName = getCurrentIndex(indexKey)
          val url = s"${conUrl}/${indexName}/${t.typeName}/_bulk"
          val indexJsonStr =
            s"""{"index":{"_index":"${indexName}","_type":"${t.typeName}"}""".stripMargin
          var bodyJsonStr = ""
          var sum = 0
          var count = 0
          logger.info(s" bulkIndex $url ${indexJsonStr} \n")
          body.foreach{e=>
            bodyJsonStr= s"${bodyJsonStr}\n${indexJsonStr}\n${e}"
           // logger.info(s" bulkIndex $url count ${count} \n")
            count = count +1
            if(count> 100){
              rc = bulkIndex(bodyJsonStr, url, esUser, esPwd, esToken)
              if (rc == false) {
                createIndexByPattern(indexKey)
                logger.info(s" bulkIndex  \n")
              }
              sum = sum + count
              count = 0
              bodyJsonStr = ""
            }
          }
         // logger.info(s" bulkIndex $url count ${count} \n")
          if(count>0){
            if(count == 1) bodyJsonStr = s"${bodyJsonStr}\n"
            rc = bulkIndex(bodyJsonStr, url, esUser, esPwd, esToken)
            if (rc == false) {
              createIndexByPattern(indexKey)
              logger.info(s" bulkIndex  \n")
            }
            sum = sum + count
          }
          logger.info(s" bulkIndex $url count ${sum} \n")
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


  def updateIndexMappingSchema = {
    try {
      getIndexMapKeySet().foreach{indexType =>
        logger.info(s"  index key ${indexType} \n")
        getEntity(indexType) match {
          case Some(t) => {
            val indexName = getCurrentIndex(indexType)
            val url = s"${conUrl}/${indexName}"
            val mappings = getIndex(url, esUser, esPwd, esToken)
            val schemaMap = getSchemaMapByRepsonse(indexName,t.typeName,mappings)
            logger.info(s" = = = ${url}    ${schemaMap} ")
            setIndexMap(indexType, IndexEntity(t.index, t.typeName, t.createIndexJson, t.createIndexPattern, t.createIndexInterval, t.retainIndexDays, schemaMap))
          }
          case None => logger.info(s" can't found the setting for index ${indexType}\n")
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"failed to auto create index by pattern", e)
    }
  }

  def getClassType( tStr:String)= {
    tStr match {
      case "date" => classOf[String]
      case "keyword" => classOf[String]
      case "text" => classOf[String]
      case "integer" => classOf[Int]
      case "long" => classOf[Long]
    }
  }

  def getSchemaMap(indexKey: String): Map[String,Any] = {
    getEntity(indexKey) match {
      case Some(t) => {
       t.EsMappingsSchema
      }
      case None =>
        logger.info(s" can't found the setting for index ${indexKey}\n")
        null
    }
  }

   def getSchemaMapByRepsonse(_index: String, _type: String, mapJsonStr: String ):Map[String, Any] ={
     //logger.info(s"= = = = 00 ${_index}    ${_type}  ")
    // logger.info(s"= = = = 0 ${mapJsonStr}")
    val jv = JsonUtils.json2jValue(mapJsonStr)
     //logger.info(s"= = = = 1 ${jv}")
     try {

       val jObj = JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(jv, _index.toString), s"mappings"), _type.toString), "properties")
       //logger.info(s"= = = = 1 ${jObj}")
         if(jObj != JNothing && jObj != null){
           val jObj2 = JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(jv, _index.toString), s"mappings"), _type.toString), "properties"), "@metadata")
         //  logger.info(s"= = = = 1 ${jObj2}")
           if( jObj2 != JNothing && jObj2 != null ){
             val jObj3 = JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(JsonUtils.getJValue(jv, _index.toString), s"mappings"), _type.toString), "properties"), "@metadata"),"properties")
           //  logger.info(s"= = = = 1 ${jObj3}")
             if(jObj3 != JNothing && jObj3 != null){
               val mapCols = jObj3.extract[Map[String, JValue]]
               mapCols.map{ e=>
                 (e._1, JsonUtils.getJValue(e._2,"type").extract[String] )
               }
             }else null
           }else{
             val mapCols = jObj.extract[Map[String, JValue]]
             mapCols.map{ e=>
               (e._1, JsonUtils.getJValue(e._2,"type").extract[String] )
             }
           }

         }else null

     }catch {
       case e: Exception =>
         logger.info(s" = = =  ${e}")
         null
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
      getEntity(indexType) match {
        case Some(t) => {
          val indexName = getCurrentIndex(indexType)
          val url = s"${conUrl}/${indexName}"
          createIndex(t.createIndexJson, url, esUser, esPwd, esToken)
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
