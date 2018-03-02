package edp.mad.rest.response

import akka.http.scaladsl.model.HttpMethods
import edp.mad.cache._
import edp.mad.elasticsearch.MadES.madES
import edp.mad.elasticsearch.MadIndex._
import edp.mad.elasticsearch._
import edp.mad.module.ModuleObj
import edp.mad.util.{HttpClient, _}
import edp.wormhole.common.util.DateUtils.{currentyyyyMMddHHmmss, yyyyMMddHHmmssToString}
import edp.wormhole.common.util.{DateUtils, DtFormat, JsonUtils}
import org.apache.log4j.Logger
import com.alibaba.fastjson.{JSON, JSONObject}
import edp.mad.rest.response.YarnRMResponse.logger
import org.json4s.JsonAST.{JNothing, JValue}

import scala.collection.mutable.ListBuffer

object RiderResponse{
  private val logger = Logger.getLogger(this.getClass)
  implicit val formats = org.json4s.DefaultFormats
  val modules = ModuleObj.getModule
  case class ProjectId(projectId:Long, streamId:Long)
  val defaultDateTime = "1970-01-01 00:00:00"

  def getStreamInfoFromRider = {
    val projectIdList = new ListBuffer[ProjectId]
    val esBulkList = new ListBuffer[String]

    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/streams/detail"
    logger.info(s" URL \n ${url} \n ")
    val response = HttpClient.syncClientGetJValue("",url, HttpMethods.GET,modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token )
    if (response._1 == true) {
      try {
        val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
        val esSchemaMap = madES.getSchemaMap(INDEXSTREAMINFOS.toString)

        val slist = JsonUtils.getList(response._2, "payload")
        slist.foreach{ streamObj =>
          logger.info(s"\n streamObj  ${streamObj} \n")
          val id = JsonUtils.getLong(JsonUtils.getJValue(streamObj, "stream"), s"id")
          val projectId = JsonUtils.getLong(JsonUtils.getJValue(streamObj, "stream"), s"projectId")
          val projectName = JsonUtils.getString( streamObj,  s"projectName")
          val name = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"name")
          val appId = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"sparkAppid")
          val status = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"status")
          val startedTime = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"startedTime")
          val sparkConfig = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"sparkConfig")
          val launchConfig = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"launchConfig")
          val startConfig = JsonUtils.getString(JsonUtils.getJValue(streamObj,  "stream"), s"startConfig")
          val launchConfigObj = JsonUtils.json2jValue(launchConfig)
          val startConfigObj = JsonUtils.json2jValue(startConfig)
          modules.streamNameMap.set(StreamNameMapKey(name), StreamNameMapValue(id,projectId,projectName))

          //modules.projectIdMap.set(ProjectIdMapKey(projectId), ProjectIdMapValue(id))
          logger.debug( s" == ${launchConfig} \n" )
          var consumerDuration =0
          var consumerMaxRecords = 0
          var processRepartition = 0
          try {
            logger.debug( s" ==  \n" )
            consumerDuration = JsonUtils.getJValue(launchConfigObj, s"durations").extract[Int]
            consumerMaxRecords = JsonUtils.getJValue(launchConfigObj, s"maxRecords").extract[Int]
            processRepartition = JsonUtils.getJValue(launchConfigObj, s"partitions").extract[Int]
          }catch{
            case e: Exception =>
              logger.error(s" Parse consumerDuration error",e)
          }

          val driverCores = JsonUtils.getJValue(startConfigObj, s"driverCores").extract[Int]
          val driverMemory = JsonUtils.getJValue(startConfigObj, s"driverMemory").extract[Int]
          val perExecuterCores = JsonUtils.getJValue(startConfigObj, s"perExecutorCores").extract[Int]
          val perExecuterMemory = JsonUtils.getJValue(startConfigObj, s"perExecutorMemory").extract[Int]
          val executerNum = JsonUtils.getJValue(startConfigObj, s"executorNums").extract[Int]
          logger.info( s" == \n" )
          val a = JsonUtils.getJValue(streamObj,s"kafkaInfo")
          var  kafkaConnection= ""
          if( a != null && a != JNothing ){
            kafkaConnection = JsonUtils.getString(JsonUtils.getJValue(streamObj,s"kafkaInfo"),s"connUrl")
          }

          val topicList = new ListBuffer[CacheTopicInfo]
          if(JsonUtils.getJValue(streamObj, s"topicInfo") != null ) {
            val tlist = JsonUtils.getJValue(streamObj, s"topicInfo").extract[Array[JValue]]
            tlist.foreach{ topicObj =>
              val topicName = JsonUtils.getJValue(topicObj,  "name").extract[String]
              val partitionOffset = JsonUtils.getJValue(topicObj,  "partitionOffsets").extract[String]
              val latestPartitionOffset = if(kafkaConnection != "") OffsetUtils.getKafkaLatestOffset( kafkaConnection, topicName) else ""
              if(latestPartitionOffset == "") logger.error(s" failed to get latest offset from kafka connection: ${kafkaConnection}  topic: ${topicName} \n")
              topicList += CacheTopicInfo(topicName,partitionOffset, latestPartitionOffset)
              logger.info( s" ${topicList.toList} \n" )
            }
          }
          projectIdList.append(ProjectId(projectId,id))

          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
            e._1 match{
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case "projectId" => flattenJson.put( e._1,projectId )
              case "projectName" => flattenJson.put( e._1,projectName )
              case "streamId" => flattenJson.put( e._1,id )
              case "streamName" => flattenJson.put( e._1,name )
              case "sparkAppId" => flattenJson.put( e._1,appId )
              case "streamStatus" => flattenJson.put( e._1,status )
              case "streamStartedTime" => flattenJson.put( e._1,DateUtils.dt2string(DateUtils.dt2dateTime(startedTime) ,DtFormat.TS_DASH_SEC) )
              case "sparkConfig" => flattenJson.put( e._1,sparkConfig )
              case "streamConsumerDuration" => flattenJson.put( e._1,consumerDuration )
              case "streamConsumerMaxRecords" => flattenJson.put( e._1,consumerMaxRecords )
              case "streamProcessRepartition" => flattenJson.put( e._1,processRepartition )
              case "streamDriverCores" => flattenJson.put( e._1,driverCores )
              case "streamDriverMemory" => flattenJson.put( e._1,driverMemory )
              case "streamPerExecuterCores" => flattenJson.put( e._1,perExecuterCores )
              case "streamPerExecuterMemory" => flattenJson.put( e._1,perExecuterMemory )
              case "executorNums" => flattenJson.put( e._1,executerNum )
              case "useCores" => flattenJson.put( e._1,(driverCores +  executerNum * perExecuterCores) )
              case "useMemoryG" => flattenJson.put( e._1, (driverMemory +  executerNum * perExecuterMemory) )
              case "kafkaConnection" => flattenJson.put( e._1,kafkaConnection )
              case "topicName" => flattenJson.put( e._1,topicList.toList.toString )
            }
          }
          esBulkList.append(flattenJson.toJSONString)

          val cacheStreamInfo = CacheStreamInfo(id, projectId, projectName, name, status,kafkaConnection,topicList.toList)
          modules.streamMap.set(StreamMapKey(id),StreamMapValue(cacheStreamInfo,null))

          logger.info( s" ==  ${cacheStreamInfo}\n" )
        }
      }catch{
        case ex: Exception =>
          logger.error(s" Parse response error ",ex)
      }
        logger.info(s" StreamSettingsInfo  ")
    }else{
      logger.error(s"Failed to get stream info from rider REST API ")
    }

    if(esBulkList.nonEmpty){
      val rc = madES.bulkIndex2Es( esBulkList.toList, INDEXSTREAMINFOS.toString)
      logger.info(s" bulkindex message into ES ${rc}\n")
    }else {
      logger.info(s" the madStreamInfo list is empty \n")
    }
  }

  def getProjectInfoFromRider : List[CacheProjectInfo] = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/projects"
    logger.info(s" URL \n ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val projectList = new ListBuffer[CacheProjectInfo]
    val esBulkList = new ListBuffer[String]

    val esSchemaMap = madES.getSchemaMap(INDEXPROJECTINFOS.toString)

    val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token)
    if (response._1 == true) {
      try {
        val plist = JsonUtils.getList(response._2, "payload")
        plist.foreach { projectObj =>
          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
            logger.info(s" = = = = 0 ${e._1}  ${e._2}")
            e._1 match{
              case "projectId" =>   flattenJson.put( e._1, JsonUtils.getJValue(projectObj, s"id").extract[Long] )
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case "projectName" =>  flattenJson.put( e._1, JsonUtils.getJValue(projectObj, s"name").extract[String] )
              case "projectResourceCores" =>  flattenJson.put( e._1, JsonUtils.getJValue(projectObj, s"resCores").extract[Int] )
              case "projectResourceMemory" => flattenJson.put( e._1,  JsonUtils.getJValue(projectObj, s"resMemoryG").extract[Int] )
              case "projectCreatedTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(JsonUtils.getJValue(projectObj, s"createTime").extract[String]) ,DtFormat.TS_DASH_SEC) )
              case "projectUpdatedTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(JsonUtils.getJValue(projectObj, s"updateTime").extract[String]) ,DtFormat.TS_DASH_SEC) )
            }
          }

          logger.info(s" = = = = 1 ${flattenJson.toJSONString}")
          esBulkList.append(flattenJson.toJSONString)

          val cacheProjectInfo = CacheProjectInfo(
            JsonUtils.getJValue(projectObj, s"id").extract[Long],
            JsonUtils.getJValue(projectObj, s"name").extract[String],
            DateUtils.dt2string(DateUtils.dt2dateTime(JsonUtils.getJValue(projectObj, s"createTime").extract[String]) ,DtFormat.TS_DASH_SEC),
            DateUtils.dt2string(DateUtils.dt2dateTime(JsonUtils.getJValue(projectObj, s"updateTime").extract[String]) ,DtFormat.TS_DASH_SEC) )
          projectList += cacheProjectInfo
        }
     }catch {
          case e: Exception =>
            logger.error(s"Failed to get stream info from rider REST API ${JsonUtils.jValue2json(response._2)} ", e)
      }
    }else{
      logger.error(s"Failed to get project info from rider REST API ")
    }


    if(esBulkList.nonEmpty){
      val rc = madES.bulkIndex2Es( esBulkList.toList, INDEXPROJECTINFOS.toString)
      logger.info(s" bulkindex message into ES ${rc}\n")
    }else {
      logger.info(s" the madProjectInfo list is empty \n")
    }

    logger.info(s" REST API get Project Info ${response._1}  ${JsonUtils.jValue2json(response._2)}")
    projectList.toList
  }

  def getFlowInfoFromRider  = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/flows"
    logger.info(s" URL ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val flowList = new ListBuffer[CacheFlowInfo]
    val esBulkList = new ListBuffer[String]
    val esSchemaMap = madES.getSchemaMap(INDEXFLOWINFOS.toString)

    val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token)
    if (response._1 == true) {
      try {
        val plist = JsonUtils.getList(response._2, "payload")
        plist.foreach { flowObj =>
          logger.debug(s"\n flowObj  ${flowObj} \n")
          val id = JsonUtils.getJValue(flowObj, s"id").extract[Long]
          val projectId = JsonUtils.getJValue(flowObj, s"projectId").extract[Long]
          val streamId = JsonUtils.getJValue(flowObj, s"streamId").extract[Long]
          val projectName = JsonUtils.getJValue(flowObj, s"projectName").extract[String]
          val streamName = JsonUtils.getJValue(flowObj, s"streamName").extract[String]
          val sourceNamespace = JsonUtils.getJValue(flowObj, s"sourceNs").extract[String]
          val sinkNamespace = JsonUtils.getJValue(flowObj, s"sinkNs").extract[String]
          val flowNamespace = s"${sourceNamespace}_${sinkNamespace}"
          val flowStatus = JsonUtils.getJValue(flowObj, s"status").extract[String]
          val flowStartedTime = JsonUtils.getJValue(flowObj, s"startedTime").extract[String]
          val updateTime =  JsonUtils.getJValue(flowObj, s"updateTime").extract[String]
          val consumedProtocol = JsonUtils.getJValue(flowObj, s"consumedProtocol").extract[String]
          val sinkSpecificConfig = JsonUtils.getJValue(flowObj, s"sinkConfig").extract[String]
          val tranConfig = JsonUtils.getJValue(flowObj, s"tranConfig")
          logger.debug(s" == \n")
          var transPushdownNamespaces =""
          var tranActionCustomClass = ""
          if(tranConfig != null && tranConfig != JNothing) {
              val action = JsonUtils.getJValue(tranConfig, s"action")
              tranActionCustomClass = if (action != null && action != JNothing) JsonUtils.getJValue(tranConfig, s"action").extract[String] else ""

//              val pushdown = JsonUtils.getJValue(tranConfigObj, s"pushdown_connection")
//              if( pushdown != null && pushdown != JNothing ){
//                logger.info(s" == ${pushdown} \n")
//                JsonUtils.getJValue(tranConfigObj, s"pushdown_connection").extract[Array[JValue]].foreach{e =>  logger.info(s" pushdown connection ${e.toString} ")}
//              }
//            }
          }
          logger.debug(s" == \n")
          val sourceNs = sourceNamespace.split("\\.")
          val sinkNs = sinkNamespace.split("\\.")
          // insert into ES

          val flattenJson = new JSONObject
          esSchemaMap.foreach{e=>
           // logger.info(s" = = = = 0 ${e._1}  ${e._2}")
            e._1 match{
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case "projectId" =>   flattenJson.put( e._1, projectId )
              case  "projectName" =>    flattenJson.put( e._1, projectName )
              case  "streamId" =>    flattenJson.put( e._1, streamId )
              case  "streamName" =>   flattenJson.put( e._1, streamName )
              case  "flowId" =>   flattenJson.put( e._1, id )
              case  "flowNamespace" =>   flattenJson.put( e._1, flowNamespace )
              case  "sourceNamespace" =>   flattenJson.put( e._1, sourceNamespace )
              case  "sourceDataSystem" =>   flattenJson.put( e._1,  sourceNs(0) )
              case  "sourceInstance" =>   flattenJson.put( e._1,  sourceNs(1))
              case  "sourceDatabase" =>   flattenJson.put( e._1,  sourceNs(2))
              case  "sourceTable" =>   flattenJson.put( e._1, sourceNs(3) )
              case  "sinkNamespace" =>   flattenJson.put( e._1, sinkNamespace )
              case  "sinkDataSystem" =>   flattenJson.put( e._1, sinkNs(0) )
              case  "sinkInstance" =>   flattenJson.put( e._1, sinkNs(1) )
              case  "sinkDatabase" =>   flattenJson.put( e._1, sinkNs(2) )
              case  "sinkTable" =>   flattenJson.put( e._1, sinkNs(3) )
              case  "flowStatus" =>   flattenJson.put( e._1, flowStatus )
              case  "flowStartedTime" =>   flattenJson.put( e._1,  if(flowStartedTime== "") defaultDateTime else DateUtils.dt2string(DateUtils.dt2dateTime(flowStartedTime) ,DtFormat.TS_DASH_SEC))
              case  "flowUpdateTime" =>   flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(updateTime) ,DtFormat.TS_DASH_SEC))
              case  "consumedProtocol" =>   flattenJson.put( e._1, consumedProtocol)
              case  "sinkSpecificConfig" =>   flattenJson.put( e._1, sinkSpecificConfig)
              case  "tranConfig" =>   flattenJson.put( e._1, tranConfig.extract[String] )
              case  "tranActionCustomClass" =>   flattenJson.put( e._1, tranActionCustomClass )
              case  "transPushdownNamespaces" =>    flattenJson.put( e._1, transPushdownNamespaces )
            }
          }
          esBulkList.append(flattenJson.toJSONString)

          val cacheFlowInfo = CacheFlowInfo( id,projectId, projectName, streamId, streamName, flowNamespace )
          flowList += cacheFlowInfo
          logger.debug(s" == ${cacheFlowInfo}\n")
        } //plist.foreach
      }catch {
        case e: Exception =>
          logger.error(s"Failed to get flow info from rider REST API ${JsonUtils.jValue2json( response._2)}", e)
      }
      logger.info(s" flowCacheList  ${flowList} \n ")
    }else{
        logger.error(s" Http response: ${response._1} ${JsonUtils.jValue2json( response._2)}\n ")
    }


    flowList.groupBy(_.streamId).foreach{e =>
      modules.streamMap.updateFlowInfo(e._1,e._2.toList)
      logger.debug( s" ${e._1}   ${e._2.toList.toString()}\n")
    }

    if( esBulkList.nonEmpty){
     // logger.info(s" ==== bulkindex   madFlowInfoJsonList ${madFlowInfoJsonList.toList}\n")
      val rc = madES.bulkIndex2Es( esBulkList.toList, INDEXFLOWINFOS.toString)
      logger.info(s" bulkindex message into ES ${rc}\n")
    }else {
      logger.info(s" the madFlowInfo list is empty \n")
    }

    //logger.info(s" ${modules.streamMap.mapPrint} \n")
  }

  def getNamespaceInfoFromRider  = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/namespaces"
    logger.info(s" URL ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val esBulkList = new ListBuffer[String]
    val esSchemaMap = madES.getSchemaMap(INDEXNAMESPACEINFOS.toString)

    val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token)
    if (response._1 == true) {
      try {
        val plist = JsonUtils.getList(response._2, "payload")
        plist.foreach { nsObj =>
          logger.info(s"\n NamespaceObj  ${nsObj} \n")

          val nsSys = JsonUtils.getJValue(nsObj, s"nsSys").extract[String]
          val nsInstance = JsonUtils.getJValue(nsObj, s"nsInstance").extract[String]
          val nsDatabase = JsonUtils.getJValue(nsObj, s"nsDatabase").extract[String]
          val nsTable = JsonUtils.getJValue(nsObj, s"nsTable").extract[String]
          val topic = JsonUtils.getJValue(nsObj, s"topic").extract[String]
          val ns = s"${nsSys}.${nsInstance}.${nsDatabase}.${nsTable}.*.*.*"

          val flattenJson = new JSONObject
          logger.info(s" namespaceSchema  ${esSchemaMap} \n")
          esSchemaMap.foreach{e=>
          //  logger.info(s" = = = = 0 ${e._1}  ${e._2}")
            e._1 match{
              case "namespace" =>   flattenJson.put( e._1, ns )
              case "madProcessTime" =>  flattenJson.put( e._1, DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC) )
              case "nsSys" =>  flattenJson.put( e._1, nsSys )
              case "nsInstance" =>  flattenJson.put( e._1, nsInstance )
              case "nsDatabase" => flattenJson.put( e._1,  nsDatabase )
              case "nsTable" =>  flattenJson.put( e._1, nsTable )
              case "topic" =>  flattenJson.put( e._1, topic )
            }
          }
          esBulkList.append(flattenJson.toJSONString)

          modules.namespaceMap.set(NamespaceMapkey(ns), NamespaceMapValue(topic))
        }
      }catch {
        case e: Exception =>
          logger.error(s"Failed to get namespace info from rider REST API ${JsonUtils.jValue2json(response._2)} ", e)
      }
    }else{
      logger.error(s"Failed to get namespace info from rider REST API ")
    }

    if( esBulkList.nonEmpty){
      val rc = madES.bulkIndex2Es(esBulkList.toList,INDEXNAMESPACEINFOS.toString )
      logger.info(s" bulkIndex2Es ${rc} \n")
    }else{
      logger.info(s" the bulkIndex list is empty \n")
    }

  }

}