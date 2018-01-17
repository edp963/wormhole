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
import org.json4s.{DefaultFormats, Formats, JNothing, JValue}

import scala.collection.mutable.ListBuffer

object RiderResponse{
  private val logger = Logger.getLogger(this.getClass)
  implicit val json4sFormats: Formats = DefaultFormats
  val modules = ModuleObj.getModule
  case class ProjectId(projectId:Long, streamId:Long)
  val defaultDateTime = "1970-01-01 00:00:00"

  def getStreamInfoFromRider = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/streams"
    logger.info(s" URL \n ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val projectIdList = new ListBuffer[ProjectId]
    val response = HttpClient.syncClientGetJValue("",url, HttpMethods.GET,modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token )
    if (response._1 == true) {
      try {
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
          logger.debug( s" == \n" )
          val a = JsonUtils.getJValue(streamObj,s"kafkaInfo")
          var  kafkaConnection= ""
          if( a != null && a != JNothing ){
            kafkaConnection = JsonUtils.getString(JsonUtils.getJValue(streamObj,s"kafkaInfo"),s"connUrl")
            logger.debug( s" == ${a}\n " )
          }else{
            logger.debug( s" == ${a}\n" )
          }

          val topicList = new ListBuffer[CacheTopicInfo]
          logger.debug( s" ==  \n" )
          if(JsonUtils.getJValue(streamObj, s"topicInfo") != null ) {
            logger.debug( s" ==  \n" )
            val tlist = JsonUtils.getJValue(streamObj, s"topicInfo").extract[Array[JValue]]
            logger.debug( s" ==  \n" )
            tlist.foreach{ topicObj =>
              val topicName = JsonUtils.getJValue(topicObj,  "name").extract[String]
              val partitionOffset = JsonUtils.getJValue(topicObj,  "partitionOffsets").extract[String]
              val latestPartitionOffset = if(kafkaConnection != "") OffsetUtils.getKafkaLatestOffset( kafkaConnection, topicName) else ""
              if(latestPartitionOffset == "") logger.error(s" failed to get latest offset from kafka connection: ${kafkaConnection}  topic: ${topicName} \n")
              topicList += CacheTopicInfo(topicName,partitionOffset, latestPartitionOffset)
              logger.debug( s" ${topicList.toList} \n" )
            }
            logger.debug( s" ==  \n" )
          }
          logger.debug( s" ==  \n" )
          projectIdList.append(ProjectId(projectId,id))

          // insert stream info into ES
          val useCores  = driverCores +  executerNum * perExecuterCores
          val useMemoryG  = driverMemory +  executerNum * perExecuterMemory
          val streamInfos = StreamInfos(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC), projectId, projectName, id, name,
            appId, status, startedTime, sparkConfig, consumerDuration,consumerMaxRecords,processRepartition,
            driverCores, driverMemory, perExecuterCores, perExecuterMemory, executerNum, useCores,useMemoryG,kafkaConnection, topicList.toList.toString )
          val postBody: String = JsonUtils.caseClass2json(streamInfos)
          val rc =   madES.insertEs(postBody,INDEXSTREAMINFOS.toString)
          logger.debug(s" EsMadFlows: response ${rc}")

          val cacheStreamInfo = CacheStreamInfo(id, projectId, projectName, name, status,kafkaConnection,topicList.toList)
          modules.streamMap.updateStreamInfo(id,cacheStreamInfo)

          logger.debug( s" ==  ${cacheStreamInfo}\n" )
        }
      }catch{
        case ex: Exception =>
          logger.error(s" Parse response error ",ex)
      }
        logger.debug(s" StreamSettingsInfo  ")
    }else{
      logger.error(s"Failed to get stream info from rider REST API ")
    }

//    projectIdList.groupBy(_.projectId).foreach{e =>
//      modules.projectIdMap.set(ProjectIdMapKey(s"p${e._1}"),ProjectIdMapValue(e._2.map(_.streamId).toList))
//      logger.info( s" projectId: List[StreamId] ${e._1}   ${e._2.toList.toString()}\n")
//    }
//    logger.info( s" ==  ${modules.projectIdMap.mapPrint}\n" )

    logger.info( s" ==  ${modules.streamMap.mapPrint}\n" )
  }

  def getProjectInfoFromRider : List[CacheProjectInfo] = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/projects"
    logger.info(s" URL \n ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val projectList = new ListBuffer[CacheProjectInfo]
    val response = HttpClient.syncClientGetJValue("", url, HttpMethods.GET, modules.riderServer.adminUser, modules.riderServer.adminPwd, modules.riderServer.token)
    if (response._1 == true) {
      try {
        val plist = JsonUtils.getList(response._2, "payload")
        plist.foreach { projectObj =>
          logger.debug(s"\n projectObj  ${projectObj} \n")
          val projectId = JsonUtils.getJValue(projectObj, s"id").extract[Long]
          val projectName = JsonUtils.getJValue(projectObj, s"name").extract[String]
          val resourceCores = JsonUtils.getJValue(projectObj, s"resCores").extract[Int]
          val resourceMemory = JsonUtils.getJValue(projectObj, s"resMemoryG").extract[Int]
          val createdTime = JsonUtils.getJValue(projectObj, s"createTime").extract[String]
          val updatedTime = JsonUtils.getJValue(projectObj, s"updateTime").extract[String]

          // insert into ES
          val esMadProjectInfos = ProjectInfos(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
            projectId, projectName, resourceCores, resourceMemory,
            DateUtils.dt2string(DateUtils.dt2dateTime(createdTime) ,DtFormat.TS_DASH_SEC),
            DateUtils.dt2string(DateUtils.dt2dateTime(updatedTime) ,DtFormat.TS_DASH_SEC) )
          val postBody: String = JsonUtils.caseClass2json(esMadProjectInfos)
          val rc =   madES.insertEs(postBody,INDEXPROJECTINFOS.toString)
          logger.debug(s" EsMadFlows: response ${rc}")
          // cache the project Info
//          val cacheProjectInfo = CacheProjectInfo(projectId, projectName, DateUtils.dt2string(DateUtils.dt2dateTime(createdTime) ,DtFormat.TS_DASH_SEC),
//            DateUtils.dt2string(DateUtils.dt2dateTime(updatedTime) ,DtFormat.TS_DASH_SEC) )
//          projectList += cacheProjectInfo
        }
      }catch {
          case e: Exception =>
            logger.error(s"Failed to get stream info from rider REST API ${JsonUtils.jValue2json(response._2)} ", e)
      }

      //      projectList.foreach{projecInfo =>
//        logger.debug(s" ${projecInfo} \n")
//        val value = modules.projectIdMap.get(ProjectIdMapKey(s"p${projecInfo.id}"))
//        if(null != value && value != None) {
//          value.get.streamIds.foreach{streamId=>
//            logger.debug(s" ${streamId} \n")
//            modules.streamMap.updateProjectInfo(streamId, projecInfo)
//          }
//        }
//      }
//      logger.info(s" ${modules.streamMap.mapPrint} \n")
    }else{
      logger.error(s"Failed to get project info from rider REST API ")
    }

    logger.info(s" REST API get Project Info ${response._1}  ${JsonUtils.jValue2json(response._2)}")
    projectList.toList
  }

  def getFlowInfoFromRider  = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/flows"
    logger.info(s" URL \n ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)
    val flowList = new ListBuffer[CacheFlowInfo]
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
          val esMadFlowInfos = FlowInfos(
            DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC),
            projectId, projectName, streamId, streamName,id, flowNamespace,sourceNamespace,
            sourceNs(0),sourceNs(1), sourceNs(2),sourceNs(3),sinkNamespace,sinkNs(0),sinkNs(1), sinkNs(2), sinkNs(3), flowStatus,
            if(flowStartedTime== "") defaultDateTime else DateUtils.dt2string(DateUtils.dt2dateTime(flowStartedTime) ,DtFormat.TS_DASH_SEC),
            DateUtils.dt2string(DateUtils.dt2dateTime(updateTime) ,DtFormat.TS_DASH_SEC),
            consumedProtocol,
            sinkSpecificConfig, tranConfig.extract[String],tranActionCustomClass, transPushdownNamespaces)
          val postBody: String = JsonUtils.caseClass2json(esMadFlowInfos)
          val rc =   madES.insertEs(postBody,INDEXFLOWINFOS.toString)
          logger.debug(s" EsMadFlows: response ${rc}")

          val cacheFlowInfo = CacheFlowInfo( id,projectId, projectName, streamId, streamName, flowNamespace )
          logger.debug(s" == ${cacheFlowInfo}\n")
          flowList += cacheFlowInfo
        } //plist.foreach
      }catch {
        case e: Exception =>
          logger.error(s"Failed to get flow info from rider REST API ${JsonUtils.jValue2json( response._2)}", e)
      }
      logger.debug(s" FlowSettingsInfo \n ${flowList} \n ")
    }else{
        logger.error(s" Http response: ${response._1} ${JsonUtils.jValue2json( response._2)}\n ")
    }

    logger.debug(s" ==== flows repsonse  \n")
    flowList.groupBy(_.streamId).foreach{e =>
      modules.streamMap.updateFlowInfo(e._1,e._2.toList)
      logger.debug( s" ${e._1}   ${e._2.toList.toString()}\n")
    }
    logger.info(s" ${modules.streamMap.mapPrint} \n")
  }

  def getNamespaceInfoFromRider  = {
    val url = s"http://${modules.riderServer.host}:${modules.riderServer.port}/api/v1/admin/namespaces"
    logger.info(s" URL \n ${url} \n ")
    val madProcessTime = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

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

          val nsInfos = NamespaceInfos( DateUtils.dt2string(DateUtils.dt2dateTime(madProcessTime) ,DtFormat.TS_DASH_SEC), ns,nsSys, nsInstance, nsDatabase, nsTable, topic )
          val postBody: String = JsonUtils.caseClass2json(nsInfos)
          val rc = madES.insertEs(postBody,INDEXNAMESPACEINFOS.toString)
          logger.debug(s"insert namespace info response ${rc}")

          modules.namespaceMap.set(NamespaceMapkey(ns), NamespaceMapValue(topic))
        }
      }catch {
        case e: Exception =>
          logger.error(s"Failed to get namespace info from rider REST API ${JsonUtils.jValue2json(response._2)} ", e)
      }
    }else{
      logger.error(s"Failed to get namespace info from rider REST API ")
    }
    logger.info(s"  ${modules.namespaceMap.mapPrint}")
  }

}