package edp.mad.cache

import edp.mad.rest.response._
import edp.wormhole.common.util.JsonUtils
import org.apache.log4j.Logger

class StreamCache extends RedisModule[StreamMapKey,StreamMapValue]{
  private val logger = Logger.getLogger(this.getClass)
//  def updateProjectInfo(streamId: Long, cacheProjectInfo: CacheProjectInfo) = {
//    get(StreamMapKey(streamId)) match{
//      case Some(value) => set( StreamMapKey(streamId), StreamMapValue(cacheProjectInfo, value.cacheStreamInfo, value.listCacheFlowInfo) )
//      case None  => set( StreamMapKey(streamId), StreamMapValue(cacheProjectInfo,null,null) )
//    }
//  }

  def updateStreamInfo(streamId: Long, cacheStreamInfo: CacheStreamInfo) = {
    get(StreamMapKey(streamId)) match {
      case Some(value) => set( StreamMapKey(streamId), StreamMapValue( cacheStreamInfo, value.listCacheFlowInfo) )
      case None => set( StreamMapKey(streamId), StreamMapValue(cacheStreamInfo,null) )
    }
  }

  def updateFlowInfo(streamId: Long, listCacheFlowInfo: List[CacheFlowInfo]) = {
    get(StreamMapKey(streamId)) match {
      case Some(value) => set (StreamMapKey (streamId), StreamMapValue ( value.cacheStreamInfo, listCacheFlowInfo) )
      case None => set (StreamMapKey (streamId), StreamMapValue ( null, listCacheFlowInfo) )
    }
  }

  def refresh: Unit =
    try {
      RiderResponse.getStreamInfoFromRider
      RiderResponse.getProjectInfoFromRider
      RiderResponse.getFlowInfoFromRider
      // logger.info("  stream Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"stream cache map refresh failed", ex)
        throw ex
    }

  def get( key: StreamMapKey ): Option[StreamMapValue] = {
    val vStr = getValueStr(key)
    if(vStr != null && vStr !="") {
      Option(JsonUtils.json2caseClass[StreamMapValue](vStr))
    }else None
  }

}

class ApplicationCache extends RedisModule[ApplicationMapKey,ApplicationMapValue]{
  private val logger = Logger.getLogger(this.getClass)

  def refresh =
    try {
      YarnRMResponse.getActiveAppsInfo()
      logger.info(" Application Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"flow cache map refresh failed", ex)
        throw ex
    }

  def mapPrint = {
    logger.info(s" Redis [${this.getClass}] ------------------------------\n")
  }

  def get( key: ApplicationMapKey ): Option[ApplicationMapValue] = {
    val vStr = getValueStr(key)
    if(vStr != null && vStr !="") {
      Option(JsonUtils.json2caseClass[ApplicationMapValue](vStr))
    }else None
  }
}


class NamespaceCache extends RedisModule[NamespaceMapkey,NamespaceMapValue]{
  private val logger = Logger.getLogger(this.getClass)

  def refresh: Unit =
    try {
      RiderResponse.getNamespaceInfoFromRider
      logger.info(s"  refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"Map refresh failed", ex)
        throw ex
    }

  def mapPrint = {
    logger.info(s" Redis [${this.getClass}] ------------------------------\n")
  }

  def get( key: NamespaceMapkey ): Option[NamespaceMapValue] = {
    val vStr = getValueStr(key)
    if(vStr != null && vStr !="") {
      Option(JsonUtils.json2caseClass[NamespaceMapValue](vStr))
    }else None
  }
}

class StreamNameCache extends RedisModule[StreamNameMapKey,StreamNameMapValue]{
  def get( key: StreamNameMapKey ): Option[StreamNameMapValue] = {
    val vStr = getValueStr(key)
    if(vStr != null && vStr !="") {
      Option(JsonUtils.json2caseClass[StreamNameMapValue](vStr))
    }else None
  }

}

class ProjectIdCache extends RedisModule[ProjectIdMapKey,ProjectIdMapValue] {

  def get( key: ProjectIdMapKey ): Option[ProjectIdMapValue] = {
    val vStr = getValueStr(key)
    if(vStr != null && vStr !="")
      Option(JsonUtils.json2caseClass[ProjectIdMapValue](vStr))
    else None
  }

}

//class TopicCache extends RedisModule[TopicMapKey,TopicMapValue]{
//  def get( key: TopicMapKey ): Option[TopicMapValue] = {
//    val vStr = getValueStr(key)
//    if(vStr != null && vStr !="") {
//      Option(JsonUtils.json2caseClass[TopicMapValue](vStr))
//    }else None
//  }
//
//}