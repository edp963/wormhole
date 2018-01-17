package edp.mad.cache

import edp.mad.rest.response._
import org.apache.log4j.Logger

case class CacheTopicInfo( topicName: String, partitionOffsets: String, latestPartitionOffsets: String )

case class CacheProjectInfo( id: Long, name: String, createdTime: String, updatedTime: String )

case class CacheStreamInfo( id: Long, projectId: Long, projectName: String, name: String, status: String, kafkaConn: String, topicList: List[CacheTopicInfo] )

case class CacheFlowInfo( id: Long, projectId: Long, projectName: String, streamId: Long, streamName: String, flowNamespace: String )

case class StreamMapKey(streamId: Long)
case class StreamMapValue(cacheStreamInfo: CacheStreamInfo, listCacheFlowInfo: List[CacheFlowInfo])
class StreamMap extends HashMapModule[StreamMapKey,StreamMapValue]{
  private val logger = Logger.getLogger(this.getClass)

  def updateStreamInfo(streamId: Long, cacheStreamInfo: CacheStreamInfo) = {
    indexMap.get(StreamMapKey(streamId)) match {
      case Some(x) =>
        indexMap.update( StreamMapKey(streamId), StreamMapValue( cacheStreamInfo, x.listCacheFlowInfo) )
      case None =>
        indexMap.put( StreamMapKey(streamId), StreamMapValue(cacheStreamInfo,null) )
    }
  }

  def updateFlowInfo(streamId: Long, listCacheFlowInfo: List[CacheFlowInfo]) = {
    indexMap.get(StreamMapKey(streamId)) match {
      case Some(x) =>
        indexMap.update( StreamMapKey(streamId), StreamMapValue( x.cacheStreamInfo, listCacheFlowInfo ) )
      case None =>
        indexMap.put( StreamMapKey(streamId), StreamMapValue(null,listCacheFlowInfo) )
    }
  }

  def refresh: Unit =
    try {
      RiderResponse.getStreamInfoFromRider
      RiderResponse.getProjectInfoFromRider
      RiderResponse.getFlowInfoFromRider
      RiderResponse.getNamespaceInfoFromRider
      // logger.info("  stream Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"stream cache map refresh failed", ex)
        throw ex
    }

  def getMapHandle = {
    indexMap.map { e =>
      ( e._1.streamId, e._2.cacheStreamInfo.name,
        e._2.cacheStreamInfo, e._2.listCacheFlowInfo
        //JsonUtils.caseClass2json(e._2.cacheStreamInfo),
        //JsonUtils.caseClass2json(e._2.listCacheFlowInfo)
      )
    }.toList
  }
}

case class ApplicationMapKey( applicationId: String)
case class ApplicationMapValue(streamName: String )
class ApplicationMap extends HashMapModule[ApplicationMapKey,ApplicationMapValue] {
  private val logger = Logger.getLogger(this.getClass)
  def refresh: Unit =
    try {
      YarnRMResponse.getActiveAppsInfo()
      logger.info(" Application Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"flow cache map refresh failed", ex)
        throw ex
    }

  def getMapHandle = {
    indexMap.map { e =>
      (e._1.applicationId, e._2.streamName)
    }.toList
  }
}

case class StreamNameMapKey( streamName: String)
case class StreamNameMapValue(streamId: Long, projectId: Long, projectName: String )
class StreamNameMap extends HashMapModule[StreamNameMapKey,StreamNameMapValue] {
  def getMapHandle = {
    indexMap.map { e =>
      (e._1.streamName, e._2.projectId, e._2.streamId, e._2.projectName )
    }.toList
  }
}

case class NamespaceMapkey(namespace: String)
case class NamespaceMapValue(topicName: String)
class NamespaceMap extends  HashMapModule[NamespaceMapkey,NamespaceMapValue]{
  private val logger = Logger.getLogger(this.getClass)

  def refresh: Unit = {
    try {
      RiderResponse.getNamespaceInfoFromRider
    } catch {
      case ex: Exception =>
        logger.error(s"namespace cache map refresh failed", ex)
        throw ex
    }
  }

  def getMapHandle = {
    indexMap.map { e =>
      (e._1.namespace, e._2.topicName)
    }.toList
  }
}

case class StreamFeedbackMapKey(streamId: Long)
case class StreamFeedbackMapValue(hitCount:Long, latestHitDatetime: String, missedCount: Long, latestMissedDatetime: String )
class StreamFeedbackMap extends  HashMapModule[StreamFeedbackMapKey,StreamFeedbackMapValue] {
  private val logger = Logger.getLogger(this.getClass)
  def updateHitCount(streamId: Long, hitCount:Long, latestHitDatetime: String ) = {
    indexMap.get(StreamFeedbackMapKey(streamId)) match {
      case Some(x) =>
        indexMap.update(StreamFeedbackMapKey(streamId), StreamFeedbackMapValue((x.hitCount + hitCount),latestHitDatetime, x.missedCount,x.latestMissedDatetime ))
      case None =>
        indexMap.update(StreamFeedbackMapKey(streamId), StreamFeedbackMapValue(hitCount,latestHitDatetime, 0 , "2000-01-01 00:00:00"))
    }
  }

  def updateMissedCount(streamId: Long, missedCount: Long, latestMissedDatetime: String ) = {
    indexMap.get(StreamFeedbackMapKey(streamId)) match {
      case Some(x) =>
        indexMap.update(StreamFeedbackMapKey(streamId), StreamFeedbackMapValue(x.hitCount, x.latestHitDatetime, (x.missedCount + missedCount), latestMissedDatetime ))
      case None =>
        indexMap.update(StreamFeedbackMapKey(streamId), StreamFeedbackMapValue(0,"2000-01-01 00:00:00", missedCount, latestMissedDatetime))
    }
  }

  def getMapHandle = {
    val a = indexMap.map{e=>
      (e._1.streamId, e._2.hitCount, e._2.latestHitDatetime, e._2.missedCount, e._2.latestMissedDatetime)
    }
    a.toList
  }

  def refresh: Unit = {
    try {
      // getFromDBToStreamFeedbackMap
      logger.info(" Application Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"offset cache map refresh failed", ex)
        throw ex
    }
  }
}

case class OffsetMapkey(streamid: Long, topicName: String, partitionId: Int)
case class OffsetMapValue(offset: Long)
class OffsetMap extends  HashMapModule[OffsetMapkey,OffsetMapValue] {
  private val logger = Logger.getLogger(this.getClass)

  def getMapHandle = {
    indexMap.map { e =>
      (e._1.streamid, e._1.topicName, e._1.partitionId, e._2.offset)
    }.toList
  }

  def refresh: Unit = {
    try {
      //getFromDBToOffsetMap
      logger.info(" Application Map refresh ")
    } catch {
      case ex: Exception =>
        logger.error(s"offset cache map refresh failed", ex)
        throw ex
    }
  }
}

case class ProjectIdMapKey( pProjectId: String)  // "p100001"
case class ProjectIdMapValue( streamIds: List[Long])
class ProjectIdMap extends  HashMapModule[ProjectIdMapKey,ProjectIdMapValue]{
  def getMapHandle = {
    indexMap.map { e =>
      (e._1.pProjectId, e._2.streamIds.toString())
    }.toList
  }

}

