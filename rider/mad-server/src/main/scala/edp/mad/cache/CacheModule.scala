package edp.mad.cache

import edp.mad.module.ConfigObj
import org.apache.log4j.Logger

trait CacheModule[K,V]{

  def refresh: Unit ={}

  def set(key:K,value:V) ={}

  def get(key: K):Option[V] ={null}

  def del(key: K) ={}

  def mapPrint: Unit ={}

  def update(key:K,value:V) ={}

  def expire(key: K, seconds: Int) ={}
}

class StreamImpl extends CacheModule[StreamMapKey,StreamMapValue]{
  private val logger = Logger.getLogger(this.getClass)
  private lazy val cache = new StreamCache
  private lazy val map = new StreamMap
  private val modules = ConfigObj.getModule

  def updateProjectInfo(streamId: Long,cacheProjectInfo:CacheProjectInfo) = {
    if(modules.madRedis.enable == true )
      cache.updateProjectInfo(streamId, cacheProjectInfo)
    else
      map.updateProjectInfo(streamId,cacheProjectInfo)
  }

  def updateStreamInfo(streamId: Long, cacheStreamInfo: CacheStreamInfo) = {
    if(modules.madRedis.enable == true )
      cache.updateStreamInfo(streamId,cacheStreamInfo)
    else
      map.updateStreamInfo(streamId,cacheStreamInfo)
  }

  def updateFlowInfo(streamId: Long, listCacheFlowInfo: List[CacheFlowInfo]) = {
    if(modules.madRedis.enable == true )
      cache.updateFlowInfo(streamId,listCacheFlowInfo)
    else
      map.updateFlowInfo(streamId,listCacheFlowInfo)
  }

  override def refresh ={
    if(modules.madRedis.enable == true )
      cache.refresh
    else
      map.refresh
  }

  override def set(key:StreamMapKey,value: StreamMapValue) ={
    if(modules.madRedis.enable == true )
      cache.set(key,value)
    else
      map.set(key,value)
  }

  override def get(key:StreamMapKey) ={
    if(modules.madRedis.enable == true )
      cache.get(key)
    else
      map.get(key)
  }

  override def del(key:StreamMapKey) ={
    if(modules.madRedis.enable == true )
      cache.del(key)
    else
      map.del(key)
  }

  override def mapPrint: Unit ={
    if(modules.madRedis.enable == true ) {
      //streamCache.mapPrint
    }else
      map.mapPrint
  }

  override def update(key:StreamMapKey,value: StreamMapValue) ={
    if(modules.madRedis.enable == true ) {
      //streamCache.update(key,value)
    }else
      map.update(key,value)
  }

  override def expire(key:StreamMapKey, seconds: Int) ={
    if(modules.madRedis.enable == true ) {
      cache.expire(key, seconds)
    }else {
      //map.expire(key, seconds)
    }
  }

  def getMapHandle() ={
    if(modules.madRedis.enable == true ) {
      //cache.expire(key, seconds)
      List()
    }else {
      map.getMapHandle
    }
  }
}

class ApplicationImpl extends CacheModule[ApplicationMapKey,ApplicationMapValue]{
  private val logger = Logger.getLogger(this.getClass)
  private lazy val cache = new ApplicationCache
  private lazy val map = new ApplicationMap
  val modules = ConfigObj.getModule

  override def refresh ={
    if(modules.madRedis.enable == true )
      cache.refresh
    else
      map.refresh
  }

  override def set(key:ApplicationMapKey,value: ApplicationMapValue) ={
    if(modules.madRedis.enable == true )
      cache.set(key,value)
    else
       map.set(key,value)
  }

  override def get(key:ApplicationMapKey) ={
    if(modules.madRedis.enable == true )
      cache.get(key)
    else
      map.get(key)
  }

  override def del(key:ApplicationMapKey) ={
    if(modules.madRedis.enable == true )
      cache.del(key)
    else
      map.del(key)
  }

  override def mapPrint: Unit ={
    if(modules.madRedis.enable == true ) {
      //streamCache.mapPrint
    }else
      map.mapPrint
  }

  override def update(key:ApplicationMapKey,value: ApplicationMapValue) ={
    if(modules.madRedis.enable == true ) {
      //streamCache.update(key,value)
    }else
      map.update(key,value)
  }

  override def expire(key:ApplicationMapKey, seconds: Int) ={
    if(modules.madRedis.enable == true ) {
      cache.expire(key, seconds)
    }else {
      //streamMap.expire(key, seconds)
    }
  }

  def getMapHandle() ={
    if(modules.madRedis.enable == true ) {
      //cache.expire(key, seconds)
      List()
    }else {
      map.getMapHandle
    }
  }
}

class NamespaceImpl extends CacheModule[NamespaceMapkey,NamespaceMapValue]{
  private val logger = Logger.getLogger(this.getClass)
  private lazy val cache = new NamespaceCache
  private lazy val map = new NamespaceMap
  val modules = ConfigObj.getModule

  override def refresh ={
    if(modules.madRedis.enable == true )
      cache.refresh
    else
      map.refresh
  }

  override def set(key:NamespaceMapkey,value: NamespaceMapValue) ={
    if(modules.madRedis.enable == true )
      cache.set(key,value)
    else
      map.set(key,value)
  }

  override def get(key:NamespaceMapkey) ={
    if(modules.madRedis.enable == true )
      cache.get(key)
    else
      map.get(key)
  }

  override def del(key:NamespaceMapkey) ={
    if(modules.madRedis.enable == true )
      cache.del(key)
    else
      map.del(key)
  }

  override def mapPrint: Unit ={
    if(modules.madRedis.enable == true ) {
      //streamCache.mapPrint
    }else
      map.mapPrint
  }

  override def update(key:NamespaceMapkey,value: NamespaceMapValue) ={
    if(modules.madRedis.enable == true ) {
      //streamCache.update(key,value)
    }else
      map.update(key,value)
  }

  override def expire(key:NamespaceMapkey, seconds: Int) ={
    if(modules.madRedis.enable == true ) {
      cache.expire(key, seconds)
    }else {
      //streamMap.expire(key, seconds)
    }
  }
  def getMapHandle() ={
    if(modules.madRedis.enable == true ) {
      //cache.expire(key, seconds)
      List()
    }else {
      map.getMapHandle
    }
  }
}

class StreamNameImpl extends CacheModule[StreamNameMapKey,StreamNameMapValue]{
  private val logger = Logger.getLogger(this.getClass)
  private lazy val cache = new StreamNameCache
  private lazy val map = new StreamNameMap
  val modules = ConfigObj.getModule

  override def set(key:StreamNameMapKey,value: StreamNameMapValue) ={
    if(modules.madRedis.enable == true )
      cache.set(key,value)
    else
      map.set(key,value)
  }

  override def get(key:StreamNameMapKey) ={
    if(modules.madRedis.enable == true )
      cache.get(key)
    else
      map.get(key)
  }

  override def del(key:StreamNameMapKey) ={
    if(modules.madRedis.enable == true )
      cache.del(key)
    else
      map.del(key)
  }

  override def mapPrint: Unit ={
    if(modules.madRedis.enable == true ) {
      //streamCache.mapPrint
    }else
      map.mapPrint
  }

  override def update(key:StreamNameMapKey,value: StreamNameMapValue) ={
    if(modules.madRedis.enable == true ) {
      //streamCache.update(key,value)
    }else
      map.update(key,value)
  }

  override def expire(key:StreamNameMapKey, seconds: Int) ={
    if(modules.madRedis.enable == true ) {
      cache.expire(key, seconds)
    }else {
      //streamMap.expire(key, seconds)
    }
  }

  def getMapHandle() ={
    if(modules.madRedis.enable == true ) {
      //cache.expire(key, seconds)
      List()
    }else {
      map.getMapHandle
    }
  }
}

class ProjectIdImpl extends CacheModule[ProjectIdMapKey,ProjectIdMapValue]{
  private val logger = Logger.getLogger(this.getClass)
  private lazy val cache = new ProjectIdCache
  private lazy val map = new ProjectIdMap
  val modules = ConfigObj.getModule

  override def set(key:ProjectIdMapKey,value: ProjectIdMapValue) ={
    if(modules.madRedis.enable == true )
      cache.set(key,value)
    else
      map.set(key,value)
  }

  override def get(key:ProjectIdMapKey) ={
    if(modules.madRedis.enable == true )
      cache.get(key)
    else
      map.get(key)
  }

  override def del(key:ProjectIdMapKey) ={
    if(modules.madRedis.enable == true )
      cache.del(key)
    else
      map.del(key)
  }

  override def mapPrint: Unit ={
    if(modules.madRedis.enable == true ) {
      //streamCache.mapPrint
    }else
      map.mapPrint
  }

  override def update(key:ProjectIdMapKey,value: ProjectIdMapValue) ={
    if(modules.madRedis.enable == true ) {
      //streamCache.update(key,value)
    }else
      map.update(key,value)
  }

  override def expire(key:ProjectIdMapKey, seconds: Int) ={
    if(modules.madRedis.enable == true ) {
      cache.expire(key, seconds)
    }else {
      //streamMap.expire(key, seconds)
    }
  }

  def getMapHandle() ={
    if(modules.madRedis.enable == true ) {
      //cache.expire(key, seconds)
      List()
    }else {
      map.getMapHandle
    }
  }
}