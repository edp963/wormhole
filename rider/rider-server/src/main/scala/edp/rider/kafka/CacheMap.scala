package edp.rider.kafka

import edp.rider.common.RiderLogger
import edp.rider.module._
import slick.jdbc.MySQLProfile.api._

import edp.rider.rest.util.CommonUtils.minTimeOut
import scala.collection.mutable
import scala.concurrent.Await

object CacheMap extends RiderLogger{

  private lazy val modules = new ConfigurationModuleImpl
    with ActorModuleImpl
    with PersistenceModuleImpl
    with BusinessModuleImpl
    with RoutesModuleImpl

  val streamMap=mutable.HashMap.empty[Long,Long]

  def getProjectIdByStreamId(streamId:Long)={
    if(streamMap.contains(streamId)){
       streamMap.get(streamId)
    }else{
      val stream = Await.result(modules.streamDal.findByFilter(stream=>stream.id===streamId),minTimeOut).headOption
      if(stream.nonEmpty){
        streamMap.put(stream.get.id,stream.get.projectId)
        Some(stream.get.projectId)
      }
      else
        None
    }
  }

  def init()={
    Await.result(modules.streamDal.findAll,minTimeOut).filter( stream => !streamMap.contains(stream.id))
      .foreach(stream=>{
         streamMap.put(stream.id,stream.projectId)
      })
  }
}
