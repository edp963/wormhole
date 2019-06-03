/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


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
