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


package edp.mad.schedule

import akka.actor.{Actor, ActorRef, Props}
import edp.mad.alert.StreamDiagnosis
import edp.mad.elasticsearch.MadES._
import edp.mad.module._
import edp.mad.persistence.entities._
import edp.mad.util.OffsetUtils
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.common.util.DtFormat
import edp.wormhole.common.util.JsonUtils
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object MadScheduler {

  private val logger = Logger.getLogger(this.getClass)
  val modules = ModuleObj.getModule

  def start: Unit = {
    val schedulerActor: ActorRef = modules.system.actorOf(Props[SchedulerActor])
    modules.system.scheduler.schedule(30.seconds, 12.hours, schedulerActor, "maintenance")
    modules.system.scheduler.schedule(30.seconds, 10.minutes, schedulerActor, "analysisAndDiagnosis")
    modules.system.scheduler.schedule(30.seconds, 10.minutes, schedulerActor, "MapPersistence")
    modules.system.scheduler.schedule(30.seconds, 10.minutes, schedulerActor, "CacheRefresh")
  }

  class SchedulerActor extends Actor {
    def receive = {
      case "maintenance" => {
        createEsIndex
        deleteEsIndex
        logger.info(s" Scheduler maintenance task ${new java.util.Date().toString} start")
      }
      case "analysisAndDiagnosis" => {
        logger.info(s" Scheduler maintenance task ${new java.util.Date().toString} start")
      }
      case "MapPersistence" => {
        hashMapPersistence
        madES.indexMaintenance
        logger.info(s" Scheduler maintenance task ${new java.util.Date().toString} start")
      }
      case "CacheRefresh" => {
        logger.info(s" Scheduler maintenance task ${new java.util.Date().toString} start")
        cacheRefresh
      }
      case _ => {}
        logger.info(s"timer ${new java.util.Date().toString}")
    }
  }
  def hashMapPersistence ={
    logger.info(s"--------- StreamFeedbackMap \n ")
    val curTs = yyyyMMddHHmmssToString(currentyyyyMMddHHmmss, DtFormat.TS_DASH_MILLISEC)

    modules.streamFeedbackMap.getMapHandle.foreach{e=>
      logger.info(s"--------- StreamFeedbackMap item ${e} \n")
      modules.cacheEffectDal.updateOrInsert(Seq(CacheEffectEntity(0,e._1, e._2, e._3,e._4,e._5)))
      logger.info(s"---------  \n")
    }

    val offsetList = modules.offsetMap.getMapHandle
    logger.info(s"--------- offsetList item ${offsetList} \n")
    val topicList = offsetList.groupBy(t => (t._1, t._2)).map{e=> (e._1._1,e._1._2, e._2.map(it => it._3 +":" +it._4).sorted.mkString(",")) }
    logger.info(s"--------- getOffsetCacheHandle item ${topicList} \n")
    topicList.foreach{et =>
      //logger.info(s"--------- topicList ${et} \n")
      val partitionNum = OffsetUtils.getPartitionNumber(et._3)
      val offsetEntity = OffsetTableEntity(0, et._1,et._2, partitionNum, et._3, curTs )
      logger.info(s"--------- offset Entity ${offsetEntity} \n")
      modules.offsetSavedDal.updateOrInsert(Seq( offsetEntity))
    }
      //modules.offsetDal.insert()

//    if(modules.madMaintenance.cachePersistence == true && modules.madRedis.enable == false){
//      modules.streamMap.getMapHandle.foreach{e=>
//        logger.info(s"--------- Stream Map item ${e} \n")
//         modules.streamCacheDal.updateOrInsert(Seq(StreamCacheEntity(0,e._1, e._2, "", JsonUtils.caseClass2json(e._3), JsonUtils.caseClass2json(e._4),curTs,curTs)))
//        logger.info(s"---------  \n")
//      }
//
//      modules.applicationMap.getMapHandle.foreach{e=>
//        logger.info(s"--------- Application Map item ${e} \n")
//        modules.applicationCacheDal.updateOrInsert(Seq(ApplicationCacheEntity(0,e._1, e._2, curTs,curTs)))
//        logger.info(s"---------  \n")
//      }
//
//      modules.namespaceMap.getMapHandle.foreach{e=>
//        logger.info(s"--------- Namespace Map item ${e} \n")
//        modules.namespaceCacheDal.updateOrInsert(Seq(NamespaceCacheEntity(0,e._1, e._2, curTs,curTs)))
//        logger.info(s"---------  \n")
//      }
//    }

  }

  def cacheRefresh={
    //  1  refresh
    logger.info(s" ---------  \n")
    modules.streamMap.refresh
    logger.info(s" ---------  \n")
    modules.namespaceMap.refresh
    logger.info(s" ---------  \n")
    modules.applicationMap.refresh
    logger.info(s" ---------  \n")
    modules.streamMap.mapPrint
    StreamDiagnosis.streamStatusDiagnosis()
    logger.info(s" ---------  \n")
    // 2 delete expire map
    if(modules.madRedis.enable == false){
      // delete the streamMap  settings on rider, but the status is not running
      // delete the appllicatoinMap which is not the wormhole stream
    }
  }

  def deleteEsIndex={
    madES.deleteHistoryIndex
  }

  def createEsIndex={
    madES.autoCreateIndexByPattern
  }
}
