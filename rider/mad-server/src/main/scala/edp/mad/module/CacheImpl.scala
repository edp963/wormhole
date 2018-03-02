//implement
package edp.mad.module

import edp.mad.cache._
import org.apache.log4j.Logger

trait CacheImpl{
  val configModules = ConfigObj.getModule
  private val logger = Logger.getLogger(this.getClass)
  val streamMap = new StreamImpl
  val applicationMap = new ApplicationImpl
  val namespaceMap = new NamespaceImpl
  val streamNameMap = new StreamNameImpl
  val projectIdMap= new ProjectIdImpl
//  val topicMap= new TopicImpl

  val offsetMap = new OffsetMap
  val streamFeedbackMap = new StreamFeedbackMap
  val alertMap = new AlertMap

  def cacheInit = {
    logger.info(s" performance before streamMap refresh \n")
    streamMap.refresh
    logger.info(s" performance before applicationMap.refresh \n")
    applicationMap.refresh
    logger.info(s" performance before namespaceMap.refresh \n")
    namespaceMap.refresh
    logger.info(s" performance after namespaceMap.refresh \n")
  }

  def cachePrint = {
    streamMap.mapPrint
    applicationMap.mapPrint
    namespaceMap.mapPrint
    streamNameMap.mapPrint
    offsetMap.mapPrint
    streamFeedbackMap.mapPrint
  }

}
