//implement
package edp.mad.module

import edp.mad.cache._

trait CacheImpl{
  val configModules = ConfigObj.getModule

  val streamMap = new StreamImpl
  val applicationMap = new ApplicationImpl
  val namespaceMap = new NamespaceImpl
  val streamNameMap = new StreamNameImpl
  val projectIdMap= new ProjectIdImpl

  val offsetMap = new OffsetMap
  val streamFeedbackMap = new StreamFeedbackMap

  def cacheInit = {
    streamMap.refresh
    applicationMap.refresh
    namespaceMap.refresh
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
