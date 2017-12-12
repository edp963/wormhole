package edp.mad

import akka.actor.Props
import akka.stream.ActorMaterializer
import edp.mad.elasticsearch.MadES._
import edp.mad.kafka.{FeedbackConsumer, LogsConsumer}
import edp.mad.module._
import edp.mad.schedule.MadScheduler
import edp.wormhole.haservice.HAServiceManager
import org.apache.log4j.Logger
//import edp.mad.es.ElasticSearch

object MadStarter extends App{

  val modules = ModuleObj.getModule

  implicit val system = modules.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  private val logger = Logger.getLogger(this.getClass)

  logger.info( s" before  ElasticSearch \n")
  madES.initial
  logger.info( s" after ElasticSearch \n")
  // 1 check db connection and table query handler
/*
  if (Await.result(modules.streamDal.findByFilter(_.streamId === 0L), Duration.Inf ).isEmpty)
    logger.info( s" streamDal is null \n")
  else
    logger.info( s" streamDal is not null \n")

  if (Await.result(modules.projectDal.findByFilter(_.projectId === 0L), Duration.Inf ).isEmpty)
    logger.info( s" projectDal is null \n")
  else
    logger.info( s" projectDal is not null \n")

  if (Await.result(modules.flowDal.findByFilter(_.flowId === 0L), Duration.Inf ).isEmpty)
    logger.info( s" flowDal is null \n")
  else
    logger.info( s" flowDal is not null \n")

  if (Await.result(modules.offsetDal.findByFilter(_.id === 0L), Duration.Inf ).isEmpty)
    logger.info( s" offsetDal is null \n")
  else
    logger.info( s" offsetDal is not null \n")

  if (Await.result(modules.applicationMapDal.findByFilter(_.id === 0L), Duration.Inf ).isEmpty)
    logger.info( s" applicationMapDal is null \n")
  else
    logger.info( s" applicationMapDal is not null \n")

  if (Await.result(modules.cacheHitDal.findByFilter(_.id === 0L), Duration.Inf ).isEmpty)
    logger.info( s" cacheHitDal is null \n")
  else
    logger.info( s" cacheHitDal is not null \n")

*/
  // 2 . check the REST API for rider response
  /*
  logger.info( s" before StreamInfos: \n " )
  RiderResponse.getStreamInfoFromRider
  logger.info( s" End StreamInfos: \n " )
  RiderResponse.getProjectInfoFromRider
  logger.info( s" End project infos: \n " )
  RiderResponse.getFlowInfoFromRider
  logger.info( s" End flow infos: \n " )
*/

  // 3. check the map info
  modules.cacheInit

  val feedbackServer = new  HAServiceManager( modules.system, Props(new FeedbackConsumer), "FeedbackConsumer")
  val logsServer = if( modules.madKafka.logsEnable == true ) new HAServiceManager(modules.system, Props(new LogsConsumer), "LogsConsumer") else null

  MadScheduler.start

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      logger.info(s"==========**************** stop \n")
      println(s"==========**************** stop \n")
      feedbackServer.stopManager
      if( logsServer != null )logsServer.stopManager
      system.terminate()
    }
  }))

}