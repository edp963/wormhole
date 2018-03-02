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

  madES.initial

  logger.info( s" performance before cacheInit \n")
  modules.cacheInit
  logger.info( s" performance after cacheInit \n")

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