package edp.rider.rest.util

import edp.rider.common.RiderConfig
import edp.rider.RiderStarter.modules
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.Await
import edp.rider.rest.util.CommonUtils._
case class RiderDemo(project: String,
                     normalUser: String,
                     zkUrl: String,
                     kafkaUrl: String,
                     sourceTopic: String,
                     sourceTable: String,
                     sinkTopic: String,
                     sinkTable: String,
                     keys: String,
                     streamName: String)

object DemoUtils {

  val riderDemo = RiderDemo("demo", RiderConfig.riderServer.normalUser,
    RiderConfig.consumer.zkUrl, RiderConfig.consumer.brokers,
    "wormhole_demo_source", "wormhole_demo_source_data",
    "wormhole_demo_sink", "wormhole_demo_sink_data",
    "id", "test")

//  def createDemo = {
//    if(Await.result(modules.projectDal.findByFilter(_.name === riderDemo.project), minTimeOut).isEmpty)
//      Await.result(modules.projectDal.insert(Project()))
//  }

}
