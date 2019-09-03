package edp.wormhole.flinkx.util

import scala.collection.mutable

object FlinkKafkaProducer {

 // private var messageCount
 // private val messageQueue=mutable.ListBuffer[(String,Int,String,String,String)]

  def sendMessageForFlink(topic: String, partition: Int = 0, message: String, key: Option[String], brokers: String):Any={
//      if(Seq(messageQueue).size>=messageCount){
//        for(ele <- messageQueue){
//          sendMessage(ele._1,ele._2,ele._3,ele._4,ele._5)
//        }
//
//      }

  }

  def setMessageCount(messageCount:Int):Unit={
      // this.messageCount=messageCount
  }
}
