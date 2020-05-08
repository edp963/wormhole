package edp.wormhole.dbdriver.rocketmq

import org.apache.rocketmq.common.message.Message

import scala.collection.JavaConversions._
import scala.collection.mutable
class ListSplitter(messages: Seq[Message], sizeLimit: Int = 1000000) extends Iterator[Seq[Message]]{
  private var currIndex = 0

  override def hasNext: Boolean = {
    currIndex < messages.size
  }

  override def next(): Seq[Message] = {
    var nextIndex = currIndex
    var totalSize = 0
    var doNext = true
    while(nextIndex < messages.size && doNext) {
      val message = messages(nextIndex)
      var tmpSize = message.getTopic.length + message.getBody.length
      if(null != message.getProperties) {
        val properties = mutable.HashMap.empty[String, String]
        properties.putAll(message.getProperties)
        properties.foreach(p => {
          tmpSize += p._1.length
          tmpSize += p._2.length
        })
      }
      tmpSize += 20

      if(tmpSize > sizeLimit) {
        if(nextIndex == currIndex) {
          //对于单个的情况，将此消息加入
          nextIndex += 1
        }
        //不将此消息加入
        doNext = false
      } else if(tmpSize + totalSize > sizeLimit) {
        //不将此消息加入
        doNext = false
      } else {
        totalSize += tmpSize
        nextIndex += 1
      }
    }
    val subList = messages.subList(currIndex, nextIndex)
    currIndex = nextIndex
    subList
  }
}
