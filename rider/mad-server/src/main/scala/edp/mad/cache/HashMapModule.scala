package edp.mad.cache
import org.apache.log4j.Logger

trait HashMapModule[K,V] {
  val indexMap = new scala.collection.mutable.HashMap[K, V]()

  private val logger = Logger.getLogger(this.getClass)

  def set(key: K, value: V) = {
    if (indexMap.contains(key)) {
      indexMap.update(key, value)
    } else {
      indexMap.put(key, value)
    }
  }

  def get(key: K): Option[V] = {
    indexMap.get(key) match {
      case Some(x) => Option(x)
      case None => null
    }
  }

  def update(key:K, value:V) = {
    indexMap.update( key, value)
  }

  def del(key: K) : Option[V] = {
    indexMap.remove(key) match {
      case Some(x) => Option(x)
      case None => null
    }
  }

  def mapPrint = {
    logger.info(s" Map [${new java.util.Date().toString}] start -----------------------------\n")
    indexMap.foreach{e=>
      logger.info(s" Map [${e._1}] -> [${e._2.toString}] \n ")
    }
    logger.info(s" Map [${new java.util.Date().toString}] end   ------------------------------\n")
  }

}



