package edp.mad.elasticsearch

import akka.http.scaladsl.model.HttpMethods
import edp.mad.util.HttpClient
import org.apache.log4j.Logger

trait ESIndexModule[D,T]{
  val indexMap = new scala.collection.mutable.HashMap[D, T]()
  private val logger = Logger.getLogger(this.getClass)

  def setIndexMap(indexType: D, indexEntity: T) = {
    if (indexMap.contains(indexType)) {
      indexMap.update(indexType, indexEntity)
    } else {
      indexMap.put(indexType, indexEntity)
    }
  }

  def getEntity(indexType: D):Option[T] = {
    indexMap.get(indexType)
  }

  def getIndexMapKeySet(): Set[D] ={
    indexMap.keySet.toSet
  }

  def existIndex(url: String, user: String, pwd: String, token: String): Boolean ={
    var rc = false
    val existsResponse = HttpClient.syncClient("", url, HttpMethods.GET, user, pwd, token)
    if (existsResponse._1) {
      logger.info(s" Index $url already exists \n")
      rc = true
    }
    rc
  }


  def createIndex( body: String, url: String, user: String, pwd: String, token: String): Boolean = {
    var rc = false
    if ( false == existIndex(url, user, pwd, token) ){
      logger.info(s" To create Index url $url \n")
      val response  = HttpClient.syncClientGetJValue(body,url, HttpMethods.PUT, user, pwd, token)
      if(response._1) {
        rc = true
        logger.info(s" Create index success $url  $body \n")
      }else
        logger.info(s" Failed to Create index $url  $body  \n")
    }
    rc
  }

  def insertDoc(body: String, url: String , user: String, pwd: String, token: String ): Boolean = {
    var rc = false
    val response  = HttpClient.syncClientGetJValue(body,url, HttpMethods.POST, user, pwd, token)
    if(response._1) {
      rc = true
      logger.info(s" insert success $url  $body \n")
    }else {
       logger.info(s" Failed to index $url  $body  \n")
    }
    rc
  }

  def deleteIndex( body: String, url: String, user: String, pwd: String, token: String): Boolean = {
    var rc = false
    if ( true == existIndex(url, user, pwd, token) ){
      val response  = HttpClient.syncClientGetJValue(body,url, HttpMethods.DELETE, user, pwd, token)
      if(response._1) {
        rc = true
        logger.info(s" delete index success $url  $body \n")
      }else {
        logger.info(s" Failed to delete index $url  $body  \n")
      }
    }
    rc
  }

}


