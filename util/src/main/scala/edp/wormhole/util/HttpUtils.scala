package edp.wormhole.util

import java.util

import com.alibaba.fastjson.JSONObject

/**
  * Created by neo_qiang on 2020/5/6.
  */
object HttpUtils {
  def scalaMap2JavaMap(data: Seq[(String, String)]): java.util.Map[String, String] = {
    if (data == null || data.isEmpty) null.asInstanceOf[java.util.Map[String, String]]
    else {
      import scala.collection.JavaConverters._
      collection.mutable.Map(data: _*).asJava
    }
  }

  def dataToJsonString(data: Seq[(String, String)]): String = {
    val json = new JSONObject()
    data.foreach(d => {
      json.put(d._1, d._2)
    })
    json.toJSONString
  }



  def getCommonHeader(): util.Map[String,String] ={
    val headerMap = new java.util.HashMap[String, String]()
    headerMap.put("Content-Type","application/json")
    headerMap
  }
}
