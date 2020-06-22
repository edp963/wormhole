package edp.wormhole.sinks.httpsink

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsNamespace
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.httpclient.{HttpClientService, HttpResult}
import edp.wormhole.util.HttpUtils.{scalaMap2JavaMap,dataToJsonString}
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.log4j.Logger


class Data2HttpSink extends SinkProcessor {

  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {

    logger.info(s"process data size ${tupleList.length}")

    var errorCount = 0

    val sinkSchemaMap = getSinkSchemaMap(sinkProcessConfig.sinkOutput, schemaMap)

    val httpUrl = getHttpUrl(sinkNamespace, connectionConfig.connectionUrl)

    val (methodType, urlParams, tranformType) = getMethodTypeUrlParamsTransformType(sinkProcessConfig.specialConfig)

    val httpService = new HttpClientService()

    val headerMap = getHttpHeader(connectionConfig)

    val isHttps = checkSSL(connectionConfig)

    tupleList.foreach(tuple => {
      try {
        val params = getHttpParams(tuple, sinkSchemaMap)
        val httpParamsUrl = if (urlParams == null) {
          httpUrl
        } else {
          val finalUrl = httpUrl + replaceParams(tuple, schemaMap, urlParams)
          logger.info("finalUrl:" + finalUrl)
          finalUrl
        }

        val hr = methodType match {
          case "get" => httpService.doGetCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), scalaMap2JavaMap(params), isHttps)
          case "post" =>
            if (tranformType == "form") httpService.doPostCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), scalaMap2JavaMap(params), isHttps)
            else httpService.doPostCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), dataToJsonString(params), ContentType.APPLICATION_JSON, isHttps)
          case "put" =>
            if (tranformType == "form") httpService.doPutCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), scalaMap2JavaMap(params), isHttps)
            else httpService.doPutCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), dataToJsonString(params), ContentType.APPLICATION_JSON, isHttps)
          case "delete" => httpService.doDeleteCommon(httpParamsUrl, scalaMap2JavaMap(headerMap), scalaMap2JavaMap(params), isHttps)
          case _ => null.asInstanceOf[HttpResult]
        }

        if (hr == null || hr.getStatus > HttpStatus.SC_NO_CONTENT || hr.getStatus < HttpStatus.SC_OK) {
          logger.error(s"sink reponse error, status=${hr.getStatus}, response=${hr.getData}, data=$tuple")
          errorCount += 1
        }
      } catch {
        case e: Throwable =>
          logger.error(s"sink error:$tuple", e)
          e.printStackTrace()
          errorCount += 1
      }
    })

    if (errorCount > 0)
      throw new Exception(s"http sink $httpUrl has $errorCount errors")

  }

  def replaceParams(tuple: Seq[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], urlParams: String): String = {
    var tmpUrl = urlParams.toLowerCase
    while (tmpUrl.contains("${")) {
      val startIndex = tmpUrl.indexOf("${")
      val endIndex = tmpUrl.indexOf("}")
      val fieldName = tmpUrl.substring(startIndex + 2, endIndex)
      val fieldValue = tuple(schemaMap(fieldName)._1)
      val part1 = tmpUrl.substring(0, startIndex)
      val part2 = tmpUrl.substring(endIndex + 1)
      tmpUrl = part1 + fieldValue + part2
    }
    tmpUrl
  }

  def getSinkSchemaMap(sinkOutput: String, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): collection.Map[String, (Int, UmsFieldType, Boolean)] = {
    if (sinkOutput == null || sinkOutput.isEmpty) schemaMap
    else {
      val sinkFields = sinkOutput.toLowerCase.split(",").map(field => (field, field)).toMap
      schemaMap.filter(fs => sinkFields.contains(fs._1))
    }
  }

  def checkSSL(connectionConfig: ConnectionConfig): Boolean = {
    connectionConfig.connectionUrl.toLowerCase.startsWith("https")
  }

  def getHttpHeader(connectionConfig: ConnectionConfig): Seq[(String, String)] = {
    if (connectionConfig.parameters.nonEmpty) connectionConfig.parameters.get.map(e => {
      (e.key, e.value)
    }) else null.asInstanceOf[Seq[(String, String)]]
  }

  def getHttpUrl(sinkNamespace: String, connectionUrl: String): String = {
    val splitNs = UmsNamespace(sinkNamespace)
    s"$connectionUrl/${splitNs.database}/${splitNs.table}/"
  }

  def getMethodTypeUrlParamsTransformType(specialConfig: Option[String]): (String, String, String) = {
    val specialConfigJson: JSONObject = if (specialConfig.isEmpty || specialConfig.get == null) null else JSON.parseObject(specialConfig.get)
    val methodType = if (specialConfigJson != null && specialConfigJson.containsKey("method_type"))
      specialConfigJson.getString("method_type")
    else "get"

    val urlParams = if (specialConfigJson != null && specialConfigJson.containsKey("url_params"))
      specialConfigJson.getString("url_params")
    else null.asInstanceOf[String]

    val transformType = if (specialConfigJson != null && specialConfigJson.containsKey("transform_type"))
      specialConfigJson.getString("transform_type")
    else "body"

    (methodType, urlParams, transformType)
  }

  def getHttpParams(tuple: Seq[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): Seq[(String, String)] = {
    if (tuple == null || tuple.isEmpty || schemaMap == null || schemaMap.isEmpty) null.asInstanceOf[Seq[(String, String)]]
    else schemaMap.map { case (k, v) =>
      (k, tuple(v._1))
    }.toSeq
  }

  /*def scalaMap2JavaMap(data: Seq[(String, String)]): java.util.Map[String, String] = {
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
  }*/

}
