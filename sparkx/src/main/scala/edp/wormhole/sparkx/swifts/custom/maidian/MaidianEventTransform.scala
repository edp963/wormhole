package edp.wormhole.sparkx.swifts.custom.maidian

import java.util
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.util.httpclient.HttpClientService
import org.apache.http.entity.ContentType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MaidianEventTransform extends EdpLogging {
  def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, param: String, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    val ruleList: mutable.Seq[JSONObject] = getRules(param)

    val sourceFieldList = getSourceFields(df)
    val fieldTypeMap: util.HashMap[String, DataType] = MaidianCommon.getFieldType(df)
    val conditionSubSelect: mutable.Seq[(String, JSONObject)] = getConditionSubSelect(fieldTypeMap, ruleList)

    val subSqlList = ListBuffer.empty[String]
    val sinksConfig: JSONObject = conditionSubSelect.head._2

    val sinkKeySet = sinksConfig.keySet()
    val sinkFieldNameSet: Array[String] = new Array[String](sinkKeySet.size())
    sinksConfig.keySet().toArray(sinkFieldNameSet)
    for( i <- sinkFieldNameSet.indices){
      val sinkFieldName = sinkFieldNameSet(i)
      val subSqlSeq: mutable.Seq[String] = conditionSubSelect.map(conditionAndSink=> {
        val condition = conditionAndSink._1
        val sinksConfig = conditionAndSink._2
        val sinkFieldValue = sinksConfig.getString(sinkFieldName)
        s" when ($condition) then '$sinkFieldValue' "
      })
      val subSql = s"case ${subSqlSeq.mkString(" ")} else 'unknown' end as $sinkFieldName  "
      subSqlList += subSql
    }

    val tmpTableName = "a" + UUID.randomUUID().toString.replaceAll("-", "")

    val sql = s"select ${sourceFieldList.mkString(",")}, ${subSqlList.mkString(",")} from $tmpTableName"

    logInfo(s"sql:$sql")

    df.createOrReplaceTempView(tmpTableName)
    try {
      session.sql(sql)
    } catch {
      case e: Throwable =>
        logError("", e)
        throw e
    } finally {
      session.sqlContext.dropTempTable(tmpTableName)
    }
  }

  def getConditionSubSelect(fieldTypeMap: util.Map[String, DataType], ruleList: mutable.Seq[JSONObject]): ListBuffer[(String, JSONObject)] = {
    val subSelectList = ListBuffer.empty[(String, JSONObject)]
    val cp = new ConditionParse()
    ruleList.foreach(rule => {
      subSelectList += ((cp.getWhereSqlByNestCondition(rule.getJSONObject("condition"), fieldTypeMap), rule.getJSONObject("sinks")))
    })
    subSelectList
  }

  def getSourceFields(df: DataFrame): Seq[String] = {
    df.schema.map(f => f.name)
  }

  def getRules(param: String): mutable.ListBuffer[JSONObject] = {
    val replaceParam = param.replaceAll("\\\\","")
    logInfo(s"replaceParam:$replaceParam")

    val configJson = JSON.parseObject(replaceParam)
    val url = configJson.getString("httpUrl")
    val token = if (configJson.containsKey("token")) configJson.getString("token") else null
    val dataFlowId = configJson.getString("dataFlowId")

    val ruleList = mutable.ListBuffer.empty[JSONObject]

//    val paramMap = new util.HashMap[String, String]
//    paramMap.put("dataFlowId", dataFlowId)
    val headerMap = new util.HashMap[String, String]
    headerMap.put("Authorization", token)
    val contentType =  "application/json"
    headerMap.put("Content-Type",contentType)
    val paramJson = new JSONObject()
    paramJson.put("dataFlowId",dataFlowId)
    paramJson.put("config","")
    val httpResult = new HttpClientService().doPost(url, headerMap, paramJson.toJSONString, ContentType.APPLICATION_JSON)
    logInfo(s"httpResult.getStatus:${httpResult.getStatus},httpResult.getData:${httpResult.getData}")
    if (httpResult.getStatus == 200) {
      val schemaRuleJson: JSONObject = JSON.parseObject(httpResult.getData.replaceAll("\uFEFF",""))
      if (schemaRuleJson.getString("code").equals("200")) {
        val dataJson: JSONObject = schemaRuleJson.getJSONObject("data")
        val ruleArray: JSONArray = dataJson.getJSONArray("list")
        for (i <- 0 until ruleArray.size()) {
          val rule: JSONObject = ruleArray.getJSONObject(i)
          val ruleConfig = rule.getJSONObject("config")
          ruleList += ruleConfig
        }
      }
    }
    ruleList
  }
}
