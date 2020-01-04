package edp.wormhole.sparkx.swifts.custom.maidian

import java.util
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.util.httpclient.HttpClientService
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class MaidianSchemaTransform extends EdpLogging {
  //param:{"httpUrl":"http://ssss","token":"tmp token","dataFlowId":"1"}
  def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, param: String, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    val ruleMap: mutable.Map[String, JSONArray] = getRules(param)
    logInfo(s"ruleMap.size:${ruleMap.size}")
    //INCLUDE(1,"保留"),EXCLUDE(2,"排除"),FORMAT(3,"格式化/类型转换"),EXPRESSION(4,"表达式"),FILTER(5,"过滤条件");
    var selectSet = mutable.HashSet.empty[String]
    selectSet = doInclude(ruleMap, selectSet)
    selectSet = doExclude(ruleMap, selectSet, df)
    selectSet = doFormat(ruleMap, selectSet)
    selectSet = doExpression(ruleMap, selectSet)

    logInfo(s"selectSet:${selectSet.toString()}")

    val filters = doFilter(ruleMap, df)

    val tmpTableName = "a" + UUID.randomUUID().toString.replaceAll("-", "")
    df.createOrReplaceTempView(tmpTableName)

    var sql = s"select ${selectSet.mkString(",")} from $tmpTableName "
    if (filters != null && filters.nonEmpty) {
      sql += " where "+filters
    }
    logInfo(s"sql:$sql")

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

  def doFilter(ruleMap: mutable.Map[String, JSONArray], df: DataFrame): String = {
    if (ruleMap.contains("5")) {
      val rule: JSONObject = ruleMap("5").getJSONObject(0)
      val ruleConfigJson = rule.getJSONObject("config")
      val filterJson = ruleConfigJson.getJSONObject("filters")
      val tableFieldMap: util.Map[String, DataType] = MaidianCommon.getFieldType(df)
      new ConditionParse().getWhereSqlByNestCondition(filterJson, tableFieldMap)
    }else null
  }

  def doExpression(ruleMap: mutable.Map[String, JSONArray], selectSet: mutable.HashSet[String]): mutable.HashSet[String] = {
    if (ruleMap.contains("4")) {
      val ruleArray: JSONArray = ruleMap("4")
      for (i <- 0 until ruleArray.size()) {
        val rule = ruleArray.getJSONObject(i)
        val ruleConfigJson = rule.getJSONObject("config")
        val expression = ruleConfigJson.getString("expression")
        val aliasName = ruleConfigJson.getString("alias")

        val selected = s"$expression as $aliasName"
        selectSet += selected
      }
    }

    selectSet
  }

  def doFormat(ruleMap: mutable.Map[String, JSONArray], selectSet: mutable.HashSet[String]): mutable.HashSet[String] = {
    if (ruleMap.contains("3")) {
      val ruleArray: JSONArray = ruleMap("3")
      for (i <- 0 until ruleArray.size()) {
        val rule = ruleArray.getJSONObject(i)
        val ruleConfigJson = rule.getJSONObject("config")
        val formatJson = ruleConfigJson.getJSONObject("format")
        val sinkType = formatJson.getString("sinkType")
        val fieldName = ruleConfigJson.getString("field")
        val aliasName = ruleConfigJson.getString("alias")
        val selected = s"cast($fieldName as $sinkType) as $aliasName"
        selectSet += selected
      }
    }

    selectSet
  }

  def doExclude(ruleMap: mutable.Map[String, JSONArray], selectSet: mutable.HashSet[String], df: DataFrame): mutable.HashSet[String] = {
    if (ruleMap.contains("2")) {
      df.schema.foreach(f => {
        selectSet += f.name.toLowerCase
      })
      val rule: JSONObject = ruleMap("2").getJSONObject(0)
      val ruleConfigJson = rule.getJSONObject("config")
      val excludesArray = ruleConfigJson.getJSONArray("excludes")
      for (i <- 0 until excludesArray.size()) {
        val excludeField = excludesArray.get(i).toString.toLowerCase
        selectSet.remove(excludeField)
      }
    }
    selectSet
  }

  def doInclude(ruleMap: mutable.Map[String, JSONArray], selectSet: mutable.HashSet[String]): mutable.HashSet[String] = {
    if (ruleMap.contains("1")) {
      val rule: JSONObject = ruleMap("1").getJSONObject(0)
      val ruleConfigJson = rule.getJSONObject("config")
      val includeArray = ruleConfigJson.getJSONArray("includes")
      for (i <- 0 until includeArray.size()) {
        val includeField = includeArray.get(i).toString
        selectSet += includeField
      }
    }

    selectSet
  }

  def getRules(param: String): mutable.HashMap[String, JSONArray] = {
    val replaceParam = param.replaceAll("\\\\","")
    logInfo(s"replaceParam:$replaceParam")
    val configJson = JSON.parseObject(replaceParam)
    val url = configJson.getString("httpUrl")
    val token = if (configJson.containsKey("token")) configJson.getString("token") else null
    val dataFlowId = configJson.getString("dataFlowId")

    val ruleMap = mutable.HashMap.empty[String, JSONArray]

    val paramMap = new util.HashMap[String, String]
    paramMap.put("dataFlowId", dataFlowId)
    val headerMap = new util.HashMap[String, String]
    headerMap.put("Authorization", token)
    val httpResult = new HttpClientService().doPost(url, headerMap, paramMap)
    logInfo(s"httpResult.getStatus:${httpResult.getStatus},httpResult.getData:${httpResult.getData}")
    if (httpResult.getStatus == 200) {
      val schemaRuleJson: JSONObject = JSON.parseObject(httpResult.getData)
      if (schemaRuleJson.getString("code").equals("200")) {
        val dataJson: JSONObject = schemaRuleJson.getJSONObject("data")
        val ruleArray: JSONArray = dataJson.getJSONArray("list")
        for (i <- 0 until ruleArray.size()) {
          val rule: JSONObject = ruleArray.get(i).asInstanceOf[JSONObject]
          val optType = rule.getString("type")
          if (ruleMap.contains(optType)) {
            ruleMap(optType).add(rule)
          } else {
            val tmpRuleArray = new JSONArray()
            tmpRuleArray.add(rule)
            ruleMap.put(optType, tmpRuleArray)
          }
        }
      }
    }
    ruleMap
  }
}
