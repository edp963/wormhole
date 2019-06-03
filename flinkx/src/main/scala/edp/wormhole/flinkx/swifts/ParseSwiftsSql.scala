package edp.wormhole.flinkx.swifts

/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


import com.alibaba.fastjson.JSONObject
import edp.wormhole.swifts.{ConnectionMemoryStorage, ParseSwiftsSqlInternal, SqlOptType}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.KVConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.log4j.Logger

class ParseSwiftsSql(sqlStr: String, sourceNamespace: String, sinkNamespace: String) {
  val logger: Logger = Logger.getLogger(this.getClass)

  def parse(dataType: String, sourceSchemaFieldSet: collection.Set[String]): Option[Array[SwiftsSql]] = {
    val pushDownSqlBeginIndex = 12
    if (null != sqlStr && sqlStr.trim.nonEmpty) {
      val sqlArray = sqlStr.trim.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ").split(";").map(sql => {
        val trimSql = sql.trim
        if (trimSql.startsWith(SqlOptType.PUSHDOWN_SQL.toString)) trimSql.substring(pushDownSqlBeginIndex).trim
        else trimSql
      })
      val swiftsSqlArr = getSwiftsSql(sqlArray, dataType, sourceSchemaFieldSet) //sourcenamespace is rule
      swiftsSqlArr
    } else {
      None
    }
  }

  private def getSwiftsSql(sqlArray: Array[String], dataType: String, sourceSchemaFieldSet: collection.Set[String]): Option[Array[SwiftsSql]] = {
    val swiftsSqlList = Some(sqlArray.map(sqlStrEle => {
      val sqlStrEleTrim = sqlStrEle.trim + " " //to fix no where clause bug, e.g select a, b from table;
      logger.info("sqlStrEle:::" + sqlStrEleTrim)
      if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.JOIN.toString) || sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.INNER_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.LEFT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.LEFT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.RIGHT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.RIGHT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.CEP.toString)) {
        ParseSwiftsSqlInternal.getCEP(SqlOptType.CEP, sqlStrEleTrim)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.FLINK_SQL.toString)) {
        ParseSwiftsSqlInternal.getFlinkSql(sqlStrEleTrim, dataType, sourceNamespace,sourceSchemaFieldSet)
      } else {
        logger.info("optType:" + sqlStrEleTrim + " is not supported")
        throw new Exception("wrong operation data type:" + sqlStrEleTrim)
      }
    })
    )
    swiftsSqlList
  }

  def registerConnections(swifts: JSONObject): Unit = {
    val pushDownConnection = if (swifts.containsKey("pushdown_connection") && swifts.getString("pushdown_connection").trim.nonEmpty && swifts.getJSONArray("pushdown_connection").size > 0) swifts.getJSONArray("pushdown_connection") else null
    if (pushDownConnection != null) {
      val connectionListSize = pushDownConnection.size()
      for (i <- 0 until connectionListSize) {
        val jsonObj = pushDownConnection.getJSONObject(i)
        val name_space = jsonObj.getString("name_space").trim.toLowerCase
        val jdbc_url = jsonObj.getString("jdbc_url")
        val username = if (jsonObj.containsKey("username")) Some(jsonObj.getString("username")) else None
        val password = if (jsonObj.containsKey("password")) Some(jsonObj.getString("password")) else None
        val parameters = if (jsonObj.containsKey("connection_config") && jsonObj.getString("connection_config").trim.nonEmpty) {
          logger.info("connection_config:" + jsonObj.getString("connection_config"))
          Some(JsonUtils.json2caseClass[Seq[KVConfig]](jsonObj.getString("connection_config")))
        } else {
          logger.info("not contains connection_config")
          None
        }
        ConnectionMemoryStorage.registerDataStoreConnectionsMap(name_space, jdbc_url, username, password, parameters)
      }
    }
  }

}
