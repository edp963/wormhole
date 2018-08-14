package edp.wormhole.swifts

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


import java.sql.ResultSetMetaData

import com.alibaba.fastjson.JSONObject
import edp.wormhole.common.{ConnectionConfig, KVConfig}
import edp.wormhole.common.db.DbConnection
import edp.wormhole.common.util.JsonUtils
import edp.wormhole.swifts.SqlOptType.SqlOptType
import edp.wormhole.ums.{UmsDataSystem, UmsSysField}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ParseSwiftsSql(sqlStr: String, sourceNamespace: String) {
  val logger: Logger = Logger.getLogger(this.getClass)

  def parse(dataType: String, dataStoreConnectionsMap: Map[String, ConnectionConfig]): Option[Array[SwiftsSql]] = {
    val pushDownSqlBeginIndex = 12
    if (null != sqlStr && sqlStr.trim.nonEmpty) {
      val sqlArray = sqlStr.trim.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ").split(";").map(sql => {
        val trimSql = sql.trim
        if (trimSql.startsWith(SqlOptType.PUSHDOWN_SQL.toString)) trimSql.substring(pushDownSqlBeginIndex).trim
        else trimSql
      })
      val swiftsSqlArr = getSwiftsSql(sqlArray, dataType, dataStoreConnectionsMap) //sourcenamespace is rule
      swiftsSqlArr
    } else {
      None
    }
  }

  private def getSwiftsSql(sqlArray: Array[String], dataType: String, dataStoreConnectionsMap: Map[String, ConnectionConfig]): Option[Array[SwiftsSql]] = {
    val swiftsSqlList = Some(sqlArray.map(sqlStrEle => {
      val sqlStrEleTrim = sqlStrEle.trim + " " //to fix no where clause bug, e.g select a, b from table;
      logger.info("sqlStrEle:::" + sqlStrEleTrim)
      if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.JOIN.toString) || sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.INNER_JOIN.toString)) {
        getJoin(SqlOptType.JOIN, sqlStrEleTrim, dataStoreConnectionsMap)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.LEFT_JOIN.toString)) {
        getJoin(SqlOptType.LEFT_JOIN, sqlStrEleTrim, dataStoreConnectionsMap)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.RIGHT_JOIN.toString)) {
        getJoin(SqlOptType.RIGHT_JOIN, sqlStrEleTrim, dataStoreConnectionsMap)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.CEP.toString)) {
        getCEP(SqlOptType.CEP, sqlStrEleTrim)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.FLINK_SQL.toString)) {
        getFlinkSql(sqlStrEleTrim, dataType)
      } else {
        logger.info("optType:" + sqlStrEleTrim + " is not supported")
        throw new Exception("wong operation data type:" + sqlStrEleTrim)
      }
    })
    )
    swiftsSqlList
  }


  private def getCEP(optType: SqlOptType, sqlStrEle: String) = {
    val patternBeginIndex = 5
    val patternList = sqlStrEle.substring(patternBeginIndex)
    println(patternList + "--------pattern list")
    SwiftsSql(SqlOptType.CEP, None, patternList, None, None, None, None)
  }


  private def getJoin(optType: SqlOptType, sqlStrEle: String, dataStoreConnectionsMap: Map[String, ConnectionConfig]) = {
    val joinNamespace = sqlStrEle.substring(sqlStrEle.toLowerCase.indexOf(" with ") + 5, sqlStrEle.indexOf("=")).trim.toLowerCase
    val sqlStr = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1)
    val (sql, lookupFields, valuesFields) = if (joinNamespace.startsWith(UmsDataSystem.HBASE.toString) || joinNamespace.startsWith(UmsDataSystem.REDIS.toString))
      getFieldsAndSqlFromHbaseOrRedis(sqlStr, joinNamespace)
    else getFieldsAndSql(sqlStr, joinNamespace)
    syntaxCheck(sql, lookupFields)
    val fieldsStr = getFieldsWithType(joinNamespace, sql, dataStoreConnectionsMap)
    val lookupFieldsAlias = getAliasLookupFields(sql, lookupFields)
    SwiftsSql(optType, fieldsStr, sql, Some(joinNamespace), Some(valuesFields), Some(lookupFields), Some(lookupFieldsAlias))
  }

  private def getFieldsAndSqlFromHbaseOrRedis(userSqlStr: String, joinNamespace: String): (String, Array[String], Array[String]) = {
    val joinByPosition = userSqlStr.toLowerCase.indexOf(" joinby ") + 8
    val joinByFields = userSqlStr.substring(joinByPosition)
    (userSqlStr, Array.empty[String], Array[String](joinByFields))

  }

  private def getFieldsAndSql(userSqlStr: String, joinNamespace: String): (String, Array[String], Array[String]) = {
    val sqlStr: String = getJoinSql(userSqlStr)
    val namespaceArray = sourceNamespace.split("\\.")
    val fourDigitNamespace = (for (i <- 0 until 4) yield namespaceArray(i)).mkString(".")
    val joinPosition = sqlStr.toLowerCase.indexOf(fourDigitNamespace)
    val inPosition = sqlStr.toLowerCase.lastIndexOf(" in ", joinPosition)
    val valueLeftPosition = sqlStr.indexOf("(", inPosition)
    val valueRightPosition = sqlStr.indexOf(")", inPosition)
    val valueFieldsStr = sqlStr.substring(valueLeftPosition + 1, valueRightPosition).toLowerCase
    val valuesFields = if (valueFieldsStr.indexOf(sourceNamespace) > -1) {
      valueFieldsStr.trim.replace(sourceNamespace + ".", "").split(",").map(_.trim)
    } else {
      valueFieldsStr.trim.replaceAll(fourDigitNamespace + "\\.", "").split(",").map(_.trim)
    }

    var tmpPosition = inPosition - 1
    var tmpChar = sqlStr.charAt(tmpPosition)
    while (tmpChar == ' ') {
      tmpPosition = tmpPosition - 1
      tmpChar = sqlStr.charAt(tmpPosition)
    }
    val joinRightPosition = tmpPosition + 1
    if (tmpChar == ')') {
      tmpPosition = tmpPosition - 1
      tmpChar = sqlStr.charAt(tmpPosition)
      while (tmpChar != '(') {
        tmpPosition = tmpPosition - 1
        tmpChar = sqlStr.charAt(tmpPosition)
      }
    } else {
      while (tmpChar != ' ') {
        tmpPosition = tmpPosition - 1
        tmpChar = sqlStr.charAt(tmpPosition)
      }
    }
    val joinLeftPosition = tmpPosition
    val joinFieldsStr = sqlStr.substring(joinLeftPosition + 1, joinRightPosition).replace(")", "").trim.toLowerCase
    val joinFields = joinFieldsStr.trim.split(",").map(_.trim)
    val sql =
      sqlStr.substring(0, joinLeftPosition) + " " + SwiftsConstants.REPLACE_STRING_INSQL + " " + sqlStr.substring(valueRightPosition + 1)
    logger.info(sql)
    (sql, joinFields, valuesFields)
  }


  private def getFieldsWithType(joinNamespace: String, sql: String, dataStoreConnectionsMap: Map[String, ConnectionConfig]) = {
    val lookupNamespacesArr = joinNamespace.split(",").map(_.trim)
    val connectionConfig = SwiftsConfMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespacesArr(0))
    val fieldsStr = if (joinNamespace.startsWith(UmsDataSystem.HBASE.toString) || joinNamespace.startsWith(UmsDataSystem.REDIS.toString) || joinNamespace.startsWith(UmsDataSystem.KUDU.toString)) {
      Some(getFieldsFromHbaseOrRedisOrKudu(sql))
    } else {
      Some(getSchema(sql, connectionConfig))
    }
    fieldsStr
  }


  private def syntaxCheck(sql: String, lookupFields: Array[String]): Unit = {
    val lowerCaseSql = sql.toLowerCase.trim
    val groupSplit = lowerCaseSql.split(" group by ")
    if (groupSplit.length > 2) throw new Exception("lookup sqlStr can only contains one 'group by'")
    val groupLeftPosition = lowerCaseSql.indexOf(" group by ")
    if (groupLeftPosition > -1) {
      val left = groupLeftPosition + 10
      val subSql = lowerCaseSql.substring(left)
      val subArray = subSql.split(",")
      val groupByMap = new scala.collection.mutable.HashMap[String, Boolean]
      var flag = true
      subArray.foreach(tmp => {
        if (flag) {
          if (tmp.trim.indexOf(" ") > -1) {
            groupByMap(tmp.trim.split(" ")(0)) = true
            flag = false
          } else {
            groupByMap(tmp.trim) = true
          }
        }
      })
      lookupFields.foreach(field => {
        if (!groupByMap.contains(field)) throw new Exception("group by fields must contains lookup fields(where in fields) ")
      })
    }
  }


  private def getAliasLookupFields(sql: String, lookupFields: Array[String]) = {
    val selectFieldsList = getIndependentFieldsFromSql(sql)
    val selectFieldsSet = selectFieldsList.map(fieldName => {
      val tmpField = fieldName.replaceAll("\\s", "")
      if (tmpField.toLowerCase.startsWith("distinct(")) {
        tmpField.substring(9, tmpField.indexOf(")"))
      } else
        fieldName.split(" ")(0).toLowerCase
    }).toSet
    lookupFields.foreach(field => {
      val name = field.split(" ")(0).toLowerCase
      if (!selectFieldsSet.contains(name)) throw new Exception("select fields must contains lookup fields(where in fields)  ")
    })
    resetLookupTableFields(lookupFields, selectFieldsList)
  }

  private def getIndependentFieldsFromSql(sql: String): List[String] = {
    val selectFieldsList: ListBuffer[String] = new ListBuffer[String]
    val selectFieldsArray = sql.substring(7, sql.toLowerCase.indexOf(" from ")).split(",")
    var tmpFields: String = ""
    var i: Int = 0
    while (i < selectFieldsArray.length) {
      if (tmpFields != "") tmpFields = tmpFields + "," + selectFieldsArray(i)
      else tmpFields = selectFieldsArray(i)
      val left = tmpFields.count(_ == '(')
      val right = tmpFields.count(_ == ')')
      if (left == right) {
        val field = tmpFields.trim
        if (!field.isEmpty) {
          selectFieldsList.append(field)
        }
        tmpFields = ""
      }
      i += 1
    }
    selectFieldsList.toList
  }


  private def resetLookupTableFields(lookupTableFields: Array[String], selectFieldsList: List[String]): Array[String] = {
    val lookupTableFieldsSet = lookupTableFields.map(fieldName => fieldName.trim.toLowerCase).toSet
    val selectFieldsMap = new mutable.HashMap[String, String]
    selectFieldsList.foreach(select => {
      if (select.trim.indexOf(" ") > -1) {
        val s = select.trim.split(" ")
        val tmpField = select.replaceAll("\\s", "")
        val originalName = if (tmpField.toLowerCase.startsWith("distinct(")) {
          tmpField.substring(9, tmpField.indexOf(")"))
        } else {
          s(0).toLowerCase
        }
        if (lookupTableFieldsSet.contains(originalName))
          selectFieldsMap(originalName) = s(s.length - 1).trim.toLowerCase
        else
          selectFieldsMap(originalName) = s(0).trim.toLowerCase
      }
    })
    lookupTableFields.map(field => {
      if (selectFieldsMap.contains(field)) {
        selectFieldsMap(field)
      } else {
        field
      }
    })
  }


  private def getFieldsFromHbaseOrRedisOrKudu(sqlStrEle: String): String = {
    val selectFieldFrom = sqlStrEle.indexOf("select ") + 7
    val selectFieldEnd = sqlStrEle.toLowerCase.indexOf(" from ")
    sqlStrEle.substring(selectFieldFrom, selectFieldEnd) //.split(",").map(_.trim)
  }

  private def getSchema(sql: String, connectionConfig: ConnectionConfig): String = {
    var testSql = sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
    if (connectionConfig.connectionUrl.toLowerCase.contains("cassandra")) {
      val index = sql.toLowerCase.indexOf(" where ")
      testSql = sql.substring(0, index) + " limit 1;"
    }
    val conn = DbConnection.getConnection(connectionConfig)
    val statement = conn.createStatement()
    logger.info(testSql)
    val rs = statement.executeQuery(testSql)
    val schema: ResultSetMetaData = rs.getMetaData
    val columnCount = schema.getColumnCount
    val fieldSchema = StringBuilder.newBuilder
    for (i <- 0 until columnCount) {
      val columnName = schema.getColumnLabel(i + 1).toLowerCase
      val columnType: String = schema.getColumnTypeName(i + 1)
      fieldSchema ++= columnName
      fieldSchema ++= ":"
      fieldSchema ++= columnType
      if (i != columnCount - 1)
        fieldSchema ++= ","
    }
    DbConnection.shutdownConnection(connectionConfig.connectionUrl, connectionConfig.username.orNull)
    fieldSchema.toString()

  }


  private def getJoinSql(sqlStr: String): String = {
    val suffix = sqlStr.substring(sqlStr.toLowerCase.indexOf(" from "))
    val fieldsList = getIndependentFieldsFromSql(sqlStr)
    val result = fieldsList.map(field => {
      val arr = field.split("\\s+")
      val size = arr.size
      if (size == 1 || arr(size - 2).toLowerCase().equals("as")) {
        field
      } else {
        var str = ""
        for (i <- 0 to size - 2)
          str = str + arr(i) + " "
        str + "as " + arr(size - 1)
      }
    })
    "select " + result.mkString(",") + suffix
  }


  def getFlinkSql(sqlStrEle: String, dataType: String): SwiftsSql = {
    val tableName = sourceNamespace.split("\\.")(3)
    var sql = "select "
    val selectFields = sqlStrEle
      .substring(sqlStrEle.toLowerCase.indexOf("select ") + 7, sqlStrEle.toLowerCase.indexOf(" from "))
      .toLowerCase.split(",")
      .map(field => {
        (field.trim.split(" ").last, true)
      }).toMap
    if (!selectFields.contains("*")) {
      if (dataType == "ums" && !selectFields.contains(UmsSysField.TS.toString)) {
        sql = sql + UmsSysField.TS.toString + ", "
      }
      if (dataType == "ums" && !selectFields.contains(UmsSysField.ID.toString)) {
        sql = sql + UmsSysField.ID.toString + ", "
      }
      if (dataType == "ums" && !selectFields.contains(UmsSysField.OP.toString)) {
        sql = sql + UmsSysField.OP.toString + ", "
      }
      sql = sql + SwiftsConstants.PROTOCOL_TYPE + ", "
    }
    sql = sql + sqlStrEle.substring(sqlStrEle.toLowerCase.indexOf("select ") + 7)
    sql = sql.replaceAll("(?i)" + " " + tableName + " ", " " + tableName + " ")

    SwiftsSql(SqlOptType.FLINK_SQL, None, sql, None, None, None, None)
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
        SwiftsConfMemoryStorage.registerDataStoreConnectionsMap(name_space, jdbc_url, username, password, parameters)
      }
    }
  }

}
