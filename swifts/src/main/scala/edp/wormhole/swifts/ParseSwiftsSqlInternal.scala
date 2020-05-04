/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
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


package edp.wormhole.swifts

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.ResultSetMetaData

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.swifts.SqlOptType.SqlOptType
import edp.wormhole.ums.{UmsDataSystem, UmsSysField}
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.{Operator, SqlCondition, SwiftsSql}
import edp.wormhole.kuduconnection.KuduConnection
import org.apache.kudu.client.KuduTable
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex

object ParseSwiftsSqlInternal {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def getUnion(sqlStrEle: String,
               sourceNamespace: String,
               sinkNamespace: String,
               validity: Boolean,
               dataType: String,
               mutation: String): SwiftsSql = {
    val unionNamespace = sqlStrEle.substring(sqlStrEle.indexOf(" with ") + 5, sqlStrEle.indexOf("=")).trim
    val sqlStr = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1).trim
    val (sql, lookupFields: Array[String], valuesFields) = getFieldsAndSql(sourceNamespace, sqlStr, unionNamespace)
    var sqlSecondPart = sql.substring(sql.trim.toLowerCase.indexOf("select ") + 7)
    val fromIndex = sql.trim.toLowerCase.indexOf(" from ")
    val selectSqlFields = sql.substring(0, fromIndex)
      .toLowerCase.split(",").map(field => {
      (field.trim, true)
    }).toMap
    if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && selectSqlFields.keys.count(_.indexOf(UmsSysField.TS.toString)>=0) <= 0){
      sqlSecondPart = UmsSysField.TS.toString + "," + sqlSecondPart
    }
    if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && selectSqlFields.keys.count(_.indexOf(UmsSysField.ID.toString)>=0) <= 0) {
      sqlSecondPart = UmsSysField.ID.toString + "," + sqlSecondPart
    }
    if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && validity && !selectSqlFields.contains(UmsSysField.UID.toString)) {
      sqlSecondPart = UmsSysField.UID.toString + "," + sqlSecondPart
    }

    sqlSecondPart = "select " + sqlSecondPart
    val lookupNSArr: Array[String] = unionNamespace.split("\\.")
    val connectionConfig = ConnectionMemoryStorage.getDataStoreConnectionConfig(unionNamespace)
    val selectSchema = UmsDataSystem.dataSystem(lookupNSArr(0).toLowerCase()) match {
      case UmsDataSystem.CASSANDRA =>
        sqlSecondPart = getCassandraSql(sql, lookupNSArr(2))
        getRmdbSchema(sqlSecondPart, connectionConfig)
      case UmsDataSystem.KUDU =>
        getKuduSchema(sqlSecondPart + sql.trim.toLowerCase.substring(fromIndex), connectionConfig, unionNamespace)
      case _ =>
        getRmdbSchema(sqlSecondPart, connectionConfig)
    }

    val selectFieldsList = getIndependentFieldsFromSql(sqlSecondPart)
    val selectFieldsSet = selectFieldsList.map(fieldName => {
      val tmpField = fieldName.replaceAll("\\s", "")
      if (tmpField.toLowerCase.startsWith("distinct(")) {
        tmpField.substring(9, tmpField.indexOf(")"))
      } else {
        fieldName.split(" ")(0).toLowerCase
      }
    }).toSet


    lookupFields.foreach(field => {
      val name = field.split(" ")(0).toLowerCase
      if (!selectFieldsSet.contains(name)) throw new Exception("select fields must contains lookup fields(where in fields)  ")
    })
    val lookupFieldsAlias = resetLookupTableFields(lookupFields, selectFieldsList)

    //get sql condition
    val sqlConditions = if (lookupNSArr(0).startsWith(UmsDataSystem.KUDU.toString)) getConstantConditions(sql, sourceNamespace)
    else None

    SwiftsSql(SqlOptType.UNION.toString, Some(selectSchema), sqlSecondPart, None, Some(unionNamespace), Some(valuesFields), Some(lookupFields), Some(lookupFieldsAlias), sqlConditions)
  }


  def getCassandraSql(sql: String, dbName: String): String = {
    val indexFrom = sql.trim.toLowerCase().indexOf(" from ")
    if (indexFrom < 0) {
      throw new Exception("Invalid sqlStr, do not contain from")
    }
    val prefix = sql.substring(0, indexFrom + 6)
    val suffix = sql.substring(indexFrom + 6).trim
    val table = suffix.split(" ")(0)
    val suffixTable = suffix.substring(suffix.indexOf(" "))
    var finalSql = ""
    if (!table.contains("\\.")) {
      finalSql = prefix + " " + dbName + "." + table + suffixTable
    }
    finalSql
  }


  def getSparkSql(sqlStrEle: String, sourceNamespace: String, validity: Boolean, dataType: String, mutation: String): SwiftsSql = {
    //sourcenamespace is rule
    val tableName = sourceNamespace.split("\\.")(3)
    val unionSqlArray = getSqlArray(sqlStrEle, " union ", 7)
    val sqlArray = unionSqlArray.map(singleSql => {
      var sql = "select "
      val selectFields = singleSql
        .substring(sqlStrEle.toLowerCase.indexOf("select ") + 7, sqlStrEle.toLowerCase.indexOf(" from "))
        .toLowerCase.split(",")
        .map(field => {
          (field.trim.split(" ").last, true)
        }).toMap
      if (!selectFields.contains("*")) {
        if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && !selectFields.contains(UmsSysField.TS.toString)) {
          sql = sql + UmsSysField.TS.toString + ", "
        }
        if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && !selectFields.contains(UmsSysField.ID.toString)) {
          sql = sql + UmsSysField.ID.toString + ", "
        }
        if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && !selectFields.contains(UmsSysField.OP.toString)) {
          sql = sql + UmsSysField.OP.toString + ", "
        }
        if ((dataType == "ums" || (dataType != "ums" && mutation != "i")) && validity && !selectFields.contains(UmsSysField.UID.toString)) {
          sql = sql + UmsSysField.UID.toString + ", "
        }
      }
      sql = sql + singleSql.substring(singleSql.toLowerCase.indexOf("select ") + 7)
      sql = sql.replaceAll("(?i)" + " " + tableName + " ", " " + tableName + " ")
      sql
    })
    SwiftsSql(SqlOptType.SPARK_SQL.toString, None, sqlArray.mkString(" union "), None, None, None, None, None)
  }

  private def getSqlArray(originalSql: String, split: String, length: Int) = {
    val lowerSql = originalSql.toLowerCase()
    var index = 0
    val buf = new ArrayBuffer[String]()
    while (lowerSql.indexOf(split, index) >= 0) {
      val end = lowerSql.indexOf(split, index)
      val singleSql = originalSql.substring(index, end)
      buf += singleSql
      index = end + length
    }
    buf += originalSql.substring(index)
    buf.toArray
  }

  private def replaceTableNameMD5(sql: String, sourceNamespace: String, lookupNamespaces: String, sinkNameSpace: String) = {
    //join in streaming, in sql,only use table name, take namespace 4 fields
    var newSql = sql
    lookupNamespaces.split(",").foreach(lookupNameSpace => {
      val fileName = sourceNamespace + "_" + sinkNameSpace + "_" + lookupNameSpace
      val md = MessageDigest.getInstance("MD5")
      md.update(fileName.getBytes())
      val tmpTableName = new BigInteger(1, md.digest()).toString(16)
      val tableName = lookupNameSpace.split("\\.")(3)
      assert(tableName != "*", "table name cannot be *") //in sql, lookup table use table name
      newSql = newSql.replaceAll(" " + tableName + " ", " " + tmpTableName + " ") //sql format: select a,b from table where a in (sourceNS.x)
    })
    newSql
  }

  def getCEP(optType: SqlOptType, sqlStrEle: String): SwiftsSql = {
    val patternBeginIndex = 5
    val patternList = sqlStrEle.substring(patternBeginIndex)
    println(patternList + "--------pattern list")
    SwiftsSql(SqlOptType.CEP.toString, None, patternList, None, None, None, None, None)
  }


  def getJoin(optType: SqlOptType, sqlStrEle: String, sourceNamespace: String, sinkNamespace: String): SwiftsSql = {
    var joinNamespace = sqlStrEle.substring(sqlStrEle.toLowerCase.indexOf(" with ") + 5, sqlStrEle.indexOf("=")).trim.toLowerCase
    val sqlStr = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1)
    val fieldsAndSql = if (joinNamespace.startsWith(UmsDataSystem.HBASE.toString) || joinNamespace.startsWith(UmsDataSystem.REDIS.toString))
      getFieldsAndSqlFromHbaseOrRedis(sqlStr, joinNamespace)
    else getFieldsAndSql(sourceNamespace, sqlStr, joinNamespace)
    val (lookupFields, valuesFields) = (fieldsAndSql._2, fieldsAndSql._3)
    var sql = fieldsAndSql._1

    val lookupNamespaceArr = joinNamespace.split(",").map(_.trim)
    var fieldsStr: Option[String] = None
    var timeout: Option[Int] = None
    if (lookupNamespaceArr.length > 1 || joinNamespace.contains("(")) {
      joinNamespace = lookupNamespaceArr.map(namespaceWithTime => {
        val namespace = namespaceWithTime.substring(0, namespaceWithTime.indexOf("(")).trim
        timeout = Some(namespaceWithTime.substring(namespaceWithTime.indexOf("(") + 1, namespaceWithTime.indexOf(")")).trim.toInt)
        namespace
      }).mkString(",")
      sql = replaceTableNameMD5(sql, sourceNamespace, joinNamespace, sinkNamespace)
    } else {
      fieldsStr = getFieldsWithType(joinNamespace, sql)
    }

    //get sql condition
    val sqlConditions = if (joinNamespace.startsWith(UmsDataSystem.KUDU.toString)) getConstantConditions(sql, sourceNamespace)
    else None

    //    syntaxCheck(sql, lookupFields)
    val lookupFieldsAlias = getAliasLookupFields(sql, lookupFields)
    //logger.info(s"joinNamespace $joinNamespace, timeout $timeout")
    SwiftsSql(optType.toString, fieldsStr, sql, timeout, Some(joinNamespace), Some(valuesFields), Some(lookupFields), Some(lookupFieldsAlias), sqlConditions)
  }

  def getConstantConditions(sql: String, sourceNs: String): Option[Array[SqlCondition]] = {
    val fourSourceNsList = sourceNs.split("\\.")
    val fourSourceNs = s"${fourSourceNsList(0)}.${fourSourceNsList(1)}.${fourSourceNsList(2)}.${fourSourceNsList(3)}"
    if(sql.contains(" where ")) {
      val constantConditions = sql.substring(sql.toLowerCase.indexOf(" where ") + 6, if (sql.endsWith(";")) sql.length - 1 else sql.length)
      val conditionSeq = constantConditions.split(" and ")
      val sqlConditions = ListBuffer.empty[SqlCondition]
      conditionSeq.foreach(condition => {
        val sqlCondition = matchCondition(condition, fourSourceNs)
        if(null != sqlCondition) sqlConditions += sqlCondition
      })
      logger.info(s"getConstantConditions sql is $sql, sourceNs is $sourceNs, sqlConditions is $sqlConditions")
      if(sqlConditions.nonEmpty) Some(sqlConditions.toArray)
      else None
    } else {
      None
    }
  }


  private def matchCondition(conditionOrg: String, sourceNs: String): SqlCondition = {
    if(null != conditionOrg && !conditionOrg.isEmpty) {
      //去空格
      val spacePattern = new Regex("""\s{1,}""")
      val conditionTrim = spacePattern.replaceAllIn(conditionOrg, " ").trim.toLowerCase()
      val condition = deleteParentheses(conditionTrim)

      if(condition.contains(s" ${Operator.IS_NULL.toString} ")) {
        val conditionSeq = condition.split(s" ${Operator.IS_NULL.toString} ")
        SqlCondition(conditionSeq.head.trim, Operator.IS_NULL , "", false)
      } else if(condition.contains(s" ${Operator.IS_NOT_NULL.toString} ")) {
        val conditionSeq = condition.split(s" ${Operator.IS_NOT_NULL.toString} ")
        SqlCondition(conditionSeq.head.trim, Operator.IS_NOT_NULL , "", false)
      }
      else if(condition.contains(s"${Operator.GREATER_EQUAL.toString}")) {
        val conditionSeq = condition.split(s"${Operator.GREATER_EQUAL.toString}")
        SqlCondition(conditionSeq.head.trim, Operator.GREATER_EQUAL , deleteQuotation(conditionSeq.last.trim), false)
      } else if(condition.contains(s"${Operator.GREATER.toString}") && !condition.contains(s"${Operator.GREATER_EQUAL.toString}")) {
        val conditionSeq = condition.split(s"${Operator.GREATER.toString}")
        SqlCondition(conditionSeq.head.trim, Operator.GREATER , deleteQuotation(conditionSeq.last.trim), false)
      } else if(condition.contains(s"${Operator.LESS_EQUAL.toString}")) {
        val conditionSeq = condition.split(s"${Operator.LESS_EQUAL.toString}")
        SqlCondition(conditionSeq.head.trim, Operator.LESS_EQUAL , deleteQuotation(conditionSeq.last.trim), false)
      } else if(condition.contains(s"${Operator.LESS.toString}") && !condition.contains(s"${Operator.LESS_EQUAL.toString}")) {
        val conditionSeq = condition.split(s"${Operator.LESS.toString}")
        SqlCondition(conditionSeq.head.trim, Operator.LESS , deleteQuotation(conditionSeq.last.trim), false)
      } else if(condition.contains(s"${Operator.EQUAL.toString}") && !condition.contains(s"${Operator.GREATER_EQUAL.toString}") && !condition.contains(s"${Operator.LESS_EQUAL.toString}")) {
        val conditionSeq = condition.split(s"${Operator.EQUAL.toString}")
        SqlCondition(conditionSeq.head.trim, Operator.EQUAL , deleteQuotation(conditionSeq.last.trim), false)
      }
      else if(condition.contains(s" ${Operator.IN.toString} ") && !condition.contains("${") && !condition.contains(sourceNs)){
        val conditionSeq = condition.split(s" ${Operator.IN.toString} ")
        val inConditionValue = deleteParentheses(conditionSeq.last.trim)
        SqlCondition(conditionSeq.head.trim, Operator.getOperator(Operator.IN.toString), inConditionValue, false)
      } else {
        null
      }
    } else null
  }

  private def deleteParentheses(s: String): String = {
    var result = s
    //去左括号
    if(result.startsWith("(")) result = result.substring(1, result.length)
    //去右括号
    if(result.endsWith(")")) result = result.substring(0, result.length-1)
    result
  }

  private def deleteQuotation(s: String): String = {
    var result = s
    //去左括号
    if(result.startsWith("\"") || result.startsWith("'")) result = result.substring(1, result.length)
    //去右括号
    if(result.endsWith("\"") || result.endsWith("'")) result = result.substring(0, result.length-1)
    result
  }

  private def getFieldsAndSqlFromHbaseOrRedis(userSqlStr: String, joinNamespace: String): (String, Array[String], Array[String]) = {
    val joinByPosition = userSqlStr.toLowerCase.indexOf(" joinby ") + 8
    val joinByFields = userSqlStr.substring(joinByPosition)
    (userSqlStr, Array.empty[String], Array[String](joinByFields))
  }

  private def getFieldsWithType(joinNamespace: String, sql: String) = {
    val lookupNamespacesArr = joinNamespace.split(",").map(_.trim)
    val connectionConfig = ConnectionMemoryStorage.getDataStoreConnectionConfig(lookupNamespacesArr(0))
    val fieldsStr = if (joinNamespace.startsWith(UmsDataSystem.HBASE.toString) || joinNamespace.startsWith(UmsDataSystem.REDIS.toString)) {
      Some(getFieldsFromHbaseOrRedis(sql))
    } else if (joinNamespace.startsWith(UmsDataSystem.KUDU.toString))
      Some(getKuduSchema(sql, connectionConfig, joinNamespace))
    else Some(getRmdbSchema(sql, connectionConfig))
    fieldsStr
  }

  private def getFieldsFromHbaseOrRedis(sql: String): String = {
    val selectFieldFrom = sql.indexOf("select ") + 7
    val selectFieldEnd = sql.toLowerCase.indexOf(" from ")
    sql.substring(selectFieldFrom, selectFieldEnd)
  }

  private def getRmdbSchema(sql: String, connectionConfig: ConnectionConfig): String = {
    val testSql = if (connectionConfig.connectionUrl.toLowerCase.contains("cassandra")) {
      val index = sql.toLowerCase.indexOf(" where ")
      sql.substring(0, index) + " limit 1;"
    } else getRmdbVerifySql(sql)
    logger.info(connectionConfig.connectionUrl + " in getSchema")
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
      fieldSchema ++= DbType.convert(columnType.toUpperCase)
      if (i != columnCount - 1)
        fieldSchema ++= ","
    }
    DbConnection.shutdownConnection(connectionConfig.connectionUrl, connectionConfig.username.orNull)
    fieldSchema.toString()
  }

  private def getRmdbVerifySql(sql: String): String = {
    if(sql.contains(SwiftsConstants.REPLACE_STRING_INSQL)) {
      sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
    } else {
      /*if(sql.trim.endsWith(";")) {
        sql.substring(0, sql.lastIndexOf(";")) + " limit 1;"
      } else {
        sql + " limit 1"
      }*/
      if(sql.toLowerCase().contains(" where ")) {
        val whereIndex = sql.toLowerCase().lastIndexOf(" where ")
        sql.substring(0, whereIndex + 7) + " 1=2 and " + sql.substring(whereIndex + 7)
      } else if(sql.trim.endsWith(";")) {
        sql.substring(0, sql.lastIndexOf(";")) + " where 1=2;"
      } else {
        sql + " where 1=2"
      }
    }
  }

  private def getKuduSchema(sql: String, connectionConfig: ConnectionConfig, joinNamespace: String) = {
    val database = joinNamespace.split("\\.")(2)
    val fromIndex = sql.indexOf(" from ")
    val afterFromSql = sql.substring(fromIndex + 6).trim
    val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
    val tableName = KuduConnection.getTableName(tmpTableName, database)
    logger.info("tableName:" + tableName)
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    val table: KuduTable = client.openTable(tableName)
    val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
    val tableSchema: mutable.Map[String, String] = KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)
    val selectLength = 6
    val selectFieldsArray = getFieldsArray(sql.substring(selectLength, fromIndex))
    val schemaInString = selectFieldsArray.map(fieldWithAs => {
      if(tableSchema.contains(fieldWithAs._1)) {
        fieldWithAs._1 + ":" + tableSchema(fieldWithAs._1) + " as " + fieldWithAs._2
      } else if(!tableSchema.contains(fieldWithAs._1) && ((fieldWithAs._1.startsWith("'") && fieldWithAs._1.endsWith("'")) || (fieldWithAs._1.startsWith("\"") && fieldWithAs._1.endsWith("\"")))) {
        fieldWithAs._1 + ":" + "default" + " as " + fieldWithAs._2
      } else {
        logger.error(s"""kudu table $database.$tmpTableName not contain field ${fieldWithAs._1}, all fields is $tableSchema""")
        throw new Exception(s"""kudu table $database.$tmpTableName not contain field ${fieldWithAs._1}""")
      }
    }).mkString(",")
    logger.info("get kudu table schema success")
    KuduConnection.closeClient(client)
    schemaInString
  }

  def getFieldsArray(fields: String): Array[(String, String)] = {
    fields.split(",").map(f => {
      val trimField = f.trim
      val lowerF = trimField.toLowerCase
      val asPosition = lowerF.indexOf(" as ")
      if (asPosition > 0) (trimField.substring(0, asPosition).trim, trimField.substring(asPosition + 4).trim)
      else (trimField, trimField)
    })
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
    if (lookupFields.nonEmpty) {
      lookupFields.foreach(field => {
        val name = field.split(" ")(0).toLowerCase
        if (!selectFieldsSet.contains(name)) throw new Exception("select fields must contains lookup fields(where in fields)  ")
      })
    } else logger.warn(s"lookup sql [$sql] doesn't contain stream fields, will select and join all data-----")
    resetLookupTableFields(lookupFields, selectFieldsList)
  }

  private def resetLookupTableFields(lookupTableFields: Array[String],
                                     selectFieldsList: List[String]): Array[String] = {
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

  def getJoinSql(sqlStr: String): String = {
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


  def getFlinkSql(sqlStrEle: String, dataType: String, sourceNamespace: String, sourceSchemaFieldSet: collection.Set[String]): SwiftsSql = {
    val tableName = sourceNamespace.split("\\.")(3)
    var sql = "select "
    val selectFields = sqlStrEle
      .substring(sqlStrEle.toLowerCase.indexOf("select ") + 7, sqlStrEle.toLowerCase.indexOf(" from "))
      .toLowerCase.split(",")
      .map(field => {
        val fieldParts = field.trim.split(" ")
        fieldParts.last
      })
    if (sqlStrEle.toLowerCase.indexOf("over") == -1 && sqlStrEle.toLowerCase.indexOf("group by") == -1)
      if (!selectFields.contains("*")) {
        if (dataType == "ums" && !selectFields.contains(UmsSysField.TS.toString)) {
          sql = sql + UmsSysField.TS.toString + ", "
        }
        if (dataType == "ums" && (!selectFields.contains(UmsSysField.ID.toString)) && sourceSchemaFieldSet.contains(UmsSysField.ID.toString)) {
          sql = sql + UmsSysField.ID.toString + ", "
        }
        if (dataType == "ums" && !selectFields.contains(UmsSysField.OP.toString) && sourceSchemaFieldSet.contains(UmsSysField.OP.toString)) {
          sql = sql + UmsSysField.OP.toString + ", "
        }
      }
    sql = sql + sqlStrEle.substring(sqlStrEle.toLowerCase.indexOf("select ") + 7)
    sql = sql.replaceAll("(?i)" + " " + tableName + " ", " " + tableName + " ")

    SwiftsSql(SqlOptType.FLINK_SQL.toString, None, sql, None, None, None, None, None)
  }


  def getFieldsAndSqlFromHbaseOrRedis(sourceNamespace: String, userSqlStr: String, joinNamespace: String): (String, Array[String], Array[String]) = {
    val joinByPosition = userSqlStr.toLowerCase.indexOf(" joinby ") + 8
    val joinbyFileds = userSqlStr.substring(joinByPosition)
    (userSqlStr, Array.empty[String], Array[String](joinbyFileds))

  }

  private def getFieldsAndSql(sourceNamespace: String, userSqlStr: String, joinNamespace: String): (String, Array[String], Array[String]) = {
    println("userSqlStr " + userSqlStr)
    val sqlStr: String = getJoinSql(userSqlStr)
    val namespaceArray = sourceNamespace.split("\\.")
    val fourDigitNamespace = (for (i <- 0 until 4) yield namespaceArray(i)).mkString(".")
    val joinPosition = if (sqlStr.toLowerCase.indexOf(fourDigitNamespace) > -1) sqlStr.toLowerCase.indexOf(fourDigitNamespace) else sqlStr.toLowerCase.indexOf("${")
    if(joinPosition > -1) {
      val temp_inPosition = sqlStr.toLowerCase.lastIndexOf(" in ", joinPosition)
      val inPosition = if (temp_inPosition < 0) sqlStr.toLowerCase.lastIndexOf(" in(", joinPosition) else temp_inPosition
      val valueLeftPosition = sqlStr.indexOf("(", inPosition)
      val valueRightPosition = sqlStr.indexOf(")", inPosition)
      val valueFieldsStr = sqlStr.substring(valueLeftPosition + 1, valueRightPosition).toLowerCase
      val valuesFields = if (valueFieldsStr.indexOf(sourceNamespace) > -1) {
        valueFieldsStr.trim.replace(sourceNamespace + ".", "").split(",").map(_.trim)
      } else if (valueFieldsStr.indexOf("${") > -1) {
        valueFieldsStr.split(",").map(field => field.replaceAll("\\$\\{", "").replaceAll("\\}", "")).map(_.trim)
      } else if(valueFieldsStr.indexOf(fourDigitNamespace) > -1) {
        valueFieldsStr.trim.replaceAll(fourDigitNamespace + "\\.", "").split(",").map(_.trim)
      } else {
        logger.error(userSqlStr + " lookup fields does not contain ${} or " + fourDigitNamespace)
        throw new Exception(userSqlStr + " lookup fields does not contain ${} or " + fourDigitNamespace)
        //Array[String]()
      }
      var joinLeftPosition: Int = 0
      var joinRightPosition: Int = 0
      var tmpPosition = inPosition - 1
      var tmpChar = sqlStr.charAt(tmpPosition)
      while (tmpChar == ' ') {
        tmpPosition = tmpPosition - 1
        tmpChar = sqlStr.charAt(tmpPosition)
      }
      joinRightPosition = tmpPosition + 1
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
      joinLeftPosition = tmpPosition

      val joinFieldsStr = sqlStr.substring(joinLeftPosition + 1, joinRightPosition).replace(")", "").trim.toLowerCase
      val joinFields = joinFieldsStr.trim.split(",").map(_.trim)
      val sql = sqlStr.substring(0, joinLeftPosition) + " " +
        SwiftsConstants.REPLACE_STRING_INSQL + " " +
        sqlStr.substring(valueRightPosition + 1)
      logger.info(s"lookup sql sql $sql, joinFields $joinFields, valuesFields $valuesFields")
      (sql, joinFields, valuesFields)
    } else {
      logger.warn(s"----sourceNamespace $sourceNamespace,joinNamespace $joinNamespace, lookup sql [$userSqlStr] doesn't contain stream fields, will select and join all data-----")
      (sqlStr, Array[String](), Array[String]())
    }
  }
}
