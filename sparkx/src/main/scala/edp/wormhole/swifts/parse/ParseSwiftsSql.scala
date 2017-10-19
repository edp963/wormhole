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


package edp.wormhole.swifts.parse

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.ResultSetMetaData

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.db.DbConnection
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.swifts._
import edp.wormhole.ums.{UmsDataSystem, UmsSysField}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ParseSwiftsSql extends EdpLogging {

  def parse(sqlStr: String,
            sourceNamespace: String, //sourceNamespace is rule
            sinkNamespace: String,
            validity: Boolean,
            dataType:String): Option[Array[SwiftsSql]] = {
    if (sqlStr.trim.nonEmpty) {
      val sqlStrArray = sqlStr.trim.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ").split(";").map(str => {
        val trimStr = str.trim
        if (trimStr.startsWith(SqlOptType.PUSHDOWN_SQL.toString)) trimStr.substring(12).trim
        else if (trimStr.startsWith(SqlOptType.PARQUET_SQL.toString)) trimStr.substring(11).trim
        else trimStr
      })
      val swiftsSqlArr = getSwiftsSql(sqlStrArray, sourceNamespace, sinkNamespace,validity,dataType)//sourcenamespace is rule
      swiftsSqlArr
    } else {
      None
    }
  }

  private def getSwiftsSql(sqlStrArray: Array[String],
                           sourceNamespace: String,//sourcenamespace is rule
                           sinkNamespace: String,
                           validity:Boolean,
                           dataType:String): Option[Array[SwiftsSql]] = {
    val swiftsSqlList = Some(sqlStrArray.map(sqlStrEle => {
      val sqlStrEleTrim = sqlStrEle.trim + " " //to fix no where clause bug, e.g select a, b from table;
      logInfo("sqlStrEle:::" + sqlStrEleTrim)
      if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.JOIN.toString) || sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.INNER_JOIN.toString)) {
        getJoin(SqlOptType.JOIN.toString, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.LEFT_JOIN.toString)) {
        getJoin(SqlOptType.LEFT_JOIN.toString, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.RIGHT_JOIN.toString)) {
        getJoin(SqlOptType.RIGHT_JOIN.toString, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.UNION.toString)) {
        getUnion(sqlStrEleTrim, sourceNamespace, sinkNamespace,validity,dataType)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.SPARK_SQL.toString)) {
        getSparkSql(sqlStrEleTrim, sourceNamespace,validity,dataType)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.CUSTOM_CLASS.toString)) {
        getCustomClass(sqlStrEleTrim)
      } else {
        logError("optType:" + sqlStrEleTrim + " is not supported")
        throw new Exception("wong operation data type:" + sqlStrEleTrim)
      }
    })
    )
    swiftsSqlList
  }

  private def getCustomClass(sqlStrEle: String): SwiftsSql = {
    val classFullName = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1).trim
    ConfMemoryStorage.registerSwiftsTransformReflectMap(classFullName)
    SwiftsSql(SqlOptType.CUSTOM_CLASS.toString, None, classFullName, None, None, None, None,None)
  }

  private def getUnion(sqlStrEle: String,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       validity:Boolean,
                       dataType:String): SwiftsSql = {
    val unionNamespace = sqlStrEle.substring(sqlStrEle.indexOf(" with ") + 5, sqlStrEle.indexOf("=")).trim
    val sqlStr = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1).trim
    val (sql, lookupFields, valuesFields) = getFieldsAndSql(sourceNamespace, sqlStr)
    var sqlSecondPart = sql.substring(sql.trim.toLowerCase.indexOf("select ") + 7)
    val selectSqlFields = sql.substring(0, sql.trim.toLowerCase.indexOf(" from "))
      .toLowerCase.split(",").map(field => {
      (field.trim, true)
    }).toMap
    if (dataType == "ums" && !selectSqlFields.contains(UmsSysField.TS.toString)) {
      sqlSecondPart = UmsSysField.TS.toString + "," + sqlSecondPart
    }
    if (dataType == "ums" && !selectSqlFields.contains(UmsSysField.ID.toString)) {
      sqlSecondPart = UmsSysField.ID.toString + "," + sqlSecondPart
    }
    if (dataType == "ums" && validity && !selectSqlFields.contains(UmsSysField.UID.toString)) {
      sqlSecondPart = UmsSysField.UID.toString + "," + sqlSecondPart
    }

    sqlSecondPart = "select " + sqlSecondPart
    val lookupNSArr: Array[String] = unionNamespace.split("\\.")
    UmsDataSystem.dataSystem(lookupNSArr(0).toLowerCase()) match {
      case UmsDataSystem.CASSANDRA =>
        sqlSecondPart = ParseCassandraSql.getCassandraSql(sql, lookupNSArr(2))
      case _ =>
    }
    val connectionConfig = ConfMemoryStorage.getDataStoreConnectionsMap(unionNamespace)
    val selectSchema = getSchema(sqlSecondPart, connectionConfig)


    val selectFieldsList = getIndependentFieldsFromSql(sqlSecondPart)
    val selectFieldsSet = selectFieldsList.map(fieldName => {
      val tmpField = fieldName.replaceAll("\\s","")
      if (tmpField.toLowerCase.startsWith("distinct(")) {
        tmpField.substring(9,tmpField.indexOf(")"))
      } else {
        fieldName.split(" ")(0).toLowerCase
      }
    }).toSet
    lookupFields.foreach(field => {
      val name = field.split(" ")(0).toLowerCase
      if (!selectFieldsSet.contains(name)) throw new Exception("select fields must contains lookup fields(where in fields)  ")
    })
    val lookupFieldsAlias = resetLookupTableFields(lookupFields, selectFieldsList)


    SwiftsSql(SqlOptType.UNION.toString, Some(selectSchema), sqlSecondPart, None,Some(unionNamespace), Some(valuesFields), Some(lookupFields), Some(lookupFieldsAlias))
  }

  private def getSchema(sql: String, connectionConfig:ConnectionConfig): String = {
    var testSql = sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
    if (connectionConfig.connectionUrl.toLowerCase.contains("cassandra")) {
      val index = sql.toLowerCase.indexOf(" where ")
      testSql = sql.substring(0, index) + " limit 1;"
    }

    val conn = DbConnection.getConnection(connectionConfig)
    val statement = conn.createStatement()
    println(testSql)
    val rs = statement.executeQuery(testSql)
    val schema: ResultSetMetaData = rs.getMetaData
    val columnCount = schema.getColumnCount
    val fieldSchema = StringBuilder.newBuilder
    for (i <- 0 until columnCount) {
      val columnName = schema.getColumnLabel(i + 1).toLowerCase
      val columnType: String = schema.getColumnTypeName(i+1)
      fieldSchema ++= columnName
      fieldSchema ++= ":"
      fieldSchema ++= DbType.convert(columnType)
      if (i != columnCount - 1) {
        fieldSchema ++= ","
      }
    }
    DbConnection.shutdownConnection(connectionConfig.connectionUrl,connectionConfig.username.orNull)
    fieldSchema.toString()

  }


  def getSparkSql(sqlStrEle: String, sourceNamespace: String,validity:Boolean,dataType:String): SwiftsSql = {//sourcenamespace is rule
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
        if (dataType == "ums" && !selectFields.contains(UmsSysField.TS.toString)) {
          sql = sql + UmsSysField.TS.toString + ", "
        }
        if (dataType == "ums" && !selectFields.contains(UmsSysField.ID.toString)) {
          sql = sql + UmsSysField.ID.toString + ", "
        }
        if (dataType == "ums" && !selectFields.contains(UmsSysField.OP.toString)) {
          sql = sql + UmsSysField.OP.toString + ", "
        }
        if (dataType == "ums" && validity && !selectFields.contains(UmsSysField.UID.toString)) {
          sql = sql + UmsSysField.UID.toString + ", "
        }
      }
      sql = sql + singleSql.substring(singleSql.toLowerCase.indexOf("select ") + 7)
      sql = sql.replaceAll("(?i)" + " " + tableName + " ", " " + tableName +" ")
      sql
    })
    SwiftsSql(SqlOptType.SPARK_SQL.toString, None, sqlArray.mkString(" union "), None,None, None, None, None)
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
      assert(tableName != "*", "table name cannot be *")  //in sql, lookup table use table name
      newSql = newSql.replaceAll(" " + tableName  + " ", " " + tmpTableName + " ") //sql format: select a,b from table where a in (sourceNS.x)
    })
    newSql
  }

  private def getJoin(optType: String,
                      sqlStrEle: String,
                      sourceNamespace: String,//namespace rule
                      sinkNamespace: String): SwiftsSql = {
    var joinNamespace = sqlStrEle.substring(sqlStrEle.toLowerCase.indexOf(" with ") + 5, sqlStrEle.indexOf("=")).trim.toLowerCase
    val sqlStr = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1)
    var (sql, lookupFields, valuesFields) = getFieldsAndSql(sourceNamespace, sqlStr)
    val lookupNamespaceArr = joinNamespace.split(",").map(_.trim)
    var fieldsStr:Option[String] = None
    var timeout:Option[Int] = None
    if (lookupNamespaceArr.length > 1 || joinNamespace.contains("(")) {
      joinNamespace = lookupNamespaceArr.map(namespaceWithTime => {
        val namespace = namespaceWithTime.substring(0, namespaceWithTime.indexOf("(")).trim
        timeout = Some(namespaceWithTime.substring(namespaceWithTime.indexOf("(") + 1, namespaceWithTime.indexOf(")")).trim.toInt)
        namespace
      }).mkString(",")
      sql = replaceTableNameMD5(sql, sourceNamespace, joinNamespace, sinkNamespace)
    } else {
      val lookupNamespaceArray: Array[String] = joinNamespace.split("\\.")
      UmsDataSystem.dataSystem(lookupNamespaceArray(0).toLowerCase()) match {
        case UmsDataSystem.CASSANDRA =>
          sql = ParseCassandraSql.getCassandraSql(sql, lookupNamespaceArray(2))
        case _ =>
      }
      val connectionConfig = ConfMemoryStorage.getDataStoreConnectionsMap(lookupNamespaceArr(0))
      fieldsStr = Some(getSchema(sql, connectionConfig))
    }

    val lowerCaseSql = sql.toLowerCase.trim
    val groupSplit = lowerCaseSql.split(" group by ")
    if (groupSplit.length > 2) throw new Exception("lookup sqlStr only contains one 'group by'")

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

    val selectFieldsList = getIndependentFieldsFromSql(sql)
    val selectFieldsSet = selectFieldsList.map(fieldName => {
      val tmpField = fieldName.replaceAll("\\s","")
      if (tmpField.toLowerCase.startsWith("distinct(")) {
        tmpField.substring(9,tmpField.indexOf(")"))
      } else {
        fieldName.split(" ")(0).toLowerCase
      }
    }).toSet
    lookupFields.foreach(field => {
      val name = field.split(" ")(0).toLowerCase
      if (!selectFieldsSet.contains(name)) throw new Exception("select fields must contains lookup fields(where in fields)  ")
    })
    val lookupFieldsAlias = resetLookupTableFields(lookupFields, selectFieldsList)
    SwiftsSql(optType, fieldsStr, sql, timeout,Some(joinNamespace), Some(valuesFields), Some(lookupFields), Some(lookupFieldsAlias))
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

  private def resetLookupTableFields(lookupTableFields: Array[String],
                                     selectFieldsList: List[String]): Array[String] = {
    val lookupTableFieldsSet = lookupTableFields.map(fieldName => fieldName.trim.toLowerCase).toSet
    val selectFieldsMap = new mutable.HashMap[String, String]
    selectFieldsList.foreach(select => {
      if (select.trim.indexOf(" ") > -1) {
        val s = select.trim.split(" ")
        val tmpField = select.replaceAll("\\s","")
        val originalName = if (tmpField.toLowerCase.startsWith("distinct(")) {
          tmpField.substring(9,tmpField.indexOf(")"))
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

  private def getFieldsAndSql(namespace: String, userSqlStr: String): (String, Array[String], Array[String]) = {
    val sqlStr: String = getJoinSql(userSqlStr)
    val namespaceArray = namespace.split("\\.")
    val fourDigitNamespace = (for (i <- 0 until 4) yield namespaceArray(i)).mkString(".")
    val joinPosition = sqlStr.toLowerCase.indexOf(fourDigitNamespace)
    val inPosition = sqlStr.toLowerCase.lastIndexOf(" in ", joinPosition)
    val valueLeftPosition = sqlStr.indexOf("(", inPosition)
    val valueRightPosition = sqlStr.indexOf(")", inPosition)
    val valueFieldsStr = sqlStr.substring(valueLeftPosition + 1, valueRightPosition).toLowerCase
    val valuesFields = if (valueFieldsStr.indexOf(namespace) > -1) {
      valueFieldsStr.trim.replace(namespace + ".", "").split(",").map(_.trim)
    }
    else {
      valueFieldsStr.trim.replaceAll(fourDigitNamespace + "\\.", "").split(",").map(_.trim)
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

    val sql = sqlStr.substring(0, joinLeftPosition) + " " + SwiftsConstants.REPLACE_STRING_INSQL + " " + sqlStr.substring(valueRightPosition + 1)
    (sql, joinFields, valuesFields)
  }
}
