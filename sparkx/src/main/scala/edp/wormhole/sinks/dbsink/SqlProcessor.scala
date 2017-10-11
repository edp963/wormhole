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


package edp.wormhole.sinks.dbsink

import java.sql._

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.db.DbConnection
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.sinks.DbHelper._
import edp.wormhole.sinks.SourceMutationType._
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.{UmsFieldType, UmsSysField, _}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsOpType._
import edp.wormhole.ums.UmsSysField._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlProcessor(sinkProcessConfig: SinkProcessConfig, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], specificConfig: DbConfig, sinkNamespace: String, connectionConfig: ConnectionConfig) extends EdpLogging {
  private lazy val namespace = UmsNamespace(sinkNamespace)
  private lazy val tableName = namespace.table
  private lazy val metaIdName = ID.toString
  private lazy val allFieldNames = schemaMap.keySet.toList
  private lazy val baseFieldNames = removeFieldNames(allFieldNames, Set(OP.toString).contains)
  private lazy val tableKeyNames = sinkProcessConfig.tableKeyList
  private lazy val fieldNamesWithoutParNames = removeFieldNames(baseFieldNames, specificConfig.partitionKeyList.contains)
  private lazy val updateFieldNames = removeFieldNames(fieldNamesWithoutParNames, tableKeyNames.contains)
  private lazy val fieldSqlTypeMap = schemaMap.map(kv => (kv._1, ums2dbType(kv._2._2)))

  def checkDbAndGetInsertUpdateList(keysTupleMap: mutable.HashMap[String, Seq[String]]): (List[Seq[String]], List[Seq[String]]) = {
    def selectMysqlSql(tupleCount: Int): String = {
      val keysString = tableKeyNames.map(tk =>s"""`$tk`""").mkString(",")
      val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      s"SELECT $keysString, $metaIdName FROM `$tableName` WHERE ($keysString) IN " +
        (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
    }

    def selectOracleSql(tupleCount: Int): String = {
      val keysString = tableKeyNames.map(name => "\"" + name.toUpperCase + "\"").mkString(",")
      val keyQuestionMarks: String = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      s"""SELECT $keysString, $metaIdName FROM "${tableName.toUpperCase}" WHERE ($keysString) IN """ +
        (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
          (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
        }).mkString(s" OR ($keysString) IN ")
    }

    def selectPostgresSql(tupleCount: Int): String = {
      val keysString = tableKeyNames.map(tk =>s"""$tk""").mkString(",")
      val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      s"SELECT $keysString, $metaIdName FROM $tableName WHERE ($keysString) IN " +
        (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
    }

    def selectOtherSql(tupleCount: Int): String = {
      val keysString = tableKeyNames.mkString(",")
      val keyQuestionMarks: String = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      s"""SELECT $keysString, $metaIdName FROM ${tableName.toUpperCase} WHERE ($keysString) IN """ +
        (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
          (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
        }).mkString(s" OR ($keysString) IN ")
    }

    def splitInsertAndUpdate(rs: ResultSet, keysTupleMap: mutable.HashMap[String, Seq[String]]) = {
      val rsKeyUmsTsMap = mutable.HashMap.empty[String, Long]
      val updateList = mutable.ListBuffer.empty[Seq[String]]
      val insertList = mutable.ListBuffer.empty[Seq[String]]
      val columnTypeMap = mutable.HashMap.empty[String, String]
      val metaData = rs.getMetaData
      val columnCount = metaData.getColumnCount
      for (i <- 1 to columnCount) {
        val columnName = metaData.getColumnLabel(i)
        val columnType = metaData.getColumnClassName(i)
        columnTypeMap(columnName.toLowerCase) = columnType
      }

      while (rs.next) {
        val keysId = tableKeyNames.map(keyName => {
          if (columnTypeMap(keyName) == "java.math.BigDecimal") rs.getBigDecimal(keyName).stripTrailingZeros.toPlainString
          else rs.getObject(keyName).toString
        }).mkString("_")
        val umsId = rs.getLong(metaIdName)
        rsKeyUmsTsMap(keysId) = umsId
      }
      logInfo("rs finish")

      keysTupleMap.foreach(keysTuple => {
        val (keysId, tuple) = keysTuple
        if (rsKeyUmsTsMap.contains(keysId)) {
          val tupleId = umsFieldValue(tuple(schemaMap(metaIdName)._1), UmsFieldType.LONG).asInstanceOf[Long]
          val rsId = rsKeyUmsTsMap(keysId)
          if (tupleId > rsId)
            updateList.append(tuple)
        } else
          insertList.append(tuple)
      })
      (insertList.toList, updateList.toList)
    }

    var ps: PreparedStatement = null
    var resultSet: ResultSet = null
    var conn: Connection = null
    try {
      val tupleList = keysTupleMap.values.toList
      val sql = namespace.dataSys match {
        case UmsDataSystem.MYSQL => selectMysqlSql(tupleList.size)
        case UmsDataSystem.ORACLE => selectOracleSql(tupleList.size)
        case UmsDataSystem.POSTGRESQL =>selectPostgresSql(tupleList.size)
        case _ => selectOtherSql(tupleList.size)
      }
      logInfo("select sql:" + sql)
      conn = DbConnection.getConnection(connectionConfig)
      ps = conn.prepareStatement(sql)
      var parameterIndex = 1
      for (tuple <- tupleList)
        for (key <- tableKeyNames) {
          psSetValue(key, parameterIndex, tuple, ps)
          parameterIndex += 1
        }
      logInfo("before query")
      resultSet = ps.executeQuery()
      logInfo("finish query")
      splitInsertAndUpdate(resultSet, keysTupleMap)
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logError("SQLTransientConnectionException", e)
        throw e
      case e: Throwable =>
        logError("execute select failed", e)
        throw e
    } finally {
      if (resultSet != null)
        try {
          resultSet.close()
        } catch {
          case e: Throwable => logError("resultSet.close", e)
        }
      if (ps != null)
        try {
          ps.close()
        } catch {
          case e: Throwable => logError("ps.close", e)
        }
      if (null != conn)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logError("conn.close", e)
        }
    }
  }

  def doInsert(tupleList: Seq[Seq[String]], sourceMutationType: SourceMutationType): Seq[Seq[String]] = {
    val columnNames = baseFieldNames.map(n =>s"""`$n`""").mkString(", ")
    val oracleColumnNames = baseFieldNames.map(n =>s"""$n""").mkString(",")
    val sql = namespace.dataSys match {
      case UmsDataSystem.MYSQL => s"INSERT INTO `$tableName` ($columnNames, ${UmsSysField.ACTIVE.toString}) VALUES " +
        (1 to baseFieldNames.size + 1).map(_ => "?").mkString("(", ",", ")")
      case _ => s"""INSERT INTO ${tableName.toUpperCase} ($oracleColumnNames, ${UmsSysField.ACTIVE.toString}) VALUES """ +
        (1 to baseFieldNames.size + 1).map(_ => "?").mkString("(", ",", ")")
    }
    logInfo("insert sql " + sql)
    val batchSize = specificConfig.`db.sql_batch_size.get`
    executeProcess(tupleList, sql, batchSize, UmsOpType.INSERT.toString)
  }

  def executeProcess(tupleList: Seq[Seq[String]], sql: String, batchSize: Int, optType: String): Seq[Seq[String]] = {
    if (tupleList.nonEmpty) {
      val errorTupleList = executeSql(tupleList, sql, UmsOpType.umsOpType(optType), batchSize)
      if (errorTupleList.nonEmpty) {
        val newErrorTupleList = if (batchSize == 1) errorTupleList else executeSql(errorTupleList, sql, UmsOpType.umsOpType(optType), 1)
        newErrorTupleList.foreach(data => logInfo(optType + ",data:" + data))
        newErrorTupleList
      } else ListBuffer.empty[List[String]]
    } else ListBuffer.empty[List[String]]
  }

  def doUpdate(tupleList: Seq[Seq[String]]): Seq[Seq[String]] = {
    val batchSize = specificConfig.`db.sql_batch_size.get`
    val sql = namespace.dataSys match {
      case UmsDataSystem.MYSQL => s"UPDATE `$tableName` SET " +
        updateFieldNames.map(fieldName => s"`$fieldName`=?").mkString(",") + s",${UmsSysField.ACTIVE.toString}=? WHERE " +
        tableKeyNames.map(key => s"`$key`=?").mkString(" AND ") + s" AND $metaIdName<? "
      case _ => s"""UPDATE ${tableName.toUpperCase()} SET """ +
        updateFieldNames.map(fieldName => s"$fieldName=?").mkString(",") + s",${UmsSysField.ACTIVE.toString}=? WHERE " +
        tableKeyNames.map(key => s"$key=?").mkString(" AND ") + s" AND $metaIdName<? "
    }
    logInfo("@update sql " + sql)
    executeProcess(tupleList, sql, batchSize, UmsOpType.UPDATE.toString)
  }


  private def psSetValue(fieldName: String, parameterIndex: Int, tuple: Seq[String], ps: PreparedStatement) = {

    val value = fieldValue(fieldName, schemaMap, tuple)
    if (value == null) ps.setNull(parameterIndex, fieldSqlTypeMap(fieldName))
    else ps.setObject(parameterIndex, value, fieldSqlTypeMap(fieldName))
  }

  def executeSql(tupleList: Seq[Seq[String]], sql: String, opType: UmsOpType, batchSize: Int): List[Seq[String]] = {
    def setPlaceholder(tuple: Seq[String], ps: PreparedStatement) = {
      var parameterIndex: Int = 1
      if (opType == UmsOpType.INSERT)
        for (field <- baseFieldNames) {
          psSetValue(field, parameterIndex, tuple, ps)
          parameterIndex += 1
        }
      else
        for (field <- updateFieldNames) {
          psSetValue(field, parameterIndex, tuple, ps)
          parameterIndex += 1
        }

      ps.setInt(parameterIndex,
        if (umsOpType(fieldValue(OP.toString, schemaMap, tuple).toString) == DELETE) UmsActiveType.INACTIVE
        else UmsActiveType.ACTIVE)

      if (opType == UPDATE) {
        for (i <- tableKeyNames.indices) {
          parameterIndex += 1
          psSetValue(tableKeyNames(i), parameterIndex, tuple, ps)
        }
        parameterIndex += 1
        psSetValue(metaIdName, parameterIndex, tuple, ps)
      }
    }

    var ps: PreparedStatement = null
    val errorTupleList: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer.empty[Seq[String]]
    var conn: Connection = null
    var index = 0 - batchSize

    try {
      conn = DbConnection.getConnection(connectionConfig)
      conn.setAutoCommit(false)
      logInfo("create connection successfully,batchsize:" + batchSize + ",tupleList:" + tupleList.size)
      ps = conn.prepareStatement(sql)
      tupleList.sliding(batchSize, batchSize).foreach(tuples => {
        index += batchSize
        for (i <- tuples.indices) {
          setPlaceholder(tuples(i), ps)
          ps.addBatch()
        }
        try {
          logInfo("execute batch start***")
          ps.executeBatch()
          logInfo("execute batch end***")
          conn.commit()
        } catch {
          case e: Throwable =>
            logWarning("executeBatch error" + e)
            errorTupleList ++= tuples
            if (batchSize == 1)
              logInfo("violate tuple -----------" + tuples)
        } finally {
          ps.clearBatch()
        }
      })
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logError("SQLTransientConnectionException" + e)
        logInfo("out batch ")
        if (index <= 0) errorTupleList ++= tupleList
        else errorTupleList ++= tupleList.takeRight(tupleList.size - index)
      case e: Throwable =>
        logError("get connection failed" + e)
        if (index <= 0) errorTupleList ++= tupleList
        else errorTupleList ++= tupleList.takeRight(tupleList.size - index)
    } finally {
      if (ps != null)
        try {
          ps.close()
        } catch {
          case e: Throwable => logError("ps.close", e)
        }
      if (null != conn)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logError("conn.close", e)
        }
    }
    errorTupleList.toList
  }
}
