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

import java.sql.{Connection, PreparedStatement, ResultSet, SQLTransientConnectionException}

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sinks.utils.{SinkCommonUtils, SinkDbSchemaUtils}
import edp.wormhole.ums.{UmsFieldType, _}
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.sinks.DbHelper._
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

import scala.collection.mutable

class SplitTableSqlProcessor(sinkProcessConfig: SinkProcessConfig, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], specificConfig: DbConfig, sinkNamespace: String, connectionConfig: ConnectionConfig) {
  private lazy val logger = Logger.getLogger(this.getClass)
  private lazy val namespace = UmsNamespace(sinkNamespace)
  private lazy val masterTableName = namespace.table
  private lazy val subTableName = specificConfig.edpTable
  private lazy val metaIdName = ID.toString
  private lazy val metaOpName = OP.toString
  private lazy val allFieldNames = schemaMap.keySet.toList
  private lazy val masterBaseFieldNames: List[String] = removeFieldNames(allFieldNames, Set(OP.toString, ID.toString, TS.toString, UID.toString).contains)
  private lazy val tableKeyNames = sinkProcessConfig.tableKeyList
  private lazy val subColumns = Set(ID.toString, TS.toString, UID.toString) ++ tableKeyNames
  private lazy val subBaseFieldNames = removeOtherFieldNames(allFieldNames, subColumns.contains)
  private lazy val masterFieldNamesWithoutParNames = removeFieldNames(masterBaseFieldNames, specificConfig.partitionKeyList.contains)
  private lazy val masterUpdateFieldNames = removeFieldNames(masterFieldNamesWithoutParNames, tableKeyNames.contains)
  private lazy val subUpdateFieldNames = removeFieldNames(subBaseFieldNames, tableKeyNames.contains)
  private lazy val fieldSqlTypeMap = schemaMap.map(kv => (kv._1, SinkDbSchemaUtils.ums2dbType(kv._2._2)))
  private val DataTable = "dataTable"
  private val IdempotencyTable = "IdempotencyTable"

  def checkDbAndGetInsertUpdateDeleteList(keysTupleMap: mutable.HashMap[String, Seq[String]]): (Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]], Seq[Seq[String]]) = {
    def selectOtherSql(tupleCount: Int, tableType: String): String = {
      val tkDecorated = tableKeyNames.map(n =>s"""$n""")
      val keysString = tkDecorated.mkString(",")
      val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      if (tableType == DataTable)
        s"SELECT $keysString FROM $masterTableName WHERE ($keysString) IN " +
          (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
      else
        s"SELECT $keysString, $metaIdName FROM $subTableName WHERE ($keysString) IN " +
          (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
    }

    def selectMysqlSql(tupleCount: Int, tableType: String): String = {
      val tkDecorated = tableKeyNames.map(n =>s"""`$n`""")
      val keysString = tkDecorated.mkString(",")
      val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      if (tableType == DataTable)
        s"SELECT $keysString FROM `$masterTableName` WHERE ($keysString) IN " +
          (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
      else
        s"SELECT $keysString, `$metaIdName` FROM `$subTableName` WHERE ($keysString) IN " +
          (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
    }

    def selectOracleSql(tupleCount: Int, tableType: String): String = {
      val keysString = tableKeyNames.map(n => n.toUpperCase).map(key => "\"" + key + "\"").mkString(",")
      val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
      if (tableType == DataTable)
        s"""SELECT $keysString FROM "${masterTableName.toUpperCase}" WHERE ($keysString) IN """ +
          (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
            (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
          }).mkString(s" OR ($keysString) IN ")
      else
        s"""SELECT $keysString, "${metaIdName.toUpperCase}" FROM "${subTableName.toUpperCase}" WHERE ($keysString) IN """ +
          (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
            (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
          }).mkString(s" OR ($keysString) IN ")
    }

    def splitInsertAndUpdate(masterRs: ResultSet, subRs: ResultSet, keysTupleMap: mutable.HashMap[String, Seq[String]]) = {
      val subRsKeyUmsIdMap = mutable.HashMap.empty[String, Long]
      val masterRsKeyList = mutable.ListBuffer.empty[String]
      val insertUpdateList = mutable.ListBuffer.empty[Seq[String]]
      val insertInsertList = mutable.ListBuffer.empty[Seq[String]]
      val noneInsertList = mutable.ListBuffer.empty[Seq[String]]
      val updateInsertList = mutable.ListBuffer.empty[Seq[String]]
      val updateUpdateList = mutable.ListBuffer.empty[Seq[String]]
      val noneUpdateList = mutable.ListBuffer.empty[Seq[String]]
      val deleteInsertList = mutable.ListBuffer.empty[Seq[String]]
      val deleteUpdateList = mutable.ListBuffer.empty[Seq[String]]
      val columnTypeMap = mutable.HashMap.empty[String, String]
      val masterMetaData = masterRs.getMetaData
      val subMetaData = subRs.getMetaData
      val masterColumnCount = masterMetaData.getColumnCount
      for (i <- 1 to masterColumnCount) {
        val columnName = masterMetaData.getColumnLabel(i)
        val columnType = masterMetaData.getColumnClassName(i)
        columnTypeMap(columnName.toLowerCase) = columnType
      }
      for (j <- 1 to subMetaData.getColumnCount) {
        val columnName = subMetaData.getColumnLabel(j)
        val columnType = subMetaData.getColumnClassName(j)
        columnTypeMap(columnName.toLowerCase) = columnType
      }
      while (masterRs.next) {
        val keysId = tableKeyNames.map(keyName => {
          if (columnTypeMap(keyName) == "java.math.BigDecimal") masterRs.getBigDecimal(keyName).stripTrailingZeros.toPlainString
          else masterRs.getObject(keyName).toString
        }).mkString("_")
        masterRsKeyList.append(keysId)
      }
      while (subRs.next) {
        val keysId = tableKeyNames.map(keyName => {
          if (columnTypeMap(keyName) == "java.math.BigDecimal") subRs.getBigDecimal(keyName).stripTrailingZeros.toPlainString
          else subRs.getObject(keyName).toString
        }).mkString("_")
        val umsId = subRs.getLong(metaIdName)
        subRsKeyUmsIdMap(keysId) = umsId
      }

      keysTupleMap.foreach(keysTuple => {
        val (keysId, tuple) = keysTuple
        val opValue = UmsFieldType.umsFieldValue(tuple(schemaMap(metaOpName)._1), UmsFieldType.STRING).asInstanceOf[String]
        if (masterRsKeyList.contains(keysId) && subRsKeyUmsIdMap.contains(keysId)) {
          val umsidInTuple = UmsFieldType.umsFieldValue(tuple(schemaMap(metaIdName)._1), UmsFieldType.LONG).asInstanceOf[Long]
          val umsidInDb = subRsKeyUmsIdMap(keysId)
          if (umsidInTuple > umsidInDb) {
            if (opValue == UmsOpType.UPDATE.toString || opValue == UmsOpType.INSERT.toString) updateUpdateList.append(tuple)
            else deleteUpdateList.append(tuple)
          }
        } else if ((!masterRsKeyList.contains(keysId)) && (!subRsKeyUmsIdMap.contains(keysId))) {
          if (opValue == UmsOpType.UPDATE.toString || opValue == UmsOpType.INSERT.toString) insertInsertList.append(tuple)
          else noneInsertList.append(tuple)
        } else if (masterRsKeyList.contains(keysId) && !subRsKeyUmsIdMap.contains(keysId)) {
          if (opValue == UmsOpType.UPDATE.toString || opValue == UmsOpType.INSERT.toString) updateInsertList.append(tuple)
          else deleteInsertList.append(tuple)
        } else if (!masterRsKeyList.contains(keysId) && subRsKeyUmsIdMap.contains(keysId)) {
          val umsidInTuple = UmsFieldType.umsFieldValue(tuple(schemaMap(metaIdName)._1), UmsFieldType.LONG).asInstanceOf[Long]
          val umsidInDb = subRsKeyUmsIdMap(keysId)
          if (umsidInTuple > umsidInDb) {
            if (opValue == UmsOpType.UPDATE.toString || opValue == UmsOpType.INSERT.toString) insertUpdateList.append(tuple)
            else noneUpdateList.append(tuple)
          }
        }
      })
      (insertInsertList, insertUpdateList, updateInsertList, updateUpdateList, deleteInsertList, deleteUpdateList, noneInsertList, noneUpdateList)
    }

    var ps: PreparedStatement = null
    var masterResultSet: ResultSet = null
    var subResultSet: ResultSet = null
    var conn: Connection = null
    try {
      val tupleList = keysTupleMap.values.toList
      val (masterSql, subSql) = namespace.dataSys match {
        case UmsDataSystem.ORACLE => (selectOracleSql(tupleList.size, DataTable), selectOracleSql(tupleList.size, IdempotencyTable))
        case UmsDataSystem.MYSQL => (selectMysqlSql(tupleList.size, DataTable), selectMysqlSql(tupleList.size, IdempotencyTable))
        case _ => (selectOtherSql(tupleList.size, DataTable), selectOtherSql(tupleList.size, IdempotencyTable))
      }
      logger.info("@masterSql: " + masterSql)
      logger.info("@subSql: " + subSql)
      conn = DbConnection.getConnection(connectionConfig)
      ps = conn.prepareStatement(masterSql)
      var masterParameterIndex = 1
      for (tuple <- tupleList)
        for (key <- tableKeyNames) {
          psSetValue(key, masterParameterIndex, tuple, ps)
          masterParameterIndex += 1
        }
      masterResultSet = ps.executeQuery()

      ps = conn.prepareStatement(subSql)
      var subParameterIndex = 1
      for (tuple <- tupleList)
        for (key <- tableKeyNames) {
          psSetValue(key, subParameterIndex, tuple, ps)
          subParameterIndex += 1
        }
      subResultSet = ps.executeQuery()

      splitInsertAndUpdate(masterResultSet, subResultSet, keysTupleMap)
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logger.error("SQLTransientConnectionException", e)
        throw e
      case e: Throwable =>
        logger.error("execute select failed", e)
        conn.rollback()
        throw e
    } finally {
      if (masterResultSet != null)
        try {
          masterResultSet.close()
        } catch {
          case e: Throwable => logger.error("resultSet.close", e)
        }
      if (subResultSet != null)
        try {
          subResultSet.close()
        } catch {
          case e: Throwable => logger.error("resultSet.close", e)
        }
      if (ps != null)
        try {
          ps.close()
        } catch {
          case e: Throwable => logger.error("ps.close", e)
        }
      if (null != conn)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logger.error("conn.close", e)
        }
    }
  }


  private def psSetValue(fieldName: String, parameterIndex: Int, tuple: Seq[String], ps: PreparedStatement) = {
    val value = SinkCommonUtils.fieldValue(fieldName, schemaMap, tuple)
    if (value == null) ps.setNull(parameterIndex, fieldSqlTypeMap(fieldName))
    else ps.setObject(parameterIndex, value, fieldSqlTypeMap(fieldName))
  }

  def contactDb(tupleList: Seq[Seq[String]], opType: String): Seq[Seq[String]] = {
    val masterColumnNames = if (namespace.dataSys == UmsDataSystem.MYSQL) masterBaseFieldNames.map(n =>s"""`$n`""").mkString(",")
    else masterBaseFieldNames.map(n => n.toUpperCase).mkString(", ")
    val subColumnNames = if (namespace.dataSys == UmsDataSystem.MYSQL) subBaseFieldNames.map(n =>s"""`$n`""").mkString(",")
    else subBaseFieldNames.map(n => n.toUpperCase).mkString(",")
    val mTableName = if (namespace.dataSys == UmsDataSystem.MYSQL) s"`$masterTableName`"
    else masterTableName.toUpperCase
    val sTableName = if (namespace.dataSys == UmsDataSystem.MYSQL) s"`$subTableName`"
    else subTableName.toUpperCase
    val updateColumns = if (namespace.dataSys == UmsDataSystem.MYSQL) masterUpdateFieldNames.map(fieldName => s"`$fieldName`=?").mkString(",")
    else masterUpdateFieldNames.map(n => n.toUpperCase).map(fieldName => fieldName + "=?").mkString(",")
    val dataTkNames = if (namespace.dataSys == UmsDataSystem.MYSQL) tableKeyNames.map(key => s"`$key`=?").mkString(" AND ")
    else tableKeyNames.map(n => n.toUpperCase).map(key => key + "=?").mkString(" AND ")
    val subTkNames = if (namespace.dataSys == UmsDataSystem.MYSQL) subUpdateFieldNames.map(fieldName => s"`$fieldName`=?").mkString(",")
    else subUpdateFieldNames.map(n => n.toUpperCase).map(fieldName => s"$fieldName=?").mkString(",")

    val masterSql =
      if (opType.startsWith("i")) {
        if (namespace.dataSys == UmsDataSystem.ORACLE && specificConfig.oracle_sequence_config.nonEmpty) {
          val fieldName = specificConfig.oracle_sequence_config.get.field_name
          val sequenceName = specificConfig.oracle_sequence_config.get.sequence_name + ".NEXTVAL"
          s"INSERT INTO $mTableName ($fieldName,$masterColumnNames) VALUES " + (1 to masterBaseFieldNames.size).map(_ => "?").mkString("(" + sequenceName + ",", ",", ")")
        } else {
          s"INSERT INTO $mTableName ($masterColumnNames) VALUES " + (1 to masterBaseFieldNames.size).map(_ => "?").mkString("(", ",", ")")
        }
      } else if (opType.startsWith("u")) s"UPDATE $mTableName SET $updateColumns WHERE $dataTkNames "
      else s"delete from $mTableName  WHERE $dataTkNames "

    val subSql = if (opType.split("_")(1).startsWith("i")) s"INSERT INTO $sTableName ($subColumnNames,${UmsSysField.ACTIVE.toString}) VALUES " + (1 to subBaseFieldNames.size + 1).map(_ => "?").mkString("(", ",", ")")
    else s"UPDATE $sTableName SET $subTkNames ,${UmsSysField.ACTIVE.toString}=? WHERE $dataTkNames "

    val errorTupleList = specialExecuteSql(tupleList, masterSql, subSql)
    val errorTuple2List = specialExecuteSql(errorTupleList, masterSql, subSql, 1)
    if (errorTuple2List.nonEmpty) logger.error("opType:" + opType + ",data:" + errorTuple2List.head)
    errorTuple2List
  }


  def specialExecuteSql(tupleList: Seq[Seq[String]], masterSql: String, subSql: String, retryCount: Int = 0): List[Seq[String]] = {
    def setPlaceholder(tuple: Seq[String], ps: PreparedStatement, sqlType: String, sql: String, insertFieldNames: List[String], updateFieldNames: List[String]) = {
      var parameterIndex: Int = 1
      if (sqlType == DataTable) {
        if (sql.startsWith("I")) {
          for (field <- insertFieldNames) {
            psSetValue(field, parameterIndex, tuple, ps)
            parameterIndex += 1
          }
        }
        else if (sql.startsWith("U")) {
          for (field <- updateFieldNames) {
            psSetValue(field, parameterIndex, tuple, ps)
            parameterIndex += 1
          }
          for (i <- tableKeyNames.indices) {
            psSetValue(tableKeyNames(i), parameterIndex, tuple, ps)
            parameterIndex += 1
          }
        }
        else {
          for (i <- tableKeyNames.indices) {
            psSetValue(tableKeyNames(i), parameterIndex, tuple, ps)
            parameterIndex += 1
          }
        }
      }
      else {
        if (sql.startsWith("I")) {
          for (field <- insertFieldNames) {
            psSetValue(field, parameterIndex, tuple, ps)
            parameterIndex += 1
          }
          val opValue: Int = if (UmsOpType.umsOpType(SinkCommonUtils.fieldValue(OP.toString, schemaMap, tuple).toString) == UmsOpType.DELETE) UmsActiveType.INACTIVE
          else UmsActiveType.ACTIVE
          ps.setInt(parameterIndex, opValue)
        }
        else {
          for (field <- updateFieldNames) {
            psSetValue(field, parameterIndex, tuple, ps)
            parameterIndex += 1
          }
          val opValue: Int = if (UmsOpType.umsOpType(SinkCommonUtils.fieldValue(OP.toString, schemaMap, tuple).toString) == UmsOpType.DELETE) UmsActiveType.INACTIVE
          else UmsActiveType.ACTIVE
          ps.setInt(parameterIndex, opValue)
          parameterIndex += 1
          for (i <- tableKeyNames.indices) {
            psSetValue(tableKeyNames(i), parameterIndex, tuple, ps)
            parameterIndex += 1
          }
        }
      }
    }

    var psMaster: PreparedStatement = null
    var psSub: PreparedStatement = null
    val errorTupleList: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer.empty[Seq[String]]

    var conn: Connection = null
    try {
      conn = DbConnection.getConnection(connectionConfig)
      conn.setAutoCommit(false)
      logger.info(s"@write list.size:${tupleList.length} masterSql $masterSql")
      logger.info(s"@write list.size:${tupleList.length} subSql $subSql")

      tupleList.foreach(tuples => {
        try {
          psMaster = conn.prepareStatement(masterSql)
          //tuples.foreach(tuple => {
          setPlaceholder(tuples, psMaster, DataTable, masterSql, masterBaseFieldNames, masterUpdateFieldNames)
          psMaster.addBatch()
          // })
          psMaster.executeBatch()

          psSub = conn.prepareStatement(subSql)
          //tuples.foreach(tuple => {
          setPlaceholder(tuples, psSub, IdempotencyTable, subSql, subBaseFieldNames, subUpdateFieldNames)
          psSub.addBatch()
          // })
          psSub.executeBatch()
          conn.commit()
        } catch {
          case e: Throwable =>
            logger.warn("executeBatch error", e)
            errorTupleList ++= tupleList
            if (retryCount == 1)
              logger.info("violate tuple -----------" + tuples)
            try {
              conn.rollback()
            } catch {
              case e: Throwable => logger.warn("rollback error", e)
            }
        } finally {
          psMaster.clearBatch()
          psSub.clearBatch()
          if (psMaster != null)
            try {
              psMaster.close()
            } catch {
              case e: Throwable => logger.error("psMaster.close", e)
            }
          if (psSub != null)
            try {
              psSub.close()
            } catch {
              case e: Throwable => logger.error("psSub.close", e)
            }
        }
      })
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logger.error("SQLTransientConnectionException", e)
        errorTupleList ++= tupleList
      case e: Throwable =>
        logger.error("get connection failed", e)
        errorTupleList ++= tupleList

    } finally {
      if (null != conn)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logger.error("conn.close", e)
        }
    }
    errorTupleList.toList
  }
}
