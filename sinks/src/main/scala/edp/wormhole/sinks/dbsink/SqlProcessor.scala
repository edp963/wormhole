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

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.sinks.SourceMutationType.SourceMutationType
import edp.wormhole.sinks.kafkasink.OracleSequenceConfig
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.ums.UmsDataSystem.UmsDataSystem
import edp.wormhole.ums.{UmsFieldType, UmsOpType, UmsSysField, _}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsOpType._
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SqlProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)

  def selectMysqlSql(tupleCount: Int, tableKeyNames: Seq[String], tableName: String, sysIdName: String): String = {
    val keysString = tableKeyNames.map(tk =>s"""`$tk`""").mkString(",")
    val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
    s"SELECT $keysString, $sysIdName FROM `$tableName` WHERE ($keysString) IN " +
      (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
  }

  def selectOracleSql(tupleCount: Int, tableKeyNames: Seq[String], tableName: String, sysIdName: String): String = {
    val keysString = tableKeyNames.map(name => "\"" + name.toUpperCase + "\"").mkString(",")
    val keyQuestionMarks: String = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
    s"""SELECT $keysString, $sysIdName FROM "${tableName.toUpperCase}" WHERE ($keysString) IN """ +
      (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
        (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
      }).mkString(s" OR ($keysString) IN ")
  }

  def selectPostgresSql(tupleCount: Int, tableKeyNames: Seq[String], tableName: String, sysIdName: String): String = {
    val keysString = tableKeyNames.map(tk =>s"""$tk""").mkString(",")
    val keyQuestionMarks = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
    s"SELECT $keysString, $sysIdName FROM $tableName WHERE ($keysString) IN " +
      (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
  }

  def selectClickHouseSql(tupleCount:Int,tableKeyNames:Seq[String],tableName:String,sysIdName:String):String={
    val keysString = tableKeyNames.map(tk =>s"""$tk""").mkString(",")
    val keyQuestionMarks =(1 to tableKeyNames.size).map( _ => "?").mkString("(", ",", ")")
    s"SELECT $keysString, $sysIdName FROM `$tableName` WHERE ($keysString) IN " +
      (1 to tupleCount).map(_ => keyQuestionMarks).mkString("(", ",", ")")
  }

  def selectOtherSql(tupleCount: Int, tableKeyNames: Seq[String], tableName: String, sysIdName: String): String = {
    val keysString = tableKeyNames.mkString(",")
    val keyQuestionMarks: String = (1 to tableKeyNames.size).map(_ => "?").mkString("(", ",", ")")
    s"""SELECT $keysString, $sysIdName FROM ${tableName.toUpperCase} WHERE ($keysString) IN """ +
      (1 to tupleCount).sliding(1000, 1000).map(slidSize => {
        (1 to slidSize.size).map(_ => keyQuestionMarks).mkString("(", ",", ")")
      }).mkString(s" OR ($keysString) IN ")
  }

  def splitInsertAndUpdate(rsKeyUmsIdMap: mutable.Map[String, Long], keysTupleMap: mutable.HashMap[String, Seq[String]], tableKeyNames: Seq[String], sysIdName: String,
                           renameSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): (List[Seq[String]], List[Seq[String]]) = {
    val updateList = mutable.ListBuffer.empty[Seq[String]]
    val insertList = mutable.ListBuffer.empty[Seq[String]]

    keysTupleMap.foreach(keysTuple => {
      val (keysId, tuple) = keysTuple
      if (rsKeyUmsIdMap.contains(keysId)) {
        val tupleId = umsFieldValue(tuple(renameSchemaMap(sysIdName)._1), UmsFieldType.LONG).asInstanceOf[Long]
        val rsId = rsKeyUmsIdMap(keysId)
        if (tupleId > rsId)
          updateList.append(tuple)
      } else
        insertList.append(tuple)
    })
    (insertList.toList, updateList.toList)
  }

  def selectDataFromDbList(keysTupleMap: mutable.HashMap[String, Seq[String]], sinkNamespace: String, tableKeyNames: Seq[String],
                           sysIdName: String, dataSys: UmsDataSystem, tableName: String, connectionConfig: ConnectionConfig,
                           schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): mutable.Map[String, Long] = {
    val tupleList = keysTupleMap.values.toList
    val rsKeyUmsIdMap: mutable.Map[String, Long] = mutable.HashMap.empty[String, Long]
    if (tupleList.nonEmpty) {
      var ps: PreparedStatement = null
      var rs: ResultSet = null
      var conn: Connection = null
      try {
        logger.info(s"datasys:$dataSys tableName:$tableName tableKeysNames:$tableKeyNames")
        val sql = dataSys match {
          case UmsDataSystem.MYSQL => selectMysqlSql(tupleList.size, tableKeyNames, tableName, sysIdName)
          case UmsDataSystem.ORACLE => selectOracleSql(tupleList.size, tableKeyNames, tableName, sysIdName)
          case UmsDataSystem.POSTGRESQL => selectPostgresSql(tupleList.size, tableKeyNames, tableName, sysIdName)
          case UmsDataSystem.CLICKHOUSE  => selectClickHouseSql(tupleList.size, tableKeyNames, tableName, sysIdName)
          case _ => selectOtherSql(tupleList.size, tableKeyNames, tableName, sysIdName)
        }
        logger.info("select sql:" + sql)
        conn = DbConnection.getConnection(connectionConfig)
        ps = conn.prepareStatement(sql)
        var parameterIndex = 1
        for (tuple <- tupleList)
          for (key <- tableKeyNames) {
            psSetValue(key, parameterIndex, tuple, ps, schemaMap)
            parameterIndex += 1
          }
        logger.info("before query")
        rs = ps.executeQuery()
        logger.info("finish query")

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
          val umsId = rs.getLong(sysIdName)
          rsKeyUmsIdMap(keysId) = umsId
        }
        rsKeyUmsIdMap
      } catch {
        case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
          logger.error("SQLTransientConnectionException", e)
          throw e
        case e: Throwable =>
          logger.error("execute select failed", e)
          throw e
      } finally {
        if (rs != null)
          try {
            rs.close()
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
    } else {
      rsKeyUmsIdMap
    }
  }

  def getInsertSql(sourceMutationType: SourceMutationType, dataSys: UmsDataSystem, tableName: String, systemRenameMap: Map[String, String],
                   allFieldNames: Seq[String], oracleSequenceConfig: Option[OracleSequenceConfig]=None): String = {
    println("oracleSequenceConfig:"+oracleSequenceConfig)
    val columnNames = getSqlField(allFieldNames, systemRenameMap, UmsOpType.INSERT, dataSys)
    val oracleColumnNames = getSqlField(allFieldNames, systemRenameMap, UmsOpType.INSERT, dataSys)
    val sql = dataSys match {
      case UmsDataSystem.MYSQL | UmsDataSystem.CLICKHOUSE =>
        s"INSERT INTO `$tableName` ($columnNames) VALUES " + (1 to allFieldNames.size).map(_ => "?").mkString("(", ",", ")")
      case UmsDataSystem.ORACLE =>
        if (oracleSequenceConfig.nonEmpty) {
          val fieldName = oracleSequenceConfig.get.field_name
          val sequenceName = oracleSequenceConfig.get.sequence_name + ".NEXTVAL"
          s"""INSERT INTO ${tableName.toUpperCase} ($fieldName,$oracleColumnNames) VALUES """ + (1 to allFieldNames.size).map(_ => "?").mkString("(" + sequenceName + " ,", ",", ")")
        } else
          s"""INSERT INTO ${tableName.toUpperCase} ($oracleColumnNames) VALUES """ + (1 to allFieldNames.size).map(_ => "?").mkString("(", ",", ")")
      case _ => s"""INSERT INTO ${tableName.toUpperCase} ($oracleColumnNames) VALUES """ + (1 to allFieldNames.size).map(_ => "?").mkString("(", ",", ")")
    }

    logger.info("insert sql " + sql)
    sql
  }

  def getSqlField(allFieldNames: Seq[String], systemRenameMap: Map[String, String], opType: UmsOpType, dataSys: UmsDataSystem): String = {
    allFieldNames.map(n => {
      val newn = if (n == UmsSysField.OP.toString) {
        systemRenameMap(UmsSysField.ACTIVE.toString)
      } else if (n == UmsSysField.ID.toString) {
        systemRenameMap(UmsSysField.ID.toString)
      } else if (n == UmsSysField.TS.toString) {
        systemRenameMap(UmsSysField.TS.toString)
      } else n

      if (opType == UmsOpType.INSERT) {
        if (dataSys == UmsDataSystem.MYSQL)
          s"""`$newn`"""
        else
          s"""$newn"""
      } else {
        if (dataSys == UmsDataSystem.MYSQL)
          s"""`$newn`=?"""
        else
          s"""$newn=?"""
      }
    }).mkString(", ")
  }

  def executeProcess(tupleList: Seq[Seq[String]], sql: String, batchSize: Int, optType: UmsOpType, sourceMutationType: SourceMutationType,
                     connectionConfig: ConnectionConfig, fieldNames: Seq[String], renameSchema: collection.Map[String, (Int, UmsFieldType, Boolean)],
                     systemRenameMap: Map[String, String], tableKeyNames: Seq[String], sysIdName: String): Seq[Seq[String]] = {
    if (tupleList.nonEmpty) {
      val errorTupleList = executeSql(tupleList, sql, optType, batchSize, sourceMutationType,
        connectionConfig, fieldNames, renameSchema, systemRenameMap, tableKeyNames, sysIdName)
      if (errorTupleList.nonEmpty) {
        val newErrorTupleList = if (batchSize == 1) errorTupleList else executeSql(errorTupleList, sql, optType, 1, sourceMutationType,
          connectionConfig, fieldNames, renameSchema, systemRenameMap, tableKeyNames, sysIdName)
        newErrorTupleList.foreach(data => logger.info(optType + ",data:" + data))
        newErrorTupleList
      } else ListBuffer.empty[List[String]]
    } else ListBuffer.empty[List[String]]
  }

  def getUpdateSql(dataSys: UmsDataSystem, tableName: String, systemRenameMap: Map[String, String], updateFieldNames: Seq[String], tableKeyNames: Seq[String], sysIdName: String): String = {
    //    val batchSize = specificConfig.`db.sql_batch_size.get`
    val fields = getSqlField(updateFieldNames, systemRenameMap, UmsOpType.UPDATE, dataSys)
    val sql = dataSys match {
      case UmsDataSystem.MYSQL => s"UPDATE `$tableName` SET " + fields + " WHERE " + tableKeyNames.map(key => s"`$key`=?").mkString(" AND ") + s" AND ($sysIdName<? or $sysIdName is null)"
      case _ => s"UPDATE ${tableName.toUpperCase()} SET " + fields + " WHERE " + tableKeyNames.map(key => s"$key=?").mkString(" AND ") + s" AND $sysIdName<? "
    }
    logger.info("@update sql " + sql)
    sql
  }


  private def psSetValue(fieldName: String, parameterIndex: Int, tuple: Seq[String], ps: PreparedStatement,
                         schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): Unit = {
    val value = fieldValue(fieldName, schemaMap, tuple)
    if (value == null) ps.setNull(parameterIndex, ums2dbType(schemaMap(fieldName)._2))
    else ps.setObject(parameterIndex, value, ums2dbType(schemaMap(fieldName)._2))
  }

  def setPlaceholder(opType: UmsOpType, tuple: Seq[String], ps: PreparedStatement, fieldNames: Seq[String],
                     renameSchema: collection.Map[String, (Int, UmsFieldType, Boolean)], systemRenameMap: Map[String, String],
                     tableKeyNames: Seq[String], sysIdName: String): Unit = {
    var parameterIndex: Int = 1
    for (field <- fieldNames) {
      if (field == UmsSysField.OP.toString) {
        ps.setInt(parameterIndex,
          if (tuple(renameSchema(field)._1) == DELETE.toString) UmsActiveType.INACTIVE
          else UmsActiveType.ACTIVE)
      } else if (field == UmsSysField.ID.toString || field == UmsSysField.TS.toString) {
        psSetValue(systemRenameMap(field), parameterIndex, tuple, ps, renameSchema)
      } else psSetValue(field, parameterIndex, tuple, ps, renameSchema)
      parameterIndex += 1
    }
    if (opType == UPDATE) {
      for (i <- tableKeyNames.indices) {
        psSetValue(tableKeyNames(i), parameterIndex, tuple, ps, renameSchema)
        parameterIndex += 1
      }
      psSetValue(sysIdName, parameterIndex, tuple, ps, renameSchema)
    }
  }


  def executeSql(tupleList: Seq[Seq[String]], sql: String, opType: UmsOpType, batchSize: Int, sourceMutationType: SourceMutationType,
                 connectionConfig: ConnectionConfig, fieldNames: Seq[String], renameSchema: collection.Map[String, (Int, UmsFieldType, Boolean)],
                 systemRenameMap: Map[String, String], tableKeyNames: Seq[String], sysIdName: String): List[Seq[String]] = {
    var ps: PreparedStatement = null
    val errorTupleList: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer.empty[Seq[String]]
    var conn: Connection = null
    val index = 0 - batchSize
    try {
      conn = DbConnection.getConnection(connectionConfig)
      conn.setAutoCommit(false)
      logger.info("create connection successfully,batchsize:" + batchSize + ",tupleList:" + tupleList.size)
      ps = conn.prepareStatement(sql)
      for (i <- tupleList.indices) {
        setPlaceholder(opType, tupleList(i), ps, fieldNames, renameSchema, systemRenameMap, tableKeyNames, sysIdName)
        ps.addBatch()
      }
      logger.info("execute batch start***")
      ps.executeBatch()
      logger.info("execute batch end***")
      conn.commit()
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logger.error("SQLTransientConnectionException", e)
        logger.info("out batch ")
        if (index <= 0) errorTupleList ++= tupleList
        else errorTupleList ++= tupleList.takeRight(tupleList.size - index)
      case e: Throwable =>
        logger.error("executeBatch error ", e)
        errorTupleList ++= tupleList
        if (batchSize == 1)
          logger.info("violate tuple -----------" + tupleList)
        try {
          conn.rollback()
        } catch {
          case e: Throwable => logger.warn("rollback error", e)
        }
        logger.error("get connection failed", e)
    } finally {
      ps.clearBatch()
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

    errorTupleList.toList
  }
}
