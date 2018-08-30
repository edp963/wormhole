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

import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.sinks.{DbHelper, SourceMutationType}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.ums.UmsProtocolType._
import org.joda.time.{DateTime, Seconds}
import edp.wormhole.ums.UmsDataSystem.UmsDataSystem
import edp.wormhole.ums.{UmsNamespace, UmsOpType, UmsSysField}
import edp.wormhole.util.JsonUtils._
import org.apache.log4j.Logger

import scala.collection.mutable

class Data2DbSink extends SinkProcessor{
  private lazy val logger = Logger.getLogger(this.getClass)
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {

    logger.info("process KafkaLog2DbSnapshot")
    val dt1: DateTime = dt2dateTime(currentyyyyMMddHHmmss)

    val sinkSpecificConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        json2caseClass[DbConfig](sinkProcessConfig.specialConfig.get)
      else DbConfig()

    val systemFieldsRename: String = sinkSpecificConfig.system_fields_rename
    val systemRenameMutableMap = mutable.HashMap.empty[String, String]
    if (systemFieldsRename.nonEmpty) {
      systemFieldsRename.split(",").foreach(t => {
        val keyValue = t.split(":").map(_.trim)
        systemRenameMutableMap(keyValue(0)) = keyValue(1)
      })
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.ACTIVE.toString)) {
      systemRenameMutableMap(UmsSysField.ACTIVE.toString) = UmsSysField.ACTIVE.toString
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.ID.toString)) {
      systemRenameMutableMap(UmsSysField.ID.toString) = UmsSysField.ID.toString
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.TS.toString)) {
      systemRenameMutableMap(UmsSysField.TS.toString) = UmsSysField.TS.toString
    }
    val systemRenameMap = systemRenameMutableMap.toMap

    val renameSchema: collection.Map[String, (Int, UmsFieldType, Boolean)] = schemaMap.map { case (name, (index, umsType, nullable)) =>
      name match {
        case "ums_id_" => (systemRenameMap(name), (index, umsType, nullable))
        case "ums_ts_" => (systemRenameMap(name), (index, umsType, nullable))
        case _ => (name, (index, umsType, nullable))
      }
    }.toMap

    val namespace = UmsNamespace(sinkNamespace)
    val dataSys: UmsDataSystem = namespace.dataSys
    val tableName: String = namespace.table
    val allFieldNames: Seq[String] = schemaMap.keySet.toList
    val batchSize = sinkSpecificConfig.`db.sql_batch_size.get`
    val tableKeyNames: Seq[String] = sinkProcessConfig.tableKeyList
    val sysIdName = systemRenameMap(UmsSysField.ID.toString)
    val sourceMutationType = SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`)
    val specialSqlProcessor: SplitTableSqlProcessor = new SplitTableSqlProcessor(sinkProcessConfig, schemaMap, sinkSpecificConfig, sinkNamespace, connectionConfig)

    sourceMutationType match {
      case SourceMutationType.INSERT_ONLY =>
        logger.info("INSERT_ONLY: " + sinkSpecificConfig.`mutation_type.get`)
        val insertSql = SqlProcessor.getInsertSql(sourceMutationType, dataSys, tableName, systemRenameMap, allFieldNames)
        val errorList = SqlProcessor.executeProcess(tupleList, insertSql, batchSize, UmsOpType.INSERT, sourceMutationType, connectionConfig, allFieldNames,
          renameSchema, systemRenameMap, tableKeyNames, sysIdName)
        if (errorList.nonEmpty) throw new Exception(SourceMutationType.INSERT_ONLY + ",some data error ,data records=" + errorList.length)
      case SourceMutationType.SPLIT_TABLE_IDU =>
        logger.info("IDEMPOTENCE_IDU: " + sinkSpecificConfig.`mutation_type.get`)

        def checkAndCategorizeAndExecute(keysTupleMap: mutable.HashMap[String, Seq[String]]): Unit = {
          if (keysTupleMap.nonEmpty) {
            val (insertInsertList, insertUpdateList, updateInsertList, updateUpdateList, deleteInsertList, deleteUpdateList, noneInsertList, noneUpdateList) =
              specialSqlProcessor.checkDbAndGetInsertUpdateDeleteList(keysTupleMap)
            logger.info(s"insertInsertList.size:${insertInsertList.size}")
            logger.info(s"insertUpdateList.size:${insertUpdateList.size}")
            logger.info(s"updateInsertList.size:${updateInsertList.size}")
            logger.info(s"updateUpdateList.size:${updateUpdateList.size}")
            logger.info(s"deleteInsertList.size:${deleteInsertList.size}")
            logger.info(s"deleteUpdateList.size:${deleteUpdateList.size}")
            logger.info(s"noneInsertList.size:${noneInsertList.size}")
            logger.info(s"noneUpdateList.size:${noneUpdateList.size}")

            val errorList = mutable.ListBuffer.empty[Seq[String]]
            errorList ++= specialSqlProcessor.contactDb(insertInsertList, SourceMutationType.INSERT_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(insertUpdateList, SourceMutationType.INSERT_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(updateInsertList, SourceMutationType.UPDATE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(updateUpdateList, SourceMutationType.UPDATE_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(deleteInsertList, SourceMutationType.DELETE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(deleteUpdateList, SourceMutationType.DELETE_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(noneInsertList, SourceMutationType.NONE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(noneUpdateList, SourceMutationType.NONE_UPDATE.toString)
            if (errorList.nonEmpty) throw new Exception(SourceMutationType.SPLIT_TABLE_IDU + ",some data error ,data records=" + errorList.length)
          }
        }

        val keysTupleMap = mutable.HashMap.empty[String, Seq[String]]
        for (tuple <- tupleList) {
          val keys = SinkCommonUtils.keyList2values(sinkProcessConfig.tableKeyList, renameSchema, tuple)
          keysTupleMap(keys) = tuple
        }
        checkAndCategorizeAndExecute(keysTupleMap)

      case _ =>
        logger.info("OTHER:" + sinkSpecificConfig.`mutation_type.get`)
        val keysTupleMap = mutable.HashMap.empty[String, Seq[String]]
        for (tuple <- tupleList) {
          val keys = SinkCommonUtils.keyList2values(sinkProcessConfig.tableKeyList, renameSchema, tuple)
          keysTupleMap(keys) = tuple
        }

        val rsKeyUmsIdMap: mutable.Map[String, Long] = SqlProcessor.selectDataFromDbList(keysTupleMap, sinkNamespace, tableKeyNames, sysIdName, dataSys, tableName, connectionConfig, schemaMap)
        val (insertList, updateList) = SqlProcessor.splitInsertAndUpdate(rsKeyUmsIdMap, keysTupleMap, tableKeyNames, sysIdName, renameSchema)

        logger.info("insertList all:" + insertList.size)
        val insertSql = SqlProcessor.getInsertSql(sourceMutationType, dataSys, tableName, systemRenameMap, allFieldNames)
        val insertErrorTupleList = SqlProcessor.executeProcess(insertList, insertSql, batchSize, UmsOpType.INSERT, sourceMutationType, connectionConfig, allFieldNames,
          renameSchema, systemRenameMap, tableKeyNames, sysIdName)
        logger.info("updateList all:" + updateList.size)
        val fieldNamesWithoutParNames = DbHelper.removeFieldNames(allFieldNames.toList, sinkSpecificConfig.partitionKeyList.contains)
        val updateFieldNames = DbHelper.removeFieldNames(fieldNamesWithoutParNames, tableKeyNames.contains)
        val updateSql = SqlProcessor.getUpdateSql(dataSys, tableName, systemRenameMap, updateFieldNames, tableKeyNames, sysIdName)
        val updateErrorTupleList = SqlProcessor.executeProcess(updateList, updateSql, batchSize, UmsOpType.UPDATE, sourceMutationType, connectionConfig, updateFieldNames,
          renameSchema, systemRenameMap, tableKeyNames, sysIdName)
        if (insertErrorTupleList.nonEmpty || updateErrorTupleList.nonEmpty) throw new Exception(SourceMutationType.I_U_D + ",some data error ,data records=" + (insertErrorTupleList.length + updateErrorTupleList.length))

    }
    val dt2: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    println("db duration:   " + dt2 + " - " + dt1 + " = " + (Seconds.secondsBetween(dt1, dt2).getSeconds % 60 + " seconds."))

  }
}
