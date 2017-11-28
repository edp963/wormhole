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

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.sinks.SourceMutationType._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.ums.UmsProtocolType._
import edp.wormhole.common.util.JsonUtils._
import org.joda.time.{DateTime, Seconds}
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.ums.UmsSysField

import scala.collection.mutable

class Data2DbSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig) = {
    logInfo("process KafkaLog2DbSnapshot")
    val dt1: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    //        println("repartition dataRepartitionRdd duration:   " + dt2 + " - "+ dt1 +" = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " seconds."))

    val sinkSpecificConfig = json2caseClass[DbConfig](sinkProcessConfig.specialConfig.get)
    val systemFieldsRename: String = sinkSpecificConfig.system_fields_rename
    val systemRenameMap: Map[String, String] = if (systemFieldsRename.isEmpty) null else {
      systemFieldsRename.split(",").map(t => {
        val keyValue = t.split(":").map(_.trim)
        (keyValue(0), keyValue(1))
      }).toMap
    }

    val renameSchema: collection.Map[String, (Int, UmsFieldType, Boolean)] = if (systemRenameMap == null) schemaMap else {
      schemaMap.map{case (name, (index, umsType, nullable)) =>
        name match {
          case "ums_id_" =>( systemRenameMap(UmsSysField.ID.toString), (index, umsType, nullable))
          case "ums_ts_" =>( systemRenameMap(UmsSysField.TS.toString), (index, umsType, nullable))
          case _ => (name, (index, umsType, nullable))
        }
      }.toMap
    }

    val sqlProcess = new SqlProcessor(sinkProcessConfig, renameSchema, sinkSpecificConfig, sinkNamespace, connectionConfig,systemRenameMap)
    val specialSqlProcessor: SpecialSqlProcessor = new SpecialSqlProcessor(sinkProcessConfig, schemaMap, sinkSpecificConfig, sinkNamespace, connectionConfig)

    SourceMutationType.sourceMutationType(sinkSpecificConfig.`db.mutation_type.get`) match {
      case INSERT_ONLY =>
        logInfo("INSERT_ONLY: " + sinkSpecificConfig.`db.mutation_type.get`)
        val errorList = sqlProcess.doInsert(tupleList, INSERT_ONLY)
        if (errorList.nonEmpty) throw new Exception(INSERT_ONLY + ",some data error ,data records=" + errorList.length)
      case IDEMPOTENCE_IDU =>
        logInfo("IDEMPOTENCE_IDU: " + sinkSpecificConfig.`db.mutation_type.get`)

        def checkAndCategorizeAndExecute(keysTupleMap: mutable.HashMap[String, Seq[String]]) = {
          if (keysTupleMap.nonEmpty) {
            val (insertInsertList, insertUpdateList, updateInsertList, updateUpdateList, deleteInsertList, deleteUpdateList, noneInsertList, noneUpdateList) =
              specialSqlProcessor.checkDbAndGetInsertUpdateDeleteList(keysTupleMap)
            logInfo(s"insertInsertList.size:${insertInsertList.size}")
            logInfo(s"insertUpdateList.size:${insertUpdateList.size}")
            logInfo(s"updateInsertList.size:${updateInsertList.size}")
            logInfo(s"updateUpdateList.size:${updateUpdateList.size}")
            logInfo(s"deleteInsertList.size:${deleteInsertList.size}")
            logInfo(s"deleteUpdateList.size:${deleteUpdateList.size}")
            logInfo(s"noneInsertList.size:${noneInsertList.size}")
            logInfo(s"noneUpdateList.size:${noneUpdateList.size}")

            val errorList = mutable.ListBuffer.empty[Seq[String]]
            errorList ++= specialSqlProcessor.contactDb(insertInsertList, INSERT_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(insertUpdateList, INSERT_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(updateInsertList, UPDATE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(updateUpdateList, UPDATE_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(deleteInsertList, DELETE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(deleteUpdateList, DELETE_UPDATE.toString)
            errorList ++= specialSqlProcessor.contactDb(noneInsertList, NONE_INSERT.toString)
            errorList ++= specialSqlProcessor.contactDb(noneUpdateList, NONE_UPDATE.toString)
            if (errorList.nonEmpty) throw new Exception(IDEMPOTENCE_IDU + ",some data error ,data records=" + errorList.length)
          }
        }

        val keysTupleMap = mutable.HashMap.empty[String, Seq[String]]
        for (tuple <- tupleList) {
          val keys = keyList2values(sinkProcessConfig.tableKeyList, renameSchema, tuple)
          keysTupleMap(keys) = tuple
        }
        checkAndCategorizeAndExecute(keysTupleMap)

      case _ =>
        logInfo("OTHER:" + sinkSpecificConfig.`db.mutation_type.get`)

        def checkAndCategorizeAndExecute(keysTupleMap: mutable.HashMap[String, Seq[String]]) = {
          if (keysTupleMap.nonEmpty) {
            logInfo("keysTupleMap all:" + keysTupleMap.size)
            val (insertList, updateList: List[Seq[String]]) = sqlProcess.checkDbAndGetInsertUpdateList(keysTupleMap)
            logInfo("insertList all:" + insertList.size)
            val insertErrorTupleList = sqlProcess.doInsert(insertList, I_U_D)
            logInfo("updateList all:" + updateList.size)
            val updateErrorTupleList = sqlProcess.doUpdate(updateList)
            if (insertErrorTupleList.nonEmpty || updateErrorTupleList.nonEmpty) throw new Exception(I_U_D + ",some data error ,data records=" + (insertErrorTupleList.length + updateErrorTupleList.length))
          }
        }

        val keysTupleMap = mutable.HashMap.empty[String, Seq[String]]
        for (tuple <- tupleList) {
          val keys = keyList2values(sinkProcessConfig.tableKeyList, renameSchema, tuple)
          keysTupleMap(keys) = tuple
        }
        checkAndCategorizeAndExecute(keysTupleMap)
    }
    val dt2: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    println("db duration:   " + dt2 + " - " + dt1 + " = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " seconds."))

  }
}
