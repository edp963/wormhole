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
package edp.wormhole.flinkx.swifts

import java.util.UUID

import edp.wormhole.common.feedback.ErrorPattern
import edp.wormhole.flinkx.common.{ExceptionConfig, FlinkxUtils}
import edp.wormhole.flinkx.swifts.custom.{LookupHbaseHelper, LookupKuduHelper, LookupRedisHelper}
import edp.wormhole.flinkx.util.FeedbackUtils
import edp.wormhole.ums.{UmsDataSystem, UmsProtocolType}
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

class LookupProcessElement(swiftsSql: SwiftsSql,
                           preSchemaMap: Map[String, (TypeInformation[_], Int)],
                           dbOutPutSchemaMap: Map[String, (String, String, Int)],
                           dataStoreConnectionsMap: Map[String, ConnectionConfig],
                           exceptionConfig: ExceptionConfig,
                           lookupTag: OutputTag[String]) extends ProcessFunction[Row, Seq[Row]] with java.io.Serializable{
  //private val outputTag = OutputTag[String]("lookupException")
  private val sourceTableFields: Array[String] = if (swiftsSql.sourceTableFields.isDefined) swiftsSql.sourceTableFields.get else null
  private val lookupTableFields = if (swiftsSql.lookupTableFields.isDefined) swiftsSql.lookupTableFields.get else null
  private val preRowSize = preSchemaMap.size
  private val resultRowSize = LookupHelper.getDbOutPutSchemaMap(swiftsSql).size + preRowSize
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def processElement(value: Row, ctx: ProcessFunction[Row, Seq[Row]]#Context, out: Collector[Seq[Row]]): Unit = {
    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    try {
      val lookupDataMap: mutable.HashMap[String, ListBuffer[Array[Any]]] = UmsDataSystem.dataSystem(lookupNamespace.split("\\.")(0).toLowerCase()) match {
        case UmsDataSystem.HBASE => LookupHbaseHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap, dbOutPutSchemaMap, sourceTableFields, dataStoreConnectionsMap)
        case UmsDataSystem.KUDU => LookupKuduHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap, dataStoreConnectionsMap, dbOutPutSchemaMap.keys.toList)
        case UmsDataSystem.REDIS => LookupRedisHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap, dataStoreConnectionsMap)
        case _ => LookupHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap, dataStoreConnectionsMap)
      }
      val joinFields = UmsDataSystem.dataSystem(lookupNamespace.split("\\.")(0).toLowerCase()) match {
        case UmsDataSystem.HBASE => LookupHbaseHelper.joinFieldsInRow(value, lookupTableFields, sourceTableFields, preSchemaMap).mkString("_")
        case UmsDataSystem.REDIS => LookupRedisHelper.joinFieldsInRow(value, swiftsSql, preSchemaMap)
        case _ => LookupHelper.joinFieldsInRow(value, lookupTableFields, sourceTableFields, preSchemaMap, false).mkString("_")
      }

      if (lookupDataMap == null || !lookupDataMap.contains(joinFields)) {
        val newRow = new Row(resultRowSize)
        for (pos <- 0 until preSchemaMap.size)
          newRow.setField(pos, value.getField(pos))
        for (pos <- preSchemaMap.size until resultRowSize)
          newRow.setField(pos, null)
        out.collect(Seq(newRow))
      } else {
        val newRows = lookupDataMap(joinFields).map { tupleList =>
          val newRow = new Row(resultRowSize)
          for (pos <- 0 until preSchemaMap.size)
            newRow.setField(pos, value.getField(pos))
          var newPos = preSchemaMap.size
          for (tupleIndex <- tupleList.indices) {
            newRow.setField(newPos, tupleList(tupleIndex))
            newPos += 1
          }
          newRow
        }
        out.collect(newRows)
      }
    } catch {
      case ex: Throwable =>
        logger.error("in doFlinkSql table query", ex)
        //out.collect(Seq(value))

        val dataInfoIt: Iterable[String] = preSchemaMap.map {
          case (schemaName, (_, pos)) =>
            FeedbackUtils.feedbackDataInfo(schemaName, pos, value)
        }
        val dataInfo = "{" + dataInfoIt.mkString(",") + "}"

        val errorMsg = FlinkxUtils.getFlowErrorMessage(dataInfo,
          exceptionConfig.sourceNamespace,
          exceptionConfig.sinkNamespace,
          1,
          ex,
          UUID.randomUUID().toString,
          UmsProtocolType.DATA_INCREMENT_DATA.toString,
          exceptionConfig.flowId,
          exceptionConfig.streamId,
          ErrorPattern.FlowError)

        ctx.output(lookupTag, errorMsg)
    }
  }
}
