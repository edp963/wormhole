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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import edp.wormhole.flinkx.swifts.common._
import edp.wormhole.ums.UmsDataSystem

class LookupMapper(swiftsSql: SwiftsSql, preSchemaMap: Map[String, (TypeInformation[_], Int)],dbOutPutSchemaMap: Map[String, (String, String, Int)], dataStoreConnectionsMap: Map[String, ConnectionConfig]) extends RichMapFunction[Row, Seq[Row]] with java.io.Serializable {

  private val sourceTableFields: Array[String] = if (swiftsSql.sourceTableFields.isDefined) swiftsSql.sourceTableFields.get else null
  private val lookupTableFields = if (swiftsSql.lookupTableFields.isDefined) swiftsSql.lookupTableFields.get else null
  private val preRowSize = preSchemaMap.size
  private val resultRowSize = LookupHelper.getDbOutPutSchemaMap(swiftsSql).size + preRowSize

  override def map(value: Row): Seq[Row] = {
    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val lookupDataMap: mutable.HashMap[String, ListBuffer[Array[Any]]] = UmsDataSystem.dataSystem(lookupNamespace.split("\\.")(0).toLowerCase()) match {
      case UmsDataSystem.HBASE=>LookupHbaseHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap,dbOutPutSchemaMap,sourceTableFields,dataStoreConnectionsMap)
      case _=>LookupHelper.covertResultSet2Map(swiftsSql, value, preSchemaMap,dataStoreConnectionsMap)
    }
    val joinFields =UmsDataSystem.dataSystem(lookupNamespace.split("\\.")(0).toLowerCase()) match{
      case UmsDataSystem.HBASE=>LookupHbaseHelper.joinFieldsInRow(value, lookupTableFields, sourceTableFields, preSchemaMap).mkString("_")
      case _=>LookupHelper.joinFieldsInRow(value, lookupTableFields, sourceTableFields, preSchemaMap).mkString("_")
    }

    if (lookupDataMap == null || !lookupDataMap.contains(joinFields)) {
      val newRow = new Row(resultRowSize)
      for (pos <- 0 until preSchemaMap.size)
        newRow.setField(pos, value.getField(pos))
      for (pos <- preSchemaMap.size until resultRowSize)
        newRow.setField(pos, null)
      Seq(newRow)
    } else {
      lookupDataMap(joinFields).map { tupleList =>
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
    }
  }
}
