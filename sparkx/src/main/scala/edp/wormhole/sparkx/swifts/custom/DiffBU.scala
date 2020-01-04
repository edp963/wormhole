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


package edp.wormhole.sparkx.swifts.custom

import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.ums.UmsSysField
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

class DiffBU extends SwiftsInterface with EdpLogging {
  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    val originalSchema: StructType = df.schema
    var resultSchema = originalSchema
    originalSchema.foreach(structField => {
      val name = structField.name
      val beforeName = "ums_b_" + name
      if (name != UmsSysField.OP.toString
        && name != UmsSysField.ID.toString
        && name != UmsSysField.UID.toString
        && name != UmsSysField.TS.toString) {
        val addColumn = StructField(beforeName, structField.dataType)
        resultSchema = resultSchema.add(addColumn)
      }
    })

    val originalSchemaMap: Map[String, Int] = originalSchema.fieldNames.map(name => (name, resultSchema.fieldIndex(name))).toMap

    val resultRowRdd: RDD[Row] = df.rdd.mapPartitionsWithIndex((index, it) => {
      val data: Seq[Row] = it.toList
      val BeforeDataMap = mutable.HashMap.empty[Long, Row]
      data.foreach(row => {
        val umsOp = row.get(originalSchemaMap(UmsSysField.OP.toString)).asInstanceOf[String]
        if (umsOp == "b") {
          val umsId = row.get(originalSchemaMap(UmsSysField.ID.toString)).asInstanceOf[Long]
          BeforeDataMap(umsId) = row
        }
      })

      val resultList = mutable.ListBuffer.empty[Row]
      data.foreach(row => {
        val umsOp: String = row.get(originalSchemaMap(UmsSysField.OP.toString)).asInstanceOf[String]
        umsOp match {
          case "i" | "d" =>
            val resultDataRow: Array[Any] = resultSchema.fieldNames.map { case name =>
              if (originalSchemaMap.contains(name)) {
                row.get(originalSchemaMap(name))
              } else {
                null
              }
            }
            resultList += new GenericRowWithSchema(resultDataRow, resultSchema)
          case "u" =>
            val umsId = row.get(originalSchemaMap(UmsSysField.ID.toString)).asInstanceOf[Long]
            if (!BeforeDataMap.contains(umsId)) {
              logError("do not contains ums_id_: " + umsId + "for op type b  " + "index: " + index)
            } else {
              val resultDataRow = resultSchema.fieldNames.map { case name =>
                val beforeRow = BeforeDataMap(umsId)
                if (name.startsWith("ums_b_")) {
                  beforeRow.get(originalSchemaMap(name.substring("ums_b_".length)))
                } else {
                  row.get(originalSchemaMap(name))
                }
              }
              resultList += new GenericRowWithSchema(resultDataRow, resultSchema)
            }
          case "b" =>
        }
      }
      )
      resultList.toIterator
    })
    session.createDataFrame(resultRowRdd, resultSchema)
  }
}
