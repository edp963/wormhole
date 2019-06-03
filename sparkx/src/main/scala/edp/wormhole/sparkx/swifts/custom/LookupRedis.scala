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

import com.alibaba.fastjson.JSON
import edp.wormhole.dbdriver.redis.JedisConnection
import edp.wormhole.sparkx.common.{SparkSchemaUtils, SparkxUtils}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.umsFieldType
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import edp.wormhole.sparkx.common.SparkSchemaUtils._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LookupRedis extends EdpLogging {

  def transform(session: SparkSession, df: DataFrame, sqlConfig: SwiftsSql, sourceNamespace: String, sinkNamespace: String, connectionConfig: ConnectionConfig): DataFrame = {
//    val selectFields: Array[(String, String)] = sqlConfig.fields.get.split(",").map(field => {
//      val fields = field.split(":")
//      (fields(0).trim, fields(1).trim)
//    })

    val selectFields: Array[(String, String,String)] = sqlConfig.fields.get.split(",").map(field => {
      val fields = field.split(":")
      val fields1trim  = fields(1).trim
      if(fields1trim.toLowerCase.contains(" as ")){
        val asIndex = fields1trim.toLowerCase.indexOf(" as ")
        val fieldType = fields1trim.substring(0,asIndex).trim
        val newName = fields1trim.substring(asIndex+4).trim
        (fields(0).trim,fieldType,newName)
      }else{
        (fields(0).trim, fields(1).trim,fields(0).trim)
      }
    })

    //    val pushdownNamespace = sqlConfig.lookupNamespace.get
    val joinbyFiledsArray = sqlConfig.sourceTableFields.get
    val joinbyFileds = if(joinbyFiledsArray(0).contains("(")){
      val left = joinbyFiledsArray(0).toLowerCase.indexOf("(")
      val right = joinbyFiledsArray(0).toLowerCase.lastIndexOf(")")
      joinbyFiledsArray(0).substring(left+1,right)
    }else{
      joinbyFiledsArray(0)
    }

    val resultSchema = {
      var resultSchema: StructType = df.schema
      val addColumnType = selectFields.map { case (_, dataType,name) =>
        StructField(name, ums2sparkType(umsFieldType(dataType)))
      }
      addColumnType.foreach(column => resultSchema = resultSchema.add(column))
      resultSchema
    }

    val joinedRow: RDD[Row] = df.rdd.mapPartitions(partition => {
      val params: Seq[KVConfig] = connectionConfig.parameters.get
      val mode = params.filter(_.key == "mode").map(_.value).head
      val resultData = ListBuffer.empty[Row]

      val originalData: ListBuffer[Row] = partition.to[ListBuffer]
      if (originalData.nonEmpty) {
        val headRow = originalData.head
        val keyFieldContentDesc: Array[(Boolean, Int, String)] = joinbyFileds.split("\\+").map(fieldName => {
          if (!fieldName.startsWith("'")) {
            (true, headRow.fieldIndex(fieldName), "")
          } else {
            (false, 0, fieldName.substring(1, fieldName.length - 1))
          }
        })

        val keys: mutable.Seq[String] = originalData.map(row => {
          val schema: Array[StructField] = row.schema.fields
          val fieldContent = keyFieldContentDesc.map(fieldDesc => {
            if (fieldDesc._1) {
              if (row.get(fieldDesc._2) == null) "N/A" else row.get(fieldDesc._2)
            } else {
              fieldDesc._3
            }
          }).mkString("")
          fieldContent
        })

        val lookupValues = keys.map(key => {
          JedisConnection.get(connectionConfig.connectionUrl, connectionConfig.password, mode, key)
        })

        for (i <- originalData.indices) {
          val ori = originalData(i)
          val lookupRowContent = lookupValues(i)
          val originalArray: Array[Any] = ori.schema.fieldNames.map(name => ori.get(ori.fieldIndex(name)))
          val dbOutputArray = selectFields.map { case (name, dataType,newName) =>
            if (sqlConfig.sql.indexOf("(json)") < 0) {
              SparkSchemaUtils.s2sparkValue(lookupRowContent, umsFieldType(dataType))
            } else {
              //SqlOptType.PUSHDOWN_REDIS_JSON
              if (lookupRowContent == null) {
                SparkSchemaUtils.s2sparkValue(null, umsFieldType(dataType))
              } else {
                val json = JSON.parseObject(lookupRowContent)
                SparkSchemaUtils.s2sparkValue(json.getString(name), umsFieldType(dataType))
              }
            }
          }

          val row = new GenericRowWithSchema(originalArray ++ dbOutputArray, resultSchema)
          resultData.append(row)
        }
      }
      resultData.toIterator
    })
    session.createDataFrame(joinedRow, resultSchema)
  }

}
