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


package edp.wormhole.common

import java.sql.ResultSet

import edp.wormhole.common.SparkSchemaUtils._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.{UmsField, UmsSchemaUtils}
//import org.apache.spark.rdd.EdpJdbcRDD
import org.apache.spark.sql.{SparkSession, _}

object JdbcPushdown extends EdpLogging {


//  def jdbcDf(session: SparkSession, sql: String, namespace: String, jsonSchema: String, jdbcConfig:ConnectionConfig): DataFrame = {
//    try {
//      val fields = UmsSchemaUtils.toUmsSchema(jsonSchema).fields_get
//      val schema = ums2sparkSchema(fields)
//      val jdbcRdd: EdpJdbcRDD[Row] = new EdpJdbcRDD[Row](session.sparkContext,
//        jdbcConfig,
//        sql,
//        rs => getRow(rs, fields))
//      val df = session.createDataFrame(jdbcRdd, schema)
//      jdbcRdd.unpersist()
//      df
//    } catch {
//      case e: Throwable => logError("getDataFrame", e); throw e
//    }
//  }


  private def getRow(rs: ResultSet, fields: Seq[UmsField]): Row = {
    val results: Seq[Any] = fields.map(field => {

      val fieldContent: Any = field.`type` match {
        case STRING => rs.getString(field.name)
        case INT => rs.getInt(field.name)
        case LONG => rs.getLong(field.name)
        case FLOAT => rs.getFloat(field.name)
        case DOUBLE => rs.getDouble(field.name)
        case BOOLEAN => rs.getBoolean(field.name)
        case DATE => rs.getDate(field.name)
        case DATETIME => rs.getTimestamp(field.name)
        case DECIMAL => rs.getBigDecimal(field.name)
        case BINARY => rs.getBytes(field.name)
      }
      fieldContent
    })

    Row(results: _*)
  }

}
