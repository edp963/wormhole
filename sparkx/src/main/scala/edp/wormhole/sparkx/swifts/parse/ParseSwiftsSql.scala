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


package edp.wormhole.sparkx.swifts.parse

import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.swifts.{ParseSwiftsSqlInternal, SqlOptType}
import edp.wormhole.util.swifts.SwiftsSql

object ParseSwiftsSql extends EdpLogging {

  def parse(sqlStr: String,
            sourceNamespace: String, //sourceNamespace is rule
            sinkNamespace: String,
            validity: Boolean,
            dataType: String,
            mutation: String): Option[Array[SwiftsSql]] = {
    if (sqlStr.trim.nonEmpty) {
      val sqlStrArray = sqlStr.trim.replaceAll("\r", " ").replaceAll("\n", " ").replaceAll("\t", " ").split(";").map(str => {
        val trimStr = str.trim
        if (trimStr.startsWith(SqlOptType.PUSHDOWN_SQL.toString)) trimStr.substring(12).trim
        else if (trimStr.startsWith(SqlOptType.PARQUET_SQL.toString)) trimStr.substring(11).trim
        else trimStr
      })
      val swiftsSqlArr = getSwiftsSql(sqlStrArray, sourceNamespace, sinkNamespace, validity, dataType, mutation) //sourcenamespace is rule
      swiftsSqlArr
    } else {
      None
    }
  }

  private def getSwiftsSql(sqlStrArray: Array[String],
                           sourceNamespace: String, //sourcenamespace is rule
                           sinkNamespace: String,
                           validity: Boolean,
                           dataType: String,
                           mutation: String): Option[Array[SwiftsSql]] = {
    val swiftsSqlList = Some(sqlStrArray.map(sqlStrEle => {
      val sqlStrEleTrim = sqlStrEle.trim + " " //to fix no where clause bug, e.g select a, b from table;
      logInfo("sqlStrEle:::" + sqlStrEleTrim)
      if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.JOIN.toString) || sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.INNER_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.LEFT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.LEFT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.RIGHT_JOIN.toString)) {
        ParseSwiftsSqlInternal.getJoin(SqlOptType.RIGHT_JOIN, sqlStrEleTrim, sourceNamespace, sinkNamespace)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.UNION.toString)) {
        ParseSwiftsSqlInternal.getUnion(sqlStrEleTrim, sourceNamespace, sinkNamespace, validity, dataType, mutation)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.SPARK_SQL.toString)) {
        ParseSwiftsSqlInternal.getSparkSql(sqlStrEleTrim, sourceNamespace, validity, dataType, mutation)
      } else if (sqlStrEleTrim.toLowerCase.startsWith(SqlOptType.CUSTOM_CLASS.toString)) {
        getCustomClass(sqlStrEleTrim)
      } else {
        logError("optType:" + sqlStrEleTrim + " is not supported")
        throw new Exception("wong operation data type:" + sqlStrEleTrim)
      }
    })
    )
    swiftsSqlList
  }


  private def getCustomClass(sqlStrEle: String): SwiftsSql = {
    val classFullName = sqlStrEle.substring(sqlStrEle.indexOf("=") + 1).trim
    ConfMemoryStorage.registerSwiftsTransformReflectMap(classFullName)
    SwiftsSql(SqlOptType.CUSTOM_CLASS.toString, None, classFullName, None, None, None, None, None)
  }
}
