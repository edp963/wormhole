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


package edp.wormhole.sparkx.swifts.transform

import java.math.BigInteger
import java.security.MessageDigest

import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.swifts.SqlOptType
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._

import scala.collection.mutable


object DataframeObtain extends EdpLogging {
  private [transform] def createDfAndViewFromParquet(matchSourceNamespace: String, lookupNamespace: String, sinkNamespace: String, session: SparkSession, pathPrefix: String) = {
    val lookupNamespaceArr = lookupNamespace.split(",").map(_.trim) //only 4 fields in namespace
    val tableNameArrMD5 = new mutable.ListBuffer[String]()
    var allTableReady = true
    lookupNamespaceArr.foreach(lookupNs => {
      val configuration = new Configuration()
      val path = pathPrefix + "/" + lookupNs.replaceAll("\\*", "-")
      if (!HdfsUtils.isParquetPathReady(configuration, path)) {
        allTableReady = false
      }
    }
    )
    if (allTableReady) {
      try{
      lookupNamespaceArr.foreach(lookupNs => {
        val fileName = matchSourceNamespace + "_" + sinkNamespace + "_" + lookupNs //source_sink_lookup
        val path = pathPrefix + "/" + lookupNs.replaceAll("\\*", "-")
        val lookupDf = session.read.parquet(path)
        val md = MessageDigest.getInstance("MD5")
        md.update(fileName.getBytes())
        val tmpTableName = new BigInteger(1, md.digest()).toString(16)
        tableNameArrMD5.append(tmpTableName)
        lookupDf.createOrReplaceTempView(tmpTableName)
      })
      }catch{
        case e:Throwable=>
          logError("createOrReplaceTempView",e)
          allTableReady=false
          tableNameArrMD5.foreach(name => session.sqlContext.dropTempTable(name))
      }
    }
    (tableNameArrMD5.toList, allTableReady)
  }

  private [transform] def getJoinDf(lastDf: DataFrame, session: SparkSession, operate: SwiftsSql, df1: DataFrame): DataFrame = {
    var tmpLastDf = lastDf
    val sourceTableFields = operate.sourceTableFields.get
    val joinType = SqlOptType.toSqlOptType(operate.optType) match {
      case SqlOptType.LEFT_JOIN => "left_outer"
      case SqlOptType.RIGHT_JOIN => "right_outer"
      case SqlOptType.JOIN => "inner"
    }

    val lookupTableFieldsAlias = operate.lookupTableFieldsAlias.get
    if (sourceTableFields.length == 1) {
      tmpLastDf = tmpLastDf.join(df1, tmpLastDf(sourceTableFields(0)) === df1(lookupTableFieldsAlias(0)), joinType)
    } else {
      val tmpLength = sourceTableFields.length
      var condition = tmpLastDf(sourceTableFields(0)) === df1(lookupTableFieldsAlias(0))
      for (i <- 1 until tmpLength) {
        condition = condition && tmpLastDf(sourceTableFields(i)) === df1(lookupTableFieldsAlias(i))
      }
      tmpLastDf = tmpLastDf.join(df1, condition, joinType)
    }
    tmpLastDf
  }

//  private [transform] def getUnionDf(lastDf: DataFrame, session: SparkSession, namespace: String, jsonSchema: String, connectionConfig: ConnectionConfig, newSql: String): DataFrame = {
//    var tmpLastDf = lastDf
//    logInfo("newSql@:" + newSql)
//    var df1 = jdbcDf(session, newSql, namespace, jsonSchema, connectionConfig)
//    val lookupNameTypeMap = new mutable.HashMap[String, String]
//    val name_type_main = tmpLastDf.dtypes
//    val name_type_lookup: Array[(String, String)] = df1.dtypes
//    for (ele <- name_type_lookup) {
//      lookupNameTypeMap(ele._1) = ele._2
//    }
//    val notInSourceFields = new ListBuffer[(String, String)]()
//    for (ele <- name_type_main) {
//      if (!lookupNameTypeMap.contains(ele._1)) {
//        notInSourceFields.append((ele._1, ele._2))
//      } else {
//        lookupNameTypeMap.remove(ele._1)
//      }
//    }
//    for (ele <- notInSourceFields) {
//      ele._2 match {
//        case "DoubleType" => df1 = df1.withColumn(ele._1, functions.lit(0))
//        case "FloatType" => df1 = df1.withColumn(ele._1, functions.lit(0))
//        case "LongType" => df1 = df1.withColumn(ele._1, functions.lit(0))
//        case "IntegerType" => df1 = df1.withColumn(ele._1, functions.lit(0))
//        case "DecimalType" => df1 = df1.withColumn(ele._1, functions.lit(0))
//        case "StringType" => df1 = df1.withColumn(ele._1, functions.lit(null))
//        case "DateType" => df1 = df1.withColumn(ele._1, functions.lit(null))
//        case "TimestampType" => df1 = df1.withColumn(ele._1, functions.lit(null))
//        case _ => logWarning("dataType " + ele._2 + " is not supported, column name is " + ele._1)
//      }
//    }
//
//    lookupNameTypeMap.foreach(nameType => {
//      df1 = df1.drop(nameType._1)
//    })
//    val selectedFields = tmpLastDf.dtypes.map(pair => pair._1)
//    df1 = df1.select(selectedFields.head, selectedFields.tail: _*)
//    tmpLastDf = tmpLastDf.union(df1)
//    tmpLastDf
//  }
}
