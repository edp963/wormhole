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

import java.util.UUID

import edp.wormhole.sparkx.common.SparkxUtils
import edp.wormhole.sparkx.memorystorage.ConfMemoryStorage
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.swifts.custom.{LookupHbase, LookupKudu, LookupRedis}
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, SwiftsSpecialProcessConfig, WormholeConfig}
import edp.wormhole.swifts.{ConnectionMemoryStorage, SqlOptType}
import edp.wormhole.ums.UmsDataSystem
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer

object SwiftsTransform extends EdpLogging {
  //source real
  def transform(session: SparkSession, sourceNamespace: String, sinkNamespace: String, df: DataFrame, matchSourceNamespace: String, config: WormholeConfig): DataFrame = {
    val uuid = UUID.randomUUID().toString
    val swiftsLogic: SwiftsProcessConfig = ConfMemoryStorage.getSwiftsLogic(matchSourceNamespace, sinkNamespace)
    val swiftsSqlArr = swiftsLogic.swiftsSql
    val dataSetShow = swiftsLogic.datasetShow
    val batchSize =
      (if (swiftsLogic.specialConfig.isDefined && !swiftsLogic.specialConfig.get.isEmpty)
        JsonUtils.json2caseClass[SwiftsSpecialProcessConfig](swiftsLogic.specialConfig.get)
      else new SwiftsSpecialProcessConfig()).batchSize

    val dataSetShowNum = if (dataSetShow.get) swiftsLogic.datasetShowNum.get else -1
    var currentDf = df
    var cacheDf = df
    var firstInLoop = true
    val tmpTableNameList = ListBuffer.empty[String]
    if (swiftsSqlArr.isDefined) {
      val swiftsArr: Array[SwiftsSql] = swiftsSqlArr.get

      swiftsArr.foreach(f = operate => {
        val lookupNamespace = if (operate.lookupNamespace.isDefined) operate.lookupNamespace.get else null
        val sourceTableFields = if (operate.sourceTableFields.isDefined) operate.sourceTableFields.get else null
        val lookupTableFields = if (operate.lookupTableFields.isDefined) operate.lookupTableFields.get else null
        val sql = operate.sql.trim
        try {
          SqlOptType.toSqlOptType(operate.optType) match {
            case SqlOptType.CUSTOM_CLASS =>
              val (obj, method,param) = ConfMemoryStorage.getSwiftsTransformReflectValue(operate.sql)

              /*currentDf = if(method.getParameterCount == 3) {
                method.invoke(obj, session, currentDf, swiftsLogic).asInstanceOf[DataFrame]
              } else if(method.getParameterCount == 4) {
                method.invoke(obj, session, currentDf, swiftsLogic,param).asInstanceOf[DataFrame]
              } else {
                method.invoke(obj, session, currentDf, swiftsLogic,param,config).asInstanceOf[DataFrame]
              }*/

              currentDf = if(param.isEmpty){method.invoke(obj, session, currentDf, swiftsLogic, config, sourceNamespace, sinkNamespace).asInstanceOf[DataFrame]}
              else {
                logInfo("Transform get Param :" + param)
                method.invoke(obj, session, currentDf, swiftsLogic, param, config, sourceNamespace, sinkNamespace).asInstanceOf[DataFrame]}

            case SqlOptType.JOIN | SqlOptType.LEFT_JOIN | SqlOptType.RIGHT_JOIN =>
              if (ConfMemoryStorage.existStreamLookup(matchSourceNamespace, sinkNamespace, lookupNamespace)) {
                // lookup Namespace is also match rule format .*.*
                val path = config.stream_hdfs_address.get + "/" + "swiftsparquet" + "/" + config.spark_config.stream_id + "/" + matchSourceNamespace.replaceAll("\\*", "-") + "/" + sinkNamespace + "/streamLookupNamespace"
                val (tableNameArrMD5, allTableReady) = DataframeObtain.createDfAndViewFromParquet(matchSourceNamespace, lookupNamespace, sinkNamespace, session, path)
                if (allTableReady) {
                  try {
                    tmpTableNameList ++= tableNameArrMD5
                    val newSql = SqlBinding.getSlidingUnionSql(session, currentDf, sourceTableFields, lookupTableFields, sql)
                    logInfo(uuid + ",lookupStreamMap JOIN newSql@:" + newSql)
                    val df1 = session.sql(newSql)
                    currentDf = DataframeObtain.getJoinDf(currentDf, session, operate, df1)
                  } catch {
                    case e: Throwable =>
                      logError("getJoinDf", e)
                      tmpTableNameList.foreach(name => session.sqlContext.dropTempTable(name))
                      throw e
                  }
                } else {
                  return df // return original dataframe
                }
              } else {
                //select id as newid,name,age,degree as degree1 from customer where  swifts_join_fields_values
                val schema = operate.fields.get
                logInfo("schema::" + schema)
                val connectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsMap(lookupNamespace)
                val lookupNameSpaceArr: Array[String] = lookupNamespace.split("\\.")
                assert(lookupNameSpaceArr.length == 3, "lookup namespace is invalid in pattern list sql")
                //  val jsonSchema = JsonSchemaObtain.getJsonSchema(schema)
                UmsDataSystem.dataSystem(lookupNameSpaceArr(0).toLowerCase()) match {
                  case UmsDataSystem.CASSANDRA =>
                    currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.CASSANDRA, Some(batchSize))
                  case UmsDataSystem.ORACLE =>
                    currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.ORACLE, Some(batchSize))
                  case UmsDataSystem.HBASE =>
                    currentDf = LookupHbase.transform(session, currentDf, operate, sourceNamespace, sinkNamespace, connectionConfig)
                  case UmsDataSystem.REDIS =>
                    currentDf = LookupRedis.transform(session, currentDf, operate, sourceNamespace, sinkNamespace, connectionConfig)
                  case UmsDataSystem.KUDU =>
                    currentDf = LookupKudu.transform(session, currentDf, operate, sourceNamespace, sinkNamespace, connectionConfig, Some(batchSize))
                  case _ =>
                    currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.MYSQL, Some(batchSize))
                }
              }
            case SqlOptType.SPARK_SQL =>
              val tmpTableName = "a" + UUID.randomUUID().toString.replaceAll("-", "")
              currentDf = DataFrameTransform.getMapDf(session, sql + " ", sourceNamespace, uuid, currentDf, dataSetShow.get, dataSetShowNum, tmpTableName) //to solve no where clause bug. select a, b from table;
              tmpTableNameList += tmpTableName
            case SqlOptType.UNION =>
              val columns = sourceTableFields.map(name => new Column(name))
              currentDf = currentDf.repartition(config.rdd_partition_number, columns: _*)
              val schema = operate.fields.get
              val connectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsMap(lookupNamespace)
              val lookupNameSpaceArr = lookupNamespace.split("\\.")
              assert(lookupNameSpaceArr.size == 3, "lookup namespace is invalid in pattern list sql")
              //       val jsonSchema = JsonSchemaObtain.getJsonSchema(schema)
              UmsDataSystem.dataSystem(lookupNameSpaceArr(0).toLowerCase()) match {
                case UmsDataSystem.CASSANDRA =>
                  currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.CASSANDRA, Some(batchSize))
                case UmsDataSystem.ORACLE =>
                  currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.ORACLE, Some(batchSize))
                case UmsDataSystem.KUDU =>
                  currentDf = DataFrameTransform.getKUDUUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, Some(batchSize))
                case _ =>
                  currentDf = DataFrameTransform.getDbJoinOrUnionDf(session, currentDf, sourceTableFields, lookupTableFields, sql, connectionConfig, schema, operate, UmsDataSystem.MYSQL, Some(batchSize))
              }
            case _ => logWarning(uuid + ",operate.optType:" + operate.optType + " is not supported")
          }
        } catch {
          case e1: Throwable =>
            logAlert("transform", e1)
            SparkxUtils.unpersistDataFrame(currentDf)
            throw e1
        }

        currentDf.cache()
        //logInfo(uuid + ",atfer operation count and then show:" + currentDf.count()) //ui:stage line
        if (dataSetShow.get) currentDf.show(dataSetShowNum)
        if (firstInLoop) firstInLoop = false else cacheDf.unpersist()
        cacheDf = currentDf
        if (tmpTableNameList.nonEmpty) {
          tmpTableNameList.foreach(name => session.sqlContext.dropTempTable(name))
          tmpTableNameList.clear()
        }

      }
      )
      cacheDf.unpersist()
    }
    currentDf
  }
}
