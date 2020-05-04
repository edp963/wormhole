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

import java.sql.{Connection, ResultSet, SQLTransientConnectionException}

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.kuduconnection.KuduConnection
import edp.wormhole.sparkx.common.SparkSchemaUtils._
import edp.wormhole.sparkx.common.{SparkSchemaUtils, SparkxUtils}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.swifts.custom.LookupKudu
import edp.wormhole.swifts.SqlOptType
import edp.wormhole.ums.UmsFieldType.{UmsFieldType, _}
import edp.wormhole.ums.{UmsDataSystem, UmsFieldType, UmsOpType, UmsSysField}
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import edp.wormhole.util.{CommonUtils, DateUtils}
import org.apache.kudu.client.KuduTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DataFrameTransform extends EdpLogging {
  def getKUDUUnionDf(session: SparkSession,
                     currentDf: DataFrame,
                     sourceTableFields: Array[String],
                     lookupTableFields: Array[String],
                     sql: String,
                     connectionConfig: ConnectionConfig,
                     schemaStr: String,
                     operate: SwiftsSql,
                     batchSize: Option[Int] = None): DataFrame = {
    logInfo("getKUDUUnionDf,batchSize=" + batchSize)
    val database = operate.lookupNamespace.get.split("\\.")(2)
    val fromIndex = operate.sql.indexOf(" from ")
    val afterFromSql = operate.sql.substring(fromIndex + 6).trim
    val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
    val tableName = KuduConnection.getTableName(tmpTableName, database)
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    val table: KuduTable = client.openTable(tableName)
    val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
    logInfo(s"query data from table $tableName success")
    KuduConnection.closeClient(client)

    val resultSchema = currentDf.schema

    val joinedRow: RDD[Row] = currentDf.rdd.mapPartitions(partition => {
      KuduConnection.initKuduConfig(connectionConfig)

      val originalDatas: ListBuffer[Row] = partition.to[ListBuffer]
      logInfo("getKUDUUnionDf,originalDatas.size=" + originalDatas.size)
      val resultDatas: ListBuffer[Row] = ListBuffer.empty[Row]
      originalDatas.foreach(resultDatas.append(_))
      val fieldsNameArray = LookupKudu.getFieldsArrayUnion(operate.fields.get)
      val selectFieldOriginalNameArray: Seq[String] = fieldsNameArray.filter(_._3 != "default").map(_._1).toList
      //(asField,(kuduField, kuduType)
      val original2AsNameMap: Map[String, (String, String)] = fieldsNameArray.map(tuple => (tuple._2, (tuple._1, tuple._3))).toMap

      val sourceFieldNameArray = operate.sourceTableFields.get
      val lookupFieldNameArray = operate.lookupTableFields.get
      if (lookupFieldNameArray.length == 1) {
        val keyType = UmsFieldType.umsFieldType(KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)(lookupFieldNameArray.head))
        val keySchemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
        keySchemaMap(lookupFieldNameArray.head) = (0, keyType, true)

        originalDatas.grouped(batchSize.get).foreach((subList: mutable.Seq[Row]) => {
          logInfo("getKUDUUnionDf,originalDatas.grouped(batchSize.get).foreach,subList.size=" + subList.size)
          val tupleList: mutable.Seq[List[String]] = subList.map(row => {
            sourceFieldNameArray.toList.map(field => {
              val tmpKey = row.get(row.fieldIndex(field))
              if (tmpKey == null) null.asInstanceOf[String]
              else tmpKey.toString
            })

          }).filter((keys: Seq[String]) => {
            !keys.contains(null)
          })
          val dataMapFromDb = KuduConnection.doQueryMultiByKeyListInBatch(tmpTableName, database, connectionConfig.connectionUrl,
            lookupFieldNameArray.head, tupleList, keySchemaMap.toMap, selectFieldOriginalNameArray, batchSize.get,
            tableSchemaInKudu, operate.lookupTableConstantCondition)

          getKuduUnionResult(resultDatas, dataMapFromDb, sourceTableFields, resultSchema, original2AsNameMap)
        })
      } else {
        val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
        val table: KuduTable = client.openTable(tableName)
        originalDatas.map(row => {
          val tuple: Array[String] = sourceFieldNameArray.map(field => {
            val tmpKey = row.get(row.fieldIndex(field))
            if (tmpKey == null) null.asInstanceOf[String]
            else tmpKey.toString
          }).filter(key => {
            key != null
          })
          val dataMapFromDb = KuduConnection.doQueryMultiByKey(operate.lookupTableFields.get, tuple.toList, tableSchemaInKudu,
            client, table, selectFieldOriginalNameArray, operate.lookupTableConstantCondition)

          getKuduUnionResult(resultDatas, dataMapFromDb, sourceTableFields, resultSchema, original2AsNameMap)
        })
        logInfo(s"query data from table $tableName success")
        KuduConnection.closeClient(client)
      }
      resultDatas.toIterator
    })

    session.createDataFrame(joinedRow, resultSchema)
  }

  def getDbJoinOrUnionDf(session: SparkSession, currentDf: DataFrame, sourceTableFields: Array[String], lookupTableFields: Array[String], sql: String, connectionConfig: ConnectionConfig, schemaStr: String, operate: SwiftsSql, sqlType: UmsDataSystem.Value, batchSize: Option[Int] = None): DataFrame = {
    var index = -1
    val dbOutPutSchemaMap: Map[String, (String, Int)] = schemaStr.split(",").map(str => {
      val arr = str.split(":")
      index += 1
      (arr(0), (arr(1), index))
    }).toMap //order is not same as input order !!!

    val inputDfSchema = currentDf.schema
    val resultSchema: StructType = SqlOptType.toSqlOptType(operate.optType) match {
      case SqlOptType.JOIN | SqlOptType.INNER_JOIN | SqlOptType.LEFT_JOIN =>
        var afterJoinSchema: StructType = inputDfSchema
        val addColumnType = dbOutPutSchemaMap.map { case (name, (dataType, _)) => StructField(name, ums2sparkType(umsFieldType(dataType))) }
        addColumnType.foreach(column => afterJoinSchema = afterJoinSchema.add(column))
        afterJoinSchema
      case SqlOptType.UNION => inputDfSchema
    }

    val joinedRow: RDD[Row] = currentDf.rdd.mapPartitions(partition => {
      val originalDatas: ListBuffer[Row] = partition.to[ListBuffer]
      if (batchSize.nonEmpty)
        originalDatas.grouped(batchSize.get).flatMap(originalData => {
          getDataByBatch(originalData, sourceTableFields, lookupTableFields, sql, connectionConfig, resultSchema, operate, sqlType, dbOutPutSchemaMap).toSeq
        })
      else
        getDataByBatch(originalDatas, sourceTableFields, lookupTableFields, sql, connectionConfig, resultSchema, operate, sqlType, dbOutPutSchemaMap)
    })

    session.createDataFrame(joinedRow, resultSchema)
  }

  def getDataByBatch(originalData: ListBuffer[Row], sourceTableFields: Array[String], lookupTableFields: Array[String], sql: String, connectionConfig: ConnectionConfig, resultSchema: StructType, operate: SwiftsSql, sqlType: UmsDataSystem.Value, dbOutPutSchemaMap: Map[String, (String, Int)]) = {
    val sourceJoinFieldsContent: Set[String] = originalData.map(row => {
      val schema: Array[StructField] = row.schema.fields
      val lookupFieldsLength = lookupTableFields.length
      val fieldContent = sourceTableFields.map(fieldName => {
        val index = row.fieldIndex(fieldName)
        val value = SparkxUtils.getFieldContentByTypeForSql(row, schema, index)
        if (value != null) value else "N/A"
      }).mkString(",")
      if (!fieldContent.contains("N/A")) {
        if (lookupFieldsLength == 1) fieldContent else "(" + fieldContent + ")"
      } else null
    }).flatMap(Option[String]).toSet //delete join fields contained null


    val executeSql: String =
      sqlType match {
        case UmsDataSystem.ORACLE =>
          SqlBinding.getSlidingUnionSql(sourceJoinFieldsContent, lookupTableFields, sql) //delete join fields contained null
        case UmsDataSystem.MYSQL | UmsDataSystem.ES | UmsDataSystem.MONGODB | UmsDataSystem.H2 | UmsDataSystem.PHOENIX | UmsDataSystem.VERTICA | UmsDataSystem.POSTGRESQL | UmsDataSystem.GREENPLUM | UmsDataSystem.CLICKHOUSE =>
          SqlBinding.getMysqlSql(sourceJoinFieldsContent, lookupTableFields, sql) //delete join fields contained null
        case UmsDataSystem.CASSANDRA =>
          if (sourceJoinFieldsContent.nonEmpty) {
            if (lookupTableFields.length == 1)
              SqlBinding.getCassandraSqlSingleField(sourceJoinFieldsContent, lookupTableFields(0), sql)
            else
              sql
          } else null
      }

    var dataMapFromDb: mutable.HashMap[String, ListBuffer[Array[String]]] = mutable.HashMap.empty[String, ListBuffer[Array[String]]]

    var resultSet: ResultSet = null
    var conn: Connection = null
    try {
      var result: Iterator[Row] = ListBuffer.empty[Row].toIterator
      logInfo("executeSql:" + executeSql)
      conn = DbConnection.getConnection(connectionConfig)
      val stmt = conn.createStatement
      if (executeSql != null) {
        if (sqlType == UmsDataSystem.CASSANDRA && lookupTableFields.length > 1) {
          sourceJoinFieldsContent.foreach(eachJoinFieldsContent => {
            val cassandraquery = SqlBinding.getCassandraSqlMutilField(eachJoinFieldsContent, lookupTableFields, executeSql)
            //              logInfo("cassandraquery::"+cassandraquery)

            resultSet = stmt.executeQuery(cassandraquery)
            dataMapFromDb ++= getDataMap(resultSet, dbOutPutSchemaMap, operate.lookupTableFieldsAlias.get)
          })
        } else {
          resultSet = stmt.executeQuery(executeSql)
          dataMapFromDb = getDataMap(resultSet, dbOutPutSchemaMap, operate.lookupTableFieldsAlias.get)
        }
      }
      if (originalData.nonEmpty) {
        SqlOptType.toSqlOptType(operate.optType) match {
          case SqlOptType.JOIN | SqlOptType.INNER_JOIN =>
            result = getInnerJoinResult(originalData, dataMapFromDb, sourceTableFields, dbOutPutSchemaMap, resultSchema)
          case SqlOptType.LEFT_JOIN =>
            result = getLeftJoinResult(originalData, dataMapFromDb, sourceTableFields, dbOutPutSchemaMap, resultSchema)
          case SqlOptType.UNION =>
            result = getUnionResult(originalData, dataMapFromDb, sourceTableFields, dbOutPutSchemaMap, resultSchema).toIterator
          case _ =>
        }
      }
      result
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logError("SQLTransientConnectionException", e)
        throw e
      case e: Throwable =>
        logError("execute select failed", e)
        throw e
    } finally {
      if (null != conn)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logError("conn.close", e)
        }
    }
  }

  def getUnionResult(originalData: ListBuffer[Row],
                     dataMapFromDb: mutable.HashMap[String, ListBuffer[Array[String]]],
                     sourceTableFields: Array[String],
                     dbOutPutSchemaMap: Map[String, (String, Int)],
                     resultSchema: StructType): ListBuffer[Row] = {
    val resultData: ListBuffer[Row] = originalData
    val originalSchemaArr = resultSchema.fieldNames.map(name => (name, resultSchema.apply(resultSchema.fieldIndex(name)).dataType)) //order is same every time?
    if (dataMapFromDb != null)
      dataMapFromDb.foreach { case (_, tupleLists) =>
        tupleLists.foreach(tupleList => {
          val unionArr = originalSchemaArr.map { case (name, dataType) =>
            if (dbOutPutSchemaMap.contains(name)) {
              toTypedValue(tupleList(dbOutPutSchemaMap(name)._2), dataType)
            } else {
              if (UmsSysField.OP.toString == name) {
                toTypedValue(UmsOpType.INSERT.toString, dataType)
              } else {
                dataType match {
                  case IntegerType | LongType | FloatType | DoubleType | DecimalType.SYSTEM_DEFAULT => toTypedValue("0", dataType)
                  case _ => toTypedValue(null, dataType)
                }
              }
            }
          }
          val row = new GenericRowWithSchema(unionArr, resultSchema)
          resultData.append(row)
        })
      }
    resultData
  }

  def getKuduUnionResult(originalData: ListBuffer[Row],
                         dataMapFromDb: mutable.Map[String, ListBuffer[Map[String, (Any, String)]]],
                         sourceTableFields: Array[String],
                         resultSchema: StructType,
                         original2AsNameMap: Map[String, (String, String)]) = {
    logInfo("getKuduUnionResult,dataMapFromDb.size=" + dataMapFromDb.size + ",originalData.size" + originalData.size)
    //    val resultData: ListBuffer[Row] = originalData
    val originalSchemaArr: Array[(String, DataType)] = resultSchema.fieldNames.map(name => (name.toLowerCase, resultSchema.apply(resultSchema.fieldIndex(name)).dataType)) //order is same every time?
    if (dataMapFromDb != null && dataMapFromDb.nonEmpty)
      dataMapFromDb.foreach { case (_, tupleLists) =>
        tupleLists.foreach(tupleMap => {
          val outputArray = ListBuffer.empty[Any]
          originalSchemaArr.foreach { case (name, originalType) =>
            if(original2AsNameMap.contains(name) && original2AsNameMap(name)._2 == "default") {
              val constantValueOrg = original2AsNameMap(name)._1
              //去除单引号/双引号
              val constantValue = constantValueOrg.substring(1, constantValueOrg.length - 1)
              outputArray.append(SparkSchemaUtils.toTypedValue(constantValue, originalType))
            } else if (!original2AsNameMap.contains(name) && UmsSysField.OP.toString == name) {
              outputArray.append(
                SparkSchemaUtils.s2sparkValue(UmsOpType.INSERT.toString, UmsFieldType.STRING))
            } else {
              val kuduName = original2AsNameMap(name)._1
              val tmpValue: (Any, String) = tupleMap(kuduName)
              //              logInfo(s"asNameMap:($name,$asName),originalType:$originalType,originalVal:$tmpValue")
              val newTmpValue = umsFieldType(tmpValue._2) match {
                case STRING => if (tmpValue._1 == null) "" else tmpValue._1.toString
                case INT | LONG | FLOAT | DOUBLE | DECIMAL | BINARY => if (tmpValue._1 == null) "0" else tmpValue._1.toString
                case DATE | DATETIME => if (tmpValue._1 == null) DateUtils.currentyyyyMMddHHmmss else tmpValue._1.toString
                case BOOLEAN => if (tmpValue._1 == null) "false" else tmpValue._1.toString
              }

              val (fieldValue, fieldType) = if (originalType.toString == "DateTimeType" || originalType.toString == "TimestampType") {
                val v = newTmpValue
                val newValue = if (v.length == 13) {
                  v.toLong * 1000
                } else if (v.length == 19) {
                  v.toLong * 1000 * 1000
                } else {
                  v.toLong
                }
                (DateUtils.yyyyMMddHHmmss(newValue), UmsFieldType.DATETIME)
              } else {
                (newTmpValue, umsFieldType(tmpValue._2))
              }
              //              logInfo(s"fieldValue:$fieldValue fieldType:$fieldType")
              outputArray.append(
                SparkSchemaUtils.s2sparkValue(if (tmpValue._1 == null) null else fieldValue, fieldType))
            }

          }
          originalData.append(new GenericRowWithSchema(outputArray.toArray, resultSchema))
        })
      }
    //    originalData
  }

  def getLeftJoinResult(originalData: ListBuffer[Row], dataMapFromDb: mutable.HashMap[String, ListBuffer[Array[String]]], sourceTableFields: Array[String], dbOutPutSchemaMap: Map[String, (String, Int)], resultSchema: StructType): Iterator[Row] = {
    val resultData = ListBuffer.empty[Row]
    val originalDataSize = originalData.size
    originalData.foreach(iter => {
      val sch: Array[StructField] = iter.schema.fields
      val originalJoinFields = if (sourceTableFields.nonEmpty) {
        sourceTableFields.map(joinFields => {
          val dataType = sch.filter(t => t.name == joinFields).head.dataType.toString
          val field = iter.get(iter.fieldIndex(joinFields)) //.toString
          if (field != null) {
            if (dataType != "StringType") {
              field.toString.split("\\.")(0)
            } else field.toString
          } else "N/A" // source flow is empty in some fields
        }).mkString("_")
      } else "all"
      if (dataMapFromDb == null || !dataMapFromDb.contains(originalJoinFields)) {
        val originalArray: Array[Any] = iter.schema.fieldNames.map(name => iter.get(iter.fieldIndex(name)))
        val dbOutputArray: Array[Any] = new Array[Any](dbOutPutSchemaMap.size)
        val row = new GenericRowWithSchema(originalArray ++ dbOutputArray, resultSchema)
        resultData.append(row)
      } else {
        val originalArray: Array[Any] = iter.schema.fieldNames.map(name => iter.get(iter.fieldIndex(name)))
        dataMapFromDb(originalJoinFields).foreach { tupleList =>
          val dbOutputArray = dbOutPutSchemaMap.map { case (_, (dataType, index)) =>
            s2sparkValue(tupleList(index), umsFieldType(dataType))
          }.toArray
          val row = new GenericRowWithSchema(originalArray ++ dbOutputArray, resultSchema)
          resultData.append(row)
        }
      }
    })
    // orignialData.clear
    val resultDataSize = resultData.size
    logInfo(s"getLeftJoinResult,originalDataSize:$originalDataSize,resultDataSize:$resultDataSize")
    resultData.toIterator
  }

  def getInnerJoinResult(orignialData: ListBuffer[Row], dataMapFromDb: mutable.HashMap[String, ListBuffer[Array[String]]], sourceTableFields: Array[String], dbOutPutSchemaMap: Map[String, (String, Int)], resultSchema: StructType): Iterator[Row] = {
    val resultData = ListBuffer.empty[Row]
    if (dataMapFromDb != null)
      orignialData.foreach(iter => {
        val originalJoinFields = if (sourceTableFields.nonEmpty) {
          sourceTableFields.map(joinFields => {
            val field = iter.get(iter.fieldIndex(joinFields))
            if (field != null) field.toString
            else {
              logWarning("Inner join, join fields " + joinFields + " is null ")
              val information = iter.schema.fieldNames.map(name => (name, iter.get(iter.fieldIndex(name))))
              information.foreach { case (name, value) => logWarning(name + "          " + value) }
              "N/A"
            } // source flow is empty in some fields
          }).mkString("_")
        } else "all"
        if (dataMapFromDb.contains(originalJoinFields)) {
          val originalArray: Array[Any] = iter.schema.fieldNames.map(name => iter.get(iter.fieldIndex(name)))
          dataMapFromDb(originalJoinFields).foreach { tupleList =>
            val dbOutputArray = dbOutPutSchemaMap.map { case (_, (dataType, index)) =>
              s2sparkValue(tupleList(index), umsFieldType(dataType))
            }.toArray
            val row = new GenericRowWithSchema(originalArray ++ dbOutputArray, resultSchema)
            resultData.append(row)
          }
        }
      })
    resultData.toIterator
  }


  def getDataMap(rs: ResultSet, dbOutPutSchemaMap: Map[String, (String, Int)], lookupTableFieldsAlias: Array[String]): mutable.HashMap[String, ListBuffer[Array[String]]] = {
    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[String]]]
    while (rs.next) {
      val tmpMap = mutable.HashMap.empty[String, String]

      val arrayBuf: Array[String] = Array.fill(dbOutPutSchemaMap.size) {
        ""
      }
      dbOutPutSchemaMap.foreach { case (name, (dataType, index)) =>
        val value = rs.getObject(name)
        //logInfo("getDataMap value:" + value)
        arrayBuf(index) = if (value != null) {
          if (dataType == UmsFieldType.BINARY.toString) CommonUtils.base64byte2s(rs.getBytes(name))
          //if (dataType == UmsFieldType.BINARY.toString) CommonUtils.base64byte2s(value.asInstanceOf[Array[Byte]])
          else value.toString
        } else null
        //logInfo("getDataMap arrayBuf(index):" + arrayBuf(index))
        tmpMap(name) = arrayBuf(index)
      }

      if(lookupTableFieldsAlias.nonEmpty) {
        val joinFieldsAsKey = lookupTableFieldsAlias.map(name => {
          if (tmpMap.contains(name)) tmpMap(name) else rs.getObject(name).toString
        }).mkString("_")

        if (!dataTupleMap.contains(joinFieldsAsKey)) {
          dataTupleMap(joinFieldsAsKey) = ListBuffer.empty[Array[String]]
        }
        dataTupleMap(joinFieldsAsKey) += arrayBuf
      } else {
        if(!dataTupleMap.contains("all")) {
          dataTupleMap("all") = ListBuffer.empty[Array[String]]
        }
        dataTupleMap("all") += arrayBuf
      }
    }
    dataTupleMap
  }


  def getMapDf(session: SparkSession, sql: String, sourceNamespace: String, uuid: String, tmpLastDf: DataFrame, dataSetShow: Boolean, dataSetShowNum: Int, tmpTableName: String): DataFrame = {
    val tableName = sourceNamespace.split("\\.")(3)
    val mapSql = sql.replaceAll(" " + tableName + " ", " " + tmpTableName + " ")
    logInfo(uuid + ",MAP SQL:" + mapSql)
    tmpLastDf.createOrReplaceTempView(tmpTableName)
    try {
      session.sql(mapSql)
    } catch {
      case e: Throwable =>
        logError("getMapDf", e)
        session.sqlContext.dropTempTable(tmpTableName)
        throw e
    }
  }

  //  private[transform] def getCassandraJoinDf(session: SparkSession, tmpLastDf: DataFrame, sourceTableFields: Array[String], uuid: String, operate: SwiftsSql, jsonSchema: String, lookupTableFields: Array[String], sql: String, sourceNamespace: String, connectionConfig: ConnectionConfig): DataFrame = {
  //    var condition = tmpLastDf(sourceTableFields(0)).isNotNull
  //    val length = sourceTableFields.length
  //    for (i <- 1 until length) {
  //      condition = condition && tmpLastDf(sourceTableFields(i)).isNotNull
  //    }
  //    val tmpDf = tmpLastDf.filter(condition)
  //    if (tmpDf.count() == 0) {
  //      logInfo(uuid + ",CASSANDRA JOIN tmpDf.count() == 0")
  //      SqlOptType.toSqlOptType(operate.optType) match {
  //        case SqlOptType.JOIN | SqlOptType.INNER_JOIN =>
  //          val streamSchema: StructType = tmpDf.schema
  //          val tmp: Seq[UmsField] = toUmsSchema(jsonSchema).fields_get
  //          var newStreamSchema = streamSchema
  //          tmp.foreach(umsField => {
  //            newStreamSchema = newStreamSchema.add(umsField.name, ums2sparkType(umsField.`type`), umsField.nullable.get, Metadata.empty)
  //          })
  //          session.createDataFrame(session.sparkContext.emptyRDD[Row], newStreamSchema)
  //        case SqlOptType.LEFT_JOIN | SqlOptType.RIGHT_JOIN =>
  //          var resultDataFrame = tmpLastDf
  //          val tmp: Seq[UmsField] = toUmsSchema(jsonSchema).fields_get
  //          tmp.foreach(field => {
  //            resultDataFrame = tmpLastDf.withColumn(field.name, functions.lit(null).cast(ums2sparkType(field.`type`)))
  //          })
  //          resultDataFrame
  //      }
  //    } else {
  //      val newSql = SqlBinding.getCassandraSql(session, tmpLastDf, sourceTableFields, lookupTableFields, sql)
  //      logInfo(uuid + ",CASSANDRA JOIN newSql@:" + newSql)
  //      val df1 = jdbcDf(session, newSql, sourceNamespace, jsonSchema, connectionConfig)
  //      DataframeObtain.getJoinDf(tmpLastDf, session, operate, df1)
  //    }
  //  }


  //  def getCassandraUnionDf(session: SparkSession, sourceTableFields: Array[String], tmpLastDf: DataFrame, lookupTableFields: Array[String], sql: String, uuid: String, sourceNamespace: String, jsonSchema: String, connectionConfig: ConnectionConfig): DataFrame = {
  //    var condition = tmpLastDf(sourceTableFields(0)).isNotNull
  //    val length = sourceTableFields.length
  //    for (i <- 1 until length) {
  //      condition = condition && tmpLastDf(sourceTableFields(i)).isNotNull
  //    }
  //    val tmpDf = tmpLastDf.filter(condition)
  //    if (tmpDf.count() != 0) {
  //      val newSql = SqlBinding.getCassandraSql(session, tmpLastDf, sourceTableFields, lookupTableFields, sql)
  //      logInfo(uuid + ",CASSANDRA UNION1 newSql@:" + newSql)
  //      DataframeObtain.getUnionDf(tmpLastDf, session, sourceNamespace, jsonSchema, connectionConfig, newSql)
  //    } else {
  //      logInfo(uuid + ",CASSANDRA UNION1 tmpDf.count()==0")
  //      tmpLastDf
  //    }
  //  }
}
