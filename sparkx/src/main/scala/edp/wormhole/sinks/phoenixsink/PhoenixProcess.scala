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


package edp.wormhole.sinks.phoenixsink

import java.sql.{Connection, PreparedStatement, SQLTransientConnectionException}

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.common.db.DbConnection
import edp.wormhole.sinks.SinkProcessConfig
import edp.wormhole.ums.{UmsNamespace, _}
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.ums.UmsOpType._
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.memorystorage.ConfMemoryStorage._


class PhoenixProcess(sinkNamespace: String, sinkProcessConfig: SinkProcessConfig, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], specificConfig: PhoenixConfig
                     , connectionConfig: ConnectionConfig) extends EdpLogging {
  //  private lazy val fields = data.schema.fields

  //  val test: List[Seq[String]] =data.collect().map(r=>r.toSeq.map(p=>p.toString)).toList
  //  private lazy val schemaMap: Map[String, (StructField, Int)] = data.schema.fields.map(x => (x.name, (x, fields.indexOf(x)))).toMap

  private lazy val allFieldNames = schemaMap.keySet.toList
  private lazy val baseFieldNames = removeFieldNames(allFieldNames, Set(OP.toString).contains)
  private lazy val namespace = UmsNamespace(sinkNamespace)
  private lazy val tableName = if (namespace.database == "test") namespace.table.toUpperCase()
  else
    namespace.database.toUpperCase() + "." + namespace.table.toUpperCase()
  private lazy val fieldSqlTypeMap = schemaMap.map(kv => (kv._1, ums2dbType(kv._2._2)))
  //private lazy val connectionConfig = getDataStoreConnectionsMap(sinkNamespace)

  def removeFieldNames(allFieldNames: List[String], removeFn: String => Boolean): List[String] = allFieldNames.filterNot(removeFn)

  def doInsert(tupleList: Seq[Seq[String]]) = {
    val columnNames = baseFieldNames.map(field=>field.toUpperCase()).map(col=> "\""+col+"\"").mkString(", ")
    val sql = s"UPSERT INTO "+ "\""+tableName+ "\""+s" ($columnNames,"+ "\""+s"${UmsSysField.ACTIVE.toString.toUpperCase}"+"\""+") VALUES " +
      (1 to baseFieldNames.size + 1).map(_ => "?").mkString("(", ",", ")")
    val batchSize = specificConfig.`phoenix.BatchSize.get`
    executeSql(tupleList, sql, batchSize)
  }

  //
  //  def ums2DbType(fieldType: DataType): Int = fieldType match {
  //    case StringType => java.sql.Types.VARCHAR
  //    case IntegerType => java.sql.Types.INTEGER
  //    case LongType => java.sql.Types.BIGINT
  //    case FloatType => java.sql.Types.FLOAT
  //    case DoubleType => java.sql.Types.DOUBLE
  //    case BinaryType => java.sql.Types.BINARY
  //    //    case DecimalType => java.sql.Types.DECIMAL
  //    case BooleanType => java.sql.Types.BIT
  //    case DateType => java.sql.Types.DATE
  //    case TimestampType => java.sql.Types.TIMESTAMP
  //    case _ => throw new UnsupportedOperationException(s"Unknown Type: $fieldType")
  //  }

  private def psSetValue(fieldName: String, parameterIndex: Int, tuple: Seq[String], ps: PreparedStatement) = {
    val value = fieldValue(fieldName, schemaMap, tuple) //this what?
    if (value == null) ps.setNull(parameterIndex, fieldSqlTypeMap(fieldName))
    else ps.setObject(parameterIndex, value, fieldSqlTypeMap(fieldName)) //how to use it?
  }

  def executeSql(tupleList: Seq[Seq[String]], sql: String, batchSize: Int) = {
    var count = 0
    def setPlaceholder(tuple: Seq[String], ps: PreparedStatement) = {
      var parameterIndex: Int = 1
      count = count + tuple.length
      for (field <- baseFieldNames) {
        psSetValue(field, parameterIndex, tuple, ps)
        parameterIndex += 1
      }
      ps.setInt(parameterIndex,
        if (umsOpType(fieldValue(OP.toString, schemaMap, tuple).toString) == DELETE) UmsActiveType.INACTIVE
        else UmsActiveType.ACTIVE)

    }

    var ps: PreparedStatement = null
    //    val errorTupleList: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer.empty[Seq[String]]
    var conn: Connection = null


    try {
      conn = DbConnection.getConnection(connectionConfig)
      conn.setAutoCommit(false)

      ps = conn.prepareStatement(sql)
      //      val splitNum = data.count() / batchSize
      //      val splitWeights = if (splitNum == 0) Array(1.0) else (for (i <- 0 until splitNum.toInt) yield 1.0 / splitNum.toDouble).toArray[Double]

      tupleList.sliding(batchSize, batchSize).foreach(tuples => {
        tuples.foreach(tuple => {
          setPlaceholder(tuple, ps)
          ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()

      })
    } catch {
      case e: SQLTransientConnectionException => DbConnection.resetConnection(connectionConfig)
        logError("SQLTransientConnectionException", e)
        throw e
      case e: Throwable =>
        logError("get connection failed", e)
        //              errorTupleList ++= tupleList
        throw e
    } finally {
      println("this time :" + System.currentTimeMillis() + "tuplelist:" + count)
      if (ps != null)
        try {
          ps.close()
        } catch {
          case e: Throwable => logError("ps.close", e)
        }
      if (conn != null)
        try {
          conn.close()
          conn == null
        } catch {
          case e: Throwable => logError("conn.close", e)
        }
    }
    //      errorTupleList.toList
  }
}
