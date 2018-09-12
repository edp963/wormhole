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


package org.apache.spark.rdd

import java.sql.{Connection, PreparedStatement, ResultSet}

import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.util.config.ConnectionConfig
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class EdpJdbcRDD[T: ClassTag](
                               sc: SparkContext,
                               jdbcConfig:ConnectionConfig,
                               sql: String,
                               mapRow: (ResultSet) => T = EdpJdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  private class JdbcPartition(idx: Int) extends Partition {
    override def index: Int = idx
  }

  override def getPartitions: Array[Partition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    (0 until 1).map { i =>
      new JdbcPartition(i)
    }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {

    context.addTaskCompletionListener { context => closeIfNeeded() }
    val part: JdbcPartition = thePart.asInstanceOf[JdbcPartition]
    val conn: Connection = DbConnection.getConnection(jdbcConfig)
    val stmt: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }

    var rs: ResultSet = _
    try {
      rs = stmt.executeQuery()
    } catch {
      case e: Throwable =>
        logError("stmt.executeQuery:" + sql, e)
        throw e
    }


    override def getNext(): T = {
      try {
        if (rs.next()) {
          mapRow(rs)
        } else {
          finished = true
          null.asInstanceOf[T]
        }
      } catch {
        case e: Throwable =>
          logError("getNext:" + sql, e)
          throw e
      }
    }

    override def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
      } catch {
        case e: Exception => logWarning("Exception conn.close()", e)
      }
    }

  }
}

object EdpJdbcRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }


  def create[T](
                 sc: JavaSparkContext,
                 jdbcConfig:ConnectionConfig,
                 sql: String,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val jdbcRDD = new EdpJdbcRDD[T](
      sc.sc,
      jdbcConfig,
      sql,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)

    new JavaRDD[T](jdbcRDD)(fakeClassTag)
  }

  def create(
              sc: JavaSparkContext,
              jdbcConfig:ConnectionConfig,
              sql: String): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, jdbcConfig, sql, mapRow)
  }
}
