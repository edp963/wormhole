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


package edp.wormhole.sinks.hbasesink

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sinks.hbasesink.HbaseConstants._
import edp.wormhole.sinks.hbasesink.RowkeyPattern._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.ums.UmsOpType._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.{UmsFieldType, UmsNamespace}
import edp.wormhole.common.util.CommonUtils._
import edp.wormhole.common.util.DateUtils
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.common.util.JsonUtils._

class Data2HbaseSink extends SinkProcessor with EdpLogging {
  override def process(protocolType: UmsProtocolType,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    HbaseConnection.initHbaseConfig(sinkNamespace, sinkProcessConfig, connectionConfig)

    def rowkey(rowkeyConfig: Seq[RowkeyInfo], recordValue: Seq[String]): String = rowkeyConfig.map(rowkey => {
      val rkName = rowkey.name.toLowerCase
      if (!schemaMap.contains(rkName)) {
        logError("schemaMap does not containing " + rkName)
        throw new Exception("schemaMap does not containing " + rkName)
      }
      val rkValue = recordValue(schemaMap(rkName)._1)
      val rkPattern = rowkey.pattern
      if (rkPattern == VALUE.toString) rkValue
      else if (rkPattern == HASH.toString) {
        val rkGet = rkHash(rkValue)
        if (rkGet == null) null else rkGet.toString
      } else if (rkPattern == REVERSE.toString) rkReverse(rkValue)
      else if (rkPattern.startsWith(MD5.toString)) rkMod(rkValue, rkPattern)
      else {
        val rkGet = rkHash(rkReverse(rkValue))
        if (rkGet == null) null else rkGet.toString
      }
    }).mkString("_")


    def gerneratePuts(rkConfig: Seq[RowkeyInfo], columnFamily: String, saveAsString: Boolean, versionColumn: String, filterRowkey2idTuples: Seq[(String, Long, Seq[String])]): ListBuffer[Put] = {
      val puts: ListBuffer[Put] = new mutable.ListBuffer[Put]
      for (tuple <- filterRowkey2idTuples) {
        try {
          val umsOpValue: String = tuple._3(schemaMap(OP.toString)._1)
          val versionValue = if (schemaMap(versionColumn)._2 == UmsFieldType.DATETIME) DateUtils.dt2long(tuple._3(schemaMap(versionColumn)._1))
          else s2long(tuple._3(schemaMap(versionColumn)._1))
          val rowkeyBytes = Bytes.toBytes(tuple._1)
          val put = new Put(rowkeyBytes)
          schemaMap.keys.foreach { column =>
            val (index, fieldType, _) = schemaMap(column)
            val valueString = tuple._3(index)
            if (OP.toString != column) {
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), versionValue, s2hbaseValue(fieldType, valueString))
              if (saveAsString && !(ID.toString == column || TS.toString == column || UID.toString == column)) put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), versionValue, s2hbaseStringValue(fieldType, valueString, column))
              else put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), versionValue, s2hbaseValue(fieldType, valueString))
            } else put.addColumn(Bytes.toBytes(columnFamily), activeColBytes, versionValue, if (DELETE.toString == umsOpValue.toLowerCase) inactiveBytes else activeBytes)
          }
          puts += put
        } catch {
          case e: Throwable => logError("rowkey:" + tuple._1 + ", tuple:" + tuple._3, e)
        }
      }
      puts
    }

    val namespace = UmsNamespace(sinkNamespace)
    val hbaseConfig = json2caseClass[HbaseConfig](sinkProcessConfig.specialConfig.get)
    val zk = HbaseConnection.getZookeeperInfo(connectionConfig.connectionUrl)
    val rowkeyConfig: Seq[RowkeyInfo] = hbaseConfig.`hbase.rowKey`

    //    logInfo("before format:" + tupleList.size)
    val rowkey2IdTuples: Seq[(String, Long, Seq[String])] = tupleList.map(tuple => {
      (rowkey(rowkeyConfig, tuple), tuple(schemaMap(ID.toString)._1).toLong, tuple)
    })

    val filterRowkey2idTuples = SourceMutationType.sourceMutationType(hbaseConfig.`hbase.mutation.type.get`) match {
      case SourceMutationType.I_U_D =>
        logInfo("hbase iud:")
        logInfo("before select:" + rowkey2IdTuples.size)
        val columnList = List((ID.toString, LONG.toString))
        val rowkey2IdMap: Map[String, Map[String, Any]] = HbaseConnection.getDatasFromHbase(namespace.database + ":" + namespace.table, hbaseConfig.`hbase.columnFamily.get`, rowkey2IdTuples.map(_._1), columnList, zk._1, zk._2)
        logInfo("before filter:" + rowkey2IdMap.size)
        if (rowkey2IdMap.nonEmpty) {
          rowkey2IdTuples.filter(row => {
            !rowkey2IdMap.contains(row._1) || (rowkey2IdMap(row._1).contains(ID.toString) && rowkey2IdMap(row._1)(ID.toString).asInstanceOf[Long] < row._2)
          })
        } else rowkey2IdTuples
      case SourceMutationType.INSERT_ONLY =>
        logInfo("hbase insert_only:")
        rowkey2IdTuples
    }

    //    logInfo("before generate puts:" + filterRowkey2idTuples.size)
    val puts = gerneratePuts(rowkeyConfig, hbaseConfig.`hbase.columnFamily.get`, hbaseConfig.`hbase.valueType.get`, hbaseConfig.`hbase.version.column.get`, filterRowkey2idTuples)
    //    logInfo("before put:" + puts.size)
    if (puts.nonEmpty) {
      HbaseConnection.dataPut(namespace.database + ":" + namespace.table, puts, zk._1, zk._2)
      puts.clear()
      //      logInfo("after put:" + puts.size)
    } else logInfo("there is nothing to insert")
  }
}
