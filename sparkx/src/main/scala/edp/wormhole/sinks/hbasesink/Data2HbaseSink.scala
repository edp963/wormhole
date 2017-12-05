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

import edp.wormhole.common.{ConnectionConfig, RowkeyPatternContent, RowkeyPatternType, RowkeyTool}
import edp.wormhole.sinks.{SinkProcessConfig, SinkProcessor, SourceMutationType}
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sinks.hbasesink.HbaseConstants._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import edp.wormhole.ums.UmsSysField._
import edp.wormhole.ums.UmsOpType._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import edp.wormhole.sinks.utils.SinkDefault._
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.UmsNamespace
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

    def rowkey(rowkeyConfig: Seq[RowkeyPatternContent], recordValue: Seq[String]): String = {
      val keydatas = rowkeyConfig.map(rowkey => {

        val rkName = rowkey.fieldContent.toLowerCase
        val rkType = rowkey.patternType
        if (rkType==RowkeyPatternType.DELIMIER.toString){
          rowkey.fieldContent
        } else {
          if (!schemaMap.contains(rkName)) {
            logError("schemaMap does not containing " + rkName)
            throw new Exception("schemaMap does not containing " + rkName)
          }
          recordValue(schemaMap(rkName)._1)

        }
      })
      RowkeyTool.generatePatternKey(keydatas, rowkeyConfig)
    }


    def gerneratePuts(columnFamily: String, saveAsString: Boolean, versionColumn: String, filterRowkey2idTuples: Seq[(String, Long, Seq[String])]): ListBuffer[Put] = {
      val puts: ListBuffer[Put] = new mutable.ListBuffer[Put]
      for (tuple <- filterRowkey2idTuples) {
        try {
          val umsOpValue: String = tuple._3(schemaMap(OP.toString)._1)
//          val versionValue = if (schemaMap(versionColumn)._2 == UmsFieldType.DATETIME) DateUtils.dt2long(tuple._3(schemaMap(versionColumn)._1))
//          else s2long(tuple._3(schemaMap(versionColumn)._1))
          val rowkeyBytes = Bytes.toBytes(tuple._1)
          val put = new Put(rowkeyBytes)
          schemaMap.keys.foreach { column =>
            val (index, fieldType, _) = schemaMap(column)
            val valueString = tuple._3(index)
            if (OP.toString != column) {
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),  s2hbaseValue(fieldType, valueString))
              if (saveAsString && !(ID.toString == column || TS.toString == column || UID.toString == column))
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), s2hbaseStringValue(fieldType, valueString, column))
              else put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),  s2hbaseValue(fieldType, valueString))
            } else put.addColumn(Bytes.toBytes(columnFamily), activeColBytes,  if (DELETE.toString == umsOpValue.toLowerCase) inactiveBytes else activeBytes)
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
    val rowkeyConfig: String = hbaseConfig.`hbase.rowKey`

    val patternContentList: mutable.Seq[RowkeyPatternContent] = RowkeyTool.parse(rowkeyConfig)

    //    logInfo("before format:" + tupleList.size)
    val rowkey2IdTuples: Seq[(String, Long, Seq[String])] = tupleList.map(tuple => {
      (rowkey(patternContentList, tuple), tuple(schemaMap(ID.toString)._1).toLong, tuple)
    })

    val filterRowkey2idTuples = SourceMutationType.sourceMutationType(hbaseConfig.`mutation.type.get`) match {
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
    val puts = gerneratePuts(hbaseConfig.`hbase.columnFamily.get`, hbaseConfig.`hbase.valueType.get`, hbaseConfig.`hbase.version.column.get`, filterRowkey2idTuples)
    //    logInfo("before put:" + puts.size)
    if (puts.nonEmpty) {
      HbaseConnection.dataPut(namespace.database + ":" + namespace.table, puts, zk._1, zk._2)
      puts.clear()
      //      logInfo("after put:" + puts.size)
    } else logInfo("there is nothing to insert")
  }
}
