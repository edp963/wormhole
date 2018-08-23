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


package edp.wormhole.hbaseconnection

import edp.wormhole.ums.UmsFieldType
import edp.wormhole.util.DateUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object HbaseConnection extends Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)
  val hBaseConfigurationMap: mutable.HashMap[(String, String), Configuration] = new mutable.HashMap[(String, String), Configuration]
  val hBaseConnectionMap: mutable.HashMap[(String, String), Connection] = new mutable.HashMap[(String, String), Connection]

  def getZookeeperInfo(zookeeper: String): (String, String, String) = {
    val zookeeperList = zookeeper.split(",")
    val zkList = zookeeperList.map(str => str.split(":").head).sorted
    val zookeeperPort = zookeeperList.head.split(":").last.split("/").head
    val zkParent = if (zookeeperList.head.contains("/")) zookeeperList.head.substring(zookeeperList.head.indexOf("/")).trim else "/hbase"
    (zkList.mkString(","), zookeeperPort, zkParent)
  }

  def initHbaseConfig(sinkNamespace: String, connectionConfig: ConnectionConfig): Unit = {
    val kvConfig = connectionConfig.parameters
    //  if (sinkNamespace.toLowerCase.startsWith("hbase")) {
    //val hbaseConfig = json2caseClass[HbaseConfig](sinkConfig.specialConfig.get)
    val (zkList, zkPort, zkParent) = getZookeeperInfo(connectionConfig.connectionUrl)
    if (!hBaseConfigurationMap.contains((zkList, zkPort))) {
      val hBaseConfiguration: Configuration = HBaseConfiguration.create()
      hBaseConfiguration.set("hbase.zookeeper.quorum", zkList)
      hBaseConfiguration.set("hbase.zookeeper.property.clientPort", zkPort)
      hBaseConfiguration.set("hbase.metrics.showTableName", "false")
      hBaseConfiguration.set("zookeeper.znode.parent", zkParent)
      if (kvConfig.isDefined) kvConfig.get.foreach(kv => hBaseConfiguration.set(kv.key, kv.value))
      hBaseConfigurationMap((zkList, zkPort)) = hBaseConfiguration
    }
    // }
  }

  def getConnection(zkList: String, port: String): Connection = {
    if (!hBaseConnectionMap.contains((zkList, port)) || hBaseConnectionMap((zkList, port)) == null) {
      synchronized {
        if (!hBaseConnectionMap.contains((zkList, port)) || hBaseConnectionMap((zkList, port)) == null) {
          val connection = ConnectionFactory.createConnection(hBaseConfigurationMap((zkList, port)))
          hBaseConnectionMap((zkList, port)) = connection
        }
      }
    }
    hBaseConnectionMap((zkList, port))
  }

  def getTable(hbaseTableName: String, zkList: String, zkPort: String): Table = {
    //getConnection(zkList, zkPort).getTable(TableName.valueOf(hbaseTableName.getBytes))
    getConnection(zkList, zkPort).getTable(TableName.valueOf(hbaseTableName))
  }

  def dataPut(hbaseTable: String, dataPuts: Seq[Put], zkList: String, zkPort: String): Unit = {
    val table = getTable(hbaseTable, zkList, zkPort)
    try {
      table.put(dataPuts.asJava)
    }
    catch {
      case e: Throwable => logger.error("hbase put error:", e)
        throw e
    } finally if (table != null) table.close
  }

  def getDatasFromHbase(tableName: String, family: String, saveAsStr: Boolean, rowKeys: Seq[String], column: Seq[(String, String)], zkList: String, zkPort: String): Map[String, Map[String, Any]] = {
    val table = getTable(tableName, zkList, zkPort)
    val dataMap = mutable.HashMap.empty[String, Map[String, Any]]
    try {
      val familyBytes: Array[Byte] = Bytes.toBytes(family)
      val getList = ListBuffer.empty[Get]
      rowKeys.foreach(rowkey => {
        val get = new Get(Bytes.toBytes(rowkey))
        get.addFamily(familyBytes)
        column.foreach(c => get.addColumn(familyBytes, Bytes.toBytes(c._1)))
        getList += get
      })
      import collection.JavaConversions._
      val resultArray = table.get(getList)
      if (resultArray != null) {
        resultArray.foreach((result: Result) => {
          if (!result.isEmpty) {
            val rowkey = Bytes.toString(result.getRow)
            if (rowkey != null) {
              val data = getDatas(result, familyBytes, column, saveAsStr)
              if (data != null && data.nonEmpty) dataMap(rowkey) = data
            }
          }
        })
      }
    } catch {
      case e: Throwable => logger.error("getDatasFromHbase:", e)
        throw e
    } finally if (table != null) table.close
    dataMap.toMap
  }

  def getDatas(result: Result, familyBytes: Array[Byte], columns: Seq[(String, String)], saveAsStr: Boolean): Map[String, Any] = {
    val dataMap = mutable.HashMap.empty[String, Any]
    if (!result.isEmpty) {
      columns.foreach(c => {
        val cn = Bytes.toBytes(c._1)
        if (result.containsColumn(familyBytes, cn)) {
          val data: Any =
            if (saveAsStr) {
              val tmp = Bytes.toString(result.getValue(familyBytes, cn))
              UmsFieldType.umsFieldType(c._2) match {
                case UmsFieldType.STRING => tmp
                case UmsFieldType.LONG => tmp.toLong
                case UmsFieldType.INT => tmp.toInt
                case UmsFieldType.FLOAT => tmp.toFloat
                case UmsFieldType.DOUBLE => tmp.toDouble
                case UmsFieldType.BOOLEAN => tmp.toBoolean
                case UmsFieldType.DECIMAL => BigDecimal(tmp)
                case UmsFieldType.BINARY => tmp.toInt.toBinaryString
                case _=>tmp
              }
            }
            else {
              UmsFieldType.umsFieldType(c._2) match {
                case UmsFieldType.STRING => Bytes.toString(result.getValue(familyBytes, cn))
                case UmsFieldType.LONG => Bytes.toLong(result.getValue(familyBytes, cn))
                case UmsFieldType.INT => Bytes.toInt(result.getValue(familyBytes, cn))
                case UmsFieldType.FLOAT => Bytes.toFloat(result.getValue(familyBytes, cn))
                case UmsFieldType.DOUBLE => Bytes.toDouble(result.getValue(familyBytes, cn))
                case UmsFieldType.BOOLEAN => Bytes.toBoolean(result.getValue(familyBytes, cn))
                case UmsFieldType.DECIMAL => Bytes.toBigDecimal(result.getValue(familyBytes, cn))
                case UmsFieldType.BINARY => result.getValue(familyBytes, cn)
                case UmsFieldType.DATE => DateUtils.dt2sqlDate(Bytes.toString(result.getValue(familyBytes, cn)).trim)
                case UmsFieldType.DATETIME => DateUtils.dt2timestamp(Bytes.toString(result.getValue(familyBytes, cn)).trim)
              }
            }
          dataMap(c._1) = data
        }
      })
    }
    dataMap.toMap
  }
}
