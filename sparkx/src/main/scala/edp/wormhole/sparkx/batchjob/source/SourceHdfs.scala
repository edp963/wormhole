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


package edp.wormhole.sparkx.batchjob.source

import com.alibaba.fastjson.JSON
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.sparkx.common.{SparkSchemaUtils, SparkUtils}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.batchjob.source.ObtainSourceDataInterface
import edp.wormhole.ums._
import edp.wormhole.util.DateUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SourceHdfs extends ObtainSourceDataInterface with EdpLogging {
  override def process(session: SparkSession, fromTime: String, toTime: String,
                       sourceNamespace: String, connectionConfig: ConnectionConfig,
                       specialConfig: Option[String]): DataFrame = {
    val configuration = getConfiguration(connectionConfig)
    val protocolTypeSet = getProtocolTypeSet(specialConfig)

    val repartitionFlag = getRepartitionSet(specialConfig)

    val hdfsPathList = HdfsLogReadUtil.getHdfsPathList(configuration, connectionConfig.connectionUrl, sourceNamespace.toLowerCase, protocolTypeSet.toSet)
    val dataPathList: Seq[String] = HdfsLogReadUtil.getHdfsFileList(configuration, hdfsPathList)
    logInfo("dataPathList.length=" + dataPathList.length + ",namespace=" + sourceNamespace)

    val startTime = if (fromTime == "19700101000000") null else fromTime
    val endTime = if (toTime == "30000101000000") null else toTime
    val filteredPathList: Seq[String] = HdfsLogReadUtil.getHdfsLogPathListBetween(configuration, dataPathList, startTime, endTime)
    logInfo("filteredPathList.length=" + filteredPathList.length + ",namespace=" + sourceNamespace)

    val ums = checkAndGetUms(filteredPathList, configuration)
    logInfo("ums:" + ums.toString + ",namespace=" + sourceNamespace)

    if (filteredPathList.nonEmpty) {

      var strDS = session.read.textFile(filteredPathList: _*)

      if (repartitionFlag) {
        strDS = strDS.repartition(session.sqlContext.getConf("spark.sql.shuffle.partitions").toInt)
      }
      val umsStrRDD: RDD[String] = strDS.rdd.mapPartitions { lineIt =>
        lineIt.filter(line => {
          val rowContent = line.trim
          rowContent.startsWith("{") && rowContent.endsWith("}")
        })
      }

      val rowRdd: RDD[Row] = umsStrRDD.mapPartitions(lineIt => {
        lineIt.flatMap(umsStr => {
          toUms(umsStr: String)
        })
      })
      //      logInfo("!!!!!!!umsRDD.getNumPartitions:" + rowRdd.getNumPartitions)

      val fields = ums.schema.fields_get
      val allData: DataFrame = SparkSchemaUtils.createDf(session, fields, rowRdd)
      filterData(allData, startTime, endTime)
    } else {
      logInfo("filteredPathList is empty,namespace=" + sourceNamespace)
      null.asInstanceOf[DataFrame]
    }
  }

  def filterData(allData: DataFrame, startTime: String, endTime: String): DataFrame = {
    val fromTs = if (startTime == null) DateUtils.dt2timestamp(DateUtils.yyyyMMddHHmmss(DateUtils.unixEpochTimestamp)) else DateUtils.dt2timestamp(startTime)
    val toTs = if (endTime == null) DateUtils.dt2timestamp(DateUtils.currentDateTime) else DateUtils.dt2timestamp(DateUtils.dt2dateTime(endTime))
    val timeFilter = s"""${UmsSysField.TS.toString} >= '$fromTs' and ${UmsSysField.TS.toString} <= '$toTs'"""
    logInfo("!!!!@filter condition: " + timeFilter)
    allData.filter(timeFilter)
  }

  def toUms(umsStr: String): Seq[Row] = {
    try {
      val ums = UmsSchemaUtils.toUms(umsStr)
      ums.payload_get.map(dataRow => {
        val row: Option[Row] = SparkUtils.umsToSparkRowWrapper(ums.schema.namespace, ums.schema.fields_get, dataRow.tuple)
        row.getOrElse(Row.empty)
      })
    } catch {
      case e: Exception =>
        logAlert(s"serialize line $umsStr to ums failed", e)
        List {
          Row.empty
        }
    }
  }

  def getProtocolTypeSet(specialConfig: Option[String]): mutable.Set[String] = {
    val specialConfigStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(specialConfig.get.toString.split(" ").mkString("")))
    val specialConfigObject = JSON.parseObject(specialConfigStr)
    val initial = specialConfigObject.getBoolean(InputDataProtocolBaseType.INITIAL.toString)
    val increment = specialConfigObject.getBoolean(InputDataProtocolBaseType.INCREMENT.toString)
    assert(initial || increment, "initial and increment should not be false at the same time.")
    val protocolTypeSet: mutable.Set[String] = mutable.HashSet.empty[String]
    if (initial) protocolTypeSet += UmsProtocolType.DATA_INITIAL_DATA.toString
    if (increment) protocolTypeSet += UmsProtocolType.DATA_INCREMENT_DATA.toString
    protocolTypeSet
  }

  def getRepartitionSet(specialConfig: Option[String]): Boolean = {
    val specialConfigStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(specialConfig.get.toString.split(" ").mkString("")))
    val specialConfigObject = JSON.parseObject(specialConfigStr)
    if (specialConfigObject.containsKey("repartition")) {
      specialConfigObject.getBoolean("repartition")
    } else {
      true
    }
  }

  def checkAndGetUms(filteredPathList: Seq[String], configuration: Configuration): Ums = {
    var ums: Ums = null
    var i = 1
    assert(filteredPathList.nonEmpty, "path list size is 0, there is no matched data")
    var umsContent = HdfsUtils.readFileByLineNum(filteredPathList.head, configuration, i)
    val umsContentList = ListBuffer.empty[String]
    while (ums == null && umsContent != null) {
      try {
        if (i == 1) {
          umsContentList += umsContent.substring(umsContent.indexOf("{"))
        } else {
          umsContentList += umsContent
        }
        ums = UmsSchemaUtils.toUms(umsContentList.mkString(" "))
      } catch {
        case e: Throwable =>
          i += 1
          umsContent = HdfsUtils.readFileByLineNum(filteredPathList.head, configuration, i)
          logAlert("umsContent=" + umsContent, e)
      }
    }
    assert(ums != null, "ums is null")
    ums
  }

  def getConfiguration(connectionConfig: ConnectionConfig): Configuration = {
    var sourceNamenodeHosts = null.asInstanceOf[String]
    var sourceNamenodeIds = null.asInstanceOf[String]
    if (connectionConfig.parameters.nonEmpty) connectionConfig.parameters.get.foreach(param => {
      if (param.key == "hdfs_namenode_hosts") sourceNamenodeHosts = param.value
      if (param.key == "hdfs_namenode_ids") sourceNamenodeIds = param.value
    })

    val configuration = new Configuration()
    val hdfsPath = connectionConfig.connectionUrl
    val hdfsPathGrp = hdfsPath.split("//")
    val hdfsRoot = if (hdfsPathGrp(1).contains("/")) hdfsPathGrp(0) + "//" + hdfsPathGrp(1).substring(0, hdfsPathGrp(1).indexOf("/")) else hdfsPathGrp(0) + "//" + hdfsPathGrp(1)
    configuration.set("fs.defaultFS", hdfsRoot)
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    if (sourceNamenodeHosts != null) {
      val clusterName = hdfsRoot.split("//")(1)
      configuration.set("dfs.nameservices", clusterName)
      configuration.set(s"dfs.ha.namenodes.$clusterName", sourceNamenodeIds)
      val namenodeAddressSeq = sourceNamenodeHosts.split(",")
      val namenodeIdSeq = sourceNamenodeIds.split(",")
      for (i <- 0 until namenodeAddressSeq.length) {
        configuration.set(s"dfs.namenode.rpc-address.$clusterName." + namenodeIdSeq(i), namenodeAddressSeq(i))
      }
      configuration.set(s"dfs.client.failover.proxy.provider.$clusterName", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    }
    configuration
  }

  def getUmsString(line: String, tmpContent: String): (String, String) = {
    var innerTmpContent = tmpContent
    var lineContent = line.trim
    val rowStr = if (innerTmpContent.isEmpty) {
      if (lineContent.startsWith("{") && lineContent.endsWith("}")) {
        lineContent
      } else {
        innerTmpContent += lineContent
        "N/A"
      }
    } else {
      innerTmpContent += lineContent
      if (innerTmpContent.startsWith("{") && innerTmpContent.endsWith("}")) {
        val tmp = innerTmpContent
        innerTmpContent = ""
        tmp
      } else {
        "N/A"
      }
    }

    (rowStr, innerTmpContent)
  }

}
