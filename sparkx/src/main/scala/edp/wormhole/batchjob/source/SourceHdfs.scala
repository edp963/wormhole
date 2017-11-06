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


package edp.wormhole.batchjob.source

import com.alibaba.fastjson.JSON
import edp.wormhole.common.SparkSchemaUtils.createDf
import edp.wormhole.common.hadoop.HdfsUtils
import edp.wormhole.common.util.DateUtils
import edp.wormhole.common.{ConnectionConfig, SparkUtils}
import edp.wormhole.core.InputDataRequirement
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

class SourceHdfs extends ObtainSourceDataInterface with EdpLogging {
  override def process(session: SparkSession, fromTime: String, toTime: String, sourceNamespace: String, connectionConfig: ConnectionConfig, specialConfig: Option[String]): DataFrame = {
    val specialConfigStr = new String(new sun.misc.BASE64Decoder().decodeBuffer(specialConfig.get.toString.split(" ").mkString("")))
    val specialConfigObject = JSON.parseObject(specialConfigStr)
    val initial = specialConfigObject.getBoolean(InputDataRequirement.INITIAL.toString)
    val increment = specialConfigObject.getBoolean(InputDataRequirement.INCREMENT.toString)
    assert((!initial && !increment) != true, "initial and increment should not be false at the same time.")
    val dataType: Array[String] = if (initial && increment) Array(UmsProtocolType.DATA_INITIAL_DATA.toString, UmsProtocolType.DATA_INCREMENT_DATA.toString)
    else if (!initial && increment) Array(UmsProtocolType.DATA_INCREMENT_DATA.toString)
    else Array(UmsProtocolType.DATA_INITIAL_DATA.toString)


    val startTime = if (fromTime == "19700101000000") null else fromTime
    val endTime = if (toTime == "30000101000000") null else toTime
    val hdfsPathList = HdfsLogReadUtil.getHdfsPathList(connectionConfig.connectionUrl, UmsNamespace(sourceNamespace))
    val dataPathList: Seq[String] = HdfsLogReadUtil.getHdfsFileList(hdfsPathList)
    logInfo("dataPathList.length=" + dataPathList.length + ",namespace=" + sourceNamespace)
    val filteredPathList = HdfsLogReadUtil.getHdfsLogPathListBetween(dataPathList, startTime, endTime)
    filteredPathList.foreach(t => println("@@@@@@@@@@@" + t))
    var ums: Ums = null
    var i = 1
    var umsContent = HdfsUtils.readFileByLineNum(filteredPathList.head, i)
    val umsContentList = ListBuffer.empty[String]
    while (ums == null && umsContent != null) {
      try {
        println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        if (i == 1) {
          umsContentList += umsContent.substring(umsContent.indexOf("{"))
        } else {
          umsContentList += umsContent
        }
        ums = UmsSchemaUtils.toUms(umsContentList.mkString(" "))
      } catch {
        case e: Throwable =>
          i += 1
          umsContent = HdfsUtils.readFileByLineNum(filteredPathList.head, i)
          logAlert("umsContent=" + umsContent, e)
      }
    }

    assert(ums != null, "ums is null")
    logInfo("ums:" + ums.toString + ",namespace=" + sourceNamespace)

    logInfo("filteredPathList.length=" + filteredPathList.length + ",namespace=" + sourceNamespace)
    if (filteredPathList.nonEmpty) {
      val fileArray = new Array[RDD[Ums]](filteredPathList.length)
      //      val finalUnionRdd = if (filteredPathList.length <= 100) {
      filteredPathList.zipWithIndex.foreach {
        case (eachFile, index) =>
          val strRdd: RDD[String] = session.sparkContext.textFile(eachFile, 1) //.persist(StorageLevel.MEMORY_AND_DISK_SER)
          logInfo("one file has partition num=" + strRdd.getNumPartitions + ",namespace=" + sourceNamespace)
          fileArray(index) = strRdd.mapPartitionsWithIndex((indexparitition, lineIt) => {
            println("arrayIndex:" + index + "   " + "partition index:" + indexparitition + "    " + "start:")
            val successList = ListBuffer.empty[Ums]
            val contentList = ListBuffer.empty[String]
            var kafkaKey = ""
            lineIt.foreach(line => {
              try {
                val conditionArr = dataType.map(str => line.startsWith(str))
                var condition = false
                conditionArr.foreach(bool => condition = condition || bool)
                if (condition) {
                  if (contentList.nonEmpty) {
                    try {
                      successList += UmsSchemaUtils.toUms(contentList.mkString(" "))
                    } catch {
                      case e: Throwable => logAlert("contentList=" + contentList, e)
                    }
                  }
                  kafkaKey = ""
                  contentList.clear()
                  val splitIndex = line.indexOf("{")
                  kafkaKey = line.substring(0, splitIndex)
                  contentList += line.substring(splitIndex)
                } else {
                  contentList += line
                }
              } catch {
                case _: Throwable => logAlert("json2caseClass content=" + contentList.mkString("\n"))
              }
            })
            if (contentList.nonEmpty) {
              try {
                if (contentList.length == 1) successList += UmsSchemaUtils.toUms(contentList.head)
                else successList += UmsSchemaUtils.toUms(contentList.mkString(" "))
              } catch {
                case e: Throwable => logAlert("contentList=" + contentList, e)
              }
            }

            logInfo("successList.length=" + successList.length)
            successList.toIterator
          })
          logInfo("index=" + index)
      }

      val finalUnionRdd = session.sparkContext.union(fileArray.toList)
      println("!!!!!!!unionRdd.getNumPartitions:" + finalUnionRdd.getNumPartitions)

      val fields = ums.schema.fields_get
      val payloadRdd: RDD[Seq[String]] = finalUnionRdd.flatMap(_.payload_get.map(_.tuple))
      val rowRdd: RDD[Row] = payloadRdd.flatMap(row => SparkUtils.umsToSparkRowWrapper(ums.schema.namespace, fields, row))
      val allData = createDf(session, fields, rowRdd)
      val fromTs = if (startTime == null) DateUtils.dt2timestamp(DateUtils.yyyyMMddHHmmss(DateUtils.unixEpochTimestamp)) else DateUtils.dt2timestamp(startTime)
      val toTs = if (endTime == null) DateUtils.dt2timestamp(DateUtils.currentDateTime) else DateUtils.dt2timestamp(DateUtils.dt2dateTime(endTime))
      val timeFilter = s"""${UmsSysField.TS.toString} >= '$fromTs' and ${UmsSysField.TS.toString} <= '$toTs'"""
      println("!!!!@filter condition: " + timeFilter)
      allData.filter(timeFilter)
    } else {
      logInfo("filteredPathList is empty,namespace=" + sourceNamespace)
      null.asInstanceOf[DataFrame]
    }
  }
}
