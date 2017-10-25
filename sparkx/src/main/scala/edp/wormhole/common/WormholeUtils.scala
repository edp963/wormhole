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


package edp.wormhole.common

import edp.wormhole.common.util.{CommonUtils, DateUtils}
import edp.wormhole.common.util.DateUtils.currentDateTime
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.ums._
import edp.wormhole.ums.UmsSchemaUtils.toUms
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal
import org.apache.spark.sql.functions._
import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.core.{PartitionOffsetConfig, WormholeConfig}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.common.util.DateUtils.dt2dateTime
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import org.joda.time.DateTime

object WormholeUtils extends EdpLogging {

  def keys2keyList(keys: String): List[String] = if (keys == null) Nil else keys.split(",").map(CommonUtils.trimBothBlank).toList


  def getTypeNamespaceFromKafkaKey(key: String): (UmsProtocolType, String) = {
    val keys = key.split("\\.")
    if (keys.length > 7) (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), keys.slice(1, 8).mkString(".").toLowerCase)
    else (UmsProtocolType.umsProtocolType(keys(0).toLowerCase), "")
  }

  def json2Ums(json: String): Ums = {
    try {
      toUms(json)
    } catch {
      case NonFatal(e) => logError(s"message convert failed:\n$json", e)
        Ums(UmsProtocol(UmsProtocolType.FEEDBACK_DIRECTIVE), UmsSchema("defaultNamespace"), None)
    }
  }

  def jsonGetValue(namespace: String, protocolType: UmsProtocolType, json: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename)]): (Seq[UmsField], Seq[UmsTuple]) = {
    if (jsonSourceParseMap.contains((protocolType, namespace))) {
      val mapValue: (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)], UmsSysRename) = jsonSourceParseMap((protocolType, namespace))
      (mapValue._1, dataParse(json,mapValue._2, mapValue._3))
    } else {
      val ums = json2Ums(json)
      (ums.schema.fields_get, ums.payload_get)
    }
  }


  def dataParse(jsonStr: String, allFieldsInfo:Seq[FieldInfo], twoFieldsArr:ArrayBuffer[(String, String)]): Seq[UmsTuple] = {

    val jsonParse = JSON.parseObject(jsonStr)
    val fieldNameSeq = twoFieldsArr.map(_._1)
    val resultSeq = ArrayBuffer[UmsTuple]()
    val oneRecord = ArrayBuffer[String]()
    var arrValue = (Seq[String](), -1)
    allFieldsInfo.foreach(fieldInfo => {
      val outerFieldIndex = allFieldsInfo.indexOf(fieldInfo)
      val fieldName = fieldInfo.name
      val fieldDataType = fieldInfo.`type`
      val umsSysField = if (fieldInfo.umsSysField.isDefined) fieldInfo.umsSysField.get else null
      dataTypeProcess(fieldDataType) match {
        case "simplearray" => {
          arrValue = (jsonParse.getJSONArray(fieldName).toArray.map(_.toString), outerFieldIndex)
        }
        case "tuple" => {
          val fieldMessage = jsonParse.getString(fieldName)
          var splitMark = fieldInfo.separator.get
          if (Array("*", "^", ":", "|", ",", ".").contains(splitMark)) splitMark = "\\" + splitMark
          val splitData = fieldMessage.split(splitMark)
          val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
          for (i <- subFieldsInfo.indices) {
            val sysField = subFieldsInfo(i).umsSysField
            val subFieldDataType = subFieldsInfo(i).`type`
            if (sysField.isDefined && sysField.get == "ums_ts_" && subFieldDataType == "long")
              oneRecord.append(convertLongTimestamp(splitData(i)).toString)
            else oneRecord.append(splitData(i))
          }

        }
        case "jsonarray" => {
          val subFieldsInfo = fieldInfo.subFields.get
          val arrayParse = jsonParse.getJSONArray(jsonParse.getString(fieldName))
          for (i <- 0 until arrayParse.size()) {
            val jsonDetail = subFieldsInfo(i)
            val jsonKey = jsonDetail.name
            val jsonDataType = jsonDetail.`type`
            val subSysField = jsonDetail.umsSysField
            val fieldIndex = fieldNameSeq.indexOf(jsonKey)
            val jsonData: JSONObject = arrayParse.get(i).asInstanceOf[JSONObject]
            dataTypeProcess(jsonDataType) match {
              case "simplearray" => arrValue = (jsonParse.getJSONArray(jsonKey).toArray.asInstanceOf[Seq[String]], fieldIndex)
              case _ => {
                if (subSysField.isDefined && subSysField.get == "ums_ts_" && jsonDataType == "long")
                  oneRecord.append(convertLongTimestamp(jsonData.getString(jsonKey)).toString)
                else oneRecord.append(jsonData.getString(jsonKey))
              }
            }

          }
        }
        case "jsonobj" => {
          val subFieldsInfo = fieldInfo.subFields.get
          val jsonParseRes = JSON.parseObject(jsonParse.getString(fieldName))
          //          val detailFieldNames = subFieldsInfo.map(_.name)
          subFieldsInfo.foreach(subField => {
            val subFieldSys = subField.umsSysField
            val fieldIndex = fieldNameSeq.indexOf(subField.name)
            val fieldDataType = subField.`type`
            dataTypeProcess(fieldDataType) match {
              case "simpleArray" => arrValue = (jsonParse.getJSONArray(subField.name).toArray.asInstanceOf[Seq[String]], fieldIndex)
              case _ => {
                if (subFieldSys.isDefined && subFieldSys.get == "ums_ts_" && fieldDataType == "long")
                  oneRecord.append(convertLongTimestamp(jsonParseRes.getString(subField.name)).toString)
                else oneRecord.append(jsonParseRes.getString(subField.name))

              }

            }
          }
          )
        }
        case _ => {
          if (umsSysField == "ums_ts_" && fieldDataType == "long")
            oneRecord.append(convertLongTimestamp(jsonParse.getString(fieldName)).toString)
          else oneRecord.append(jsonParse.getString(fieldName))
        }

      }
    }
    )
    if (arrValue._2 > (-1)) {
      arrValue._1.foreach(value => {
        val newRecord: ArrayBuffer[String] = oneRecord.clone()
        newRecord.insert(arrValue._2, value)
        resultSeq.append(UmsTuple(newRecord))
      }
      )

    }
    else {
      resultSeq.append(UmsTuple(oneRecord))
    }
    resultSeq
  }


  def dataTypeProcess(dataType: String): String = {
    //    var result=dataType
    val typeArr: Array[String] = dataType.split("")
    val arrLen = typeArr.length
    if (typeArr.slice(arrLen - 5, arrLen).mkString("") == "array" && dataType != "jsonarray") "simplearray"
    else dataType
  }

  def convertLongTimestamp(timestampStr: String): DateTime = {
    val timestampLong = if (timestampStr.split("").length < 16) timestampStr.toLong * 1000000 else timestampStr.toLong
    dt2dateTime(timestampLong)
  }


  def sendTopicPartitionOffset(offsetInfo: ArrayBuffer[OffsetRange], feedbackTopicName: String, config: WormholeConfig): Unit = {
    val topicConfigMap = mutable.HashMap.empty[String, ListBuffer[PartitionOffsetConfig]]

    offsetInfo.foreach { offsetRange =>
      logInfo(s"----------- $offsetRange")
      val topicName = offsetRange.topic
      val partition = offsetRange.partition
      val offset = offsetRange.untilOffset
      logInfo("brokers:" + config.kafka_output.brokers + ",topic:" + feedbackTopicName)
      if (!topicConfigMap.contains(topicName)) topicConfigMap(topicName) = new ListBuffer[PartitionOffsetConfig]
      topicConfigMap(topicName) += PartitionOffsetConfig(partition, offset)
    }

    val tp: Map[String, String] = topicConfigMap.map { case (topicName, partitionOffsetList) => {
      (topicName, partitionOffsetList.map(it => it.partition_num + ":" + it.offset).sorted.mkString(","))
    }
    }.toMap
    WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.FeedbackPriority2, WormholeUms.feedbackStreamTopicOffset(currentDateTime, config.spark_config.stream_id, tp), None, config.kafka_output.brokers)
  }


  def getIncrementByTs(df: DataFrame, keys: List[String], from_yyyyMMddHHmmss: String, to_yyyyMMddHHmmss: String): DataFrame = {
    val fromTs = DateUtils.dt2timestamp(from_yyyyMMddHHmmss)
    val toTs = DateUtils.dt2timestamp(DateUtils.dt2dateTime(to_yyyyMMddHHmmss).plusSeconds(1).minusMillis(1))
    getIncrementByTs(df, keys, fromTs, toTs)
  }

  private def getIncrementByTs(df: DataFrame, keys: List[String], fromTs: Timestamp, toTs: Timestamp): DataFrame = {
    val w = Window
      .partitionBy(keys.head, keys.tail: _*)
      .orderBy(df(UmsSysField.ID.toString).desc)
    //    val w = Window.partitionBy(keys.head, keys.tail: _*).orderBy(df(UmsSysField.TS.toString).desc)

    df.where(df(UmsSysField.TS.toString) >= fromTs).where(df(UmsSysField.TS.toString) <= toTs).withColumn("rn", row_number.over(w)).where("rn = 1").drop("rn").filter("ums_op_ != 'd'")
  }

}

