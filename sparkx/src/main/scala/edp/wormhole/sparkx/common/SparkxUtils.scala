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
package edp.wormhole.sparkx.common

import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.common.json.{FieldInfo, JsonParseUtils}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{StreamSpecialConfig, WormholeConfig}
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import edp.wormhole.util.DateUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable.ArrayBuffer

object SparkxUtils extends EdpLogging{

  def setFlowErrorMessage(incrementTopicList:List[String],
                          topicPartitionOffset:JSONObject,
                          config: WormholeConfig,
                          sourceNamespace:String,
                          sinkNamespace:String,
                          errorCount:Int,
                          error:Throwable,
                          batchId:String,
                          protocolType: String,
                          flowId:Long,
                          errorPattern:String): Unit ={

    val ts: String = null
    val errorMaxLength = 2000
//    val tmpJsonArray = new JSONArray()
//    val sourceTopicSet = mutable.HashSet.empty[String]
//    sourceTopicSet ++= incrementTopicList
//    sourceTopicSet ++= ConfMemoryStorage.initialTopicSet
//    sourceTopicSet.foreach(topic=>{
//      tmpJsonArray.add(topicPartitionOffset.getJSONObject(topic))
//    })
//    logInfo(s"incrementTopicList:${incrementTopicList},initialTopicSet:${ConfMemoryStorage.initialTopicSet},sourceTopicSet:${sourceTopicSet},tmpJsonArray:${tmpJsonArray}")

    val errorMsg = if(error!=null){
      val first = if(error.getStackTrace!=null&&error.getStackTrace.nonEmpty) error.getStackTrace.head.toString else ""
      val errorAll = error.toString + "\n" + first
      errorAll.substring(0, math.min(errorMaxLength, errorAll.length))
    } else null
    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name,
      FeedbackPriority.feedbackPriority, UmsProtocolUtils.feedbackFlowError(sourceNamespace,
        config.spark_config.stream_id, DateUtils.currentDateTime, sinkNamespace, UmsWatermark(ts),
        UmsWatermark(ts), errorCount, errorMsg, batchId, topicPartitionOffset.toJSONString,protocolType.replaceAll("\"",""),
        flowId,errorPattern),
      Some(UmsProtocolType.FEEDBACK_FLOW_ERROR + "." + flowId),
      config.kafka_output.brokers)
  }

  def unpersistDataFrame(df: DataFrame): Unit ={
    if(df!=null){
      try{
        df.unpersist()
      }catch{
        case e:Throwable=>logWarning("unpersistDataFrame",e)
      }
    }
  }

  def getFieldContentByTypeForSql(row: Row, schema: Array[StructField], i: Int): Any = {
    if (schema(i).dataType.toString.equals("StringType") || schema(i).dataType.toString.equals("DateType") || schema(i).dataType.toString.equals("TimestampType")) {
      //if (row.get(i) == null) "''"  // join fields cannot be null
      if (row.get(i) == null) null
      else "'" + row.get(i) + "'"
      /*else {
        if(schema(i).dataType.toString.equals("StringType") && row.get(i).toString.contains("'")) {
          "'" + row.get(i).toString.replace("'", "\\\'") + "'"
        } else "'" + row.get(i) + "'"
      }*/
    } else row.get(i)
  }


  def jsonGetValue(namespace: String, protocolType: UmsProtocolType, json: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])]): (Seq[UmsField], Seq[UmsTuple]) = {
    if (jsonSourceParseMap.contains((protocolType, namespace))) {
      val mapValue: (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)]) = jsonSourceParseMap((protocolType, namespace))
      (mapValue._1, JsonParseUtils.dataParse(json, mapValue._2, mapValue._3))
    } else {
      val ums = UmsCommonUtils.json2Ums(json)
      (ums.schema.fields_get, ums.payload_get)
    }
  }

  def printTopicPartitionOffset(offsetInfo: ArrayBuffer[OffsetRange], feedbackTopicName: String, config: WormholeConfig, batchId: String): Unit = {
    //    val topicConfigMap = mutable.HashMap.empty[String, ListBuffer[PartitionOffsetConfig]]

    offsetInfo.foreach { offsetRange =>
      logInfo(s"----------- $offsetRange")
      //      val topicName = offsetRange.topic
      //      val partition = offsetRange.partition
      //      val offset = offsetRange.untilOffset
      //      logger.info("brokers:" + config.kafka_output.brokers + ",topic:" + feedbackTopicName)
      //      if (!topicConfigMap.contains(topicName)) topicConfigMap(topicName) = new ListBuffer[PartitionOffsetConfig]
      //      topicConfigMap(topicName) += PartitionOffsetConfig(partition, offset)
    }

    //    val tp: Map[String, String] = topicConfigMap.map { case (topicName, partitionOffsetList) => {
    //      (topicName, partitionOffsetList.map(it => it.partition_num + ":" + it.offset).sorted.mkString(","))
    //    }
    //    }.toMap
    //    WormholeKafkaProducer.sendMessage(feedbackTopicName, FeedbackPriority.feedbackPriority, WormholeUms.feedbackStreamTopicOffset(currentDateTime, config.spark_config.stream_id, tp, batchId), Some(UmsProtocolType.FEEDBACK_STREAM_TOPIC_OFFSET+"."+config.spark_config.stream_id), config.kafka_output.brokers)
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

  /*def dataParse(jsonStr: String, allFieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)]): Seq[UmsTuple] = {
    val jsonParse = JSON.parseObject(jsonStr)
    val fieldNameSeq = twoFieldsArr.map(_._1)
//    val outFieldNameSeq=allFieldsInfo.map(_.name)
    val resultSeq = ArrayBuffer[UmsTuple]()
    val oneRecord = ArrayBuffer[String]()
    var arrValue = (Seq[String](), -1)
    var jsonArr=new ArrayBuffer[Seq[String]]()
    var jsonArrValue=(Seq[Seq[String]](),-1)
    def dataProcess(fieldInfo: FieldInfo, jsonValue: JSONObject): Unit = {
      val name = fieldInfo.name
      val dataType = fieldInfo.`type`
      val fieldIndex = if (fieldNameSeq.contains(name)) fieldNameSeq.indexOf(name) else -1
      val umsSysField = if(fieldInfo.rename.isDefined) fieldInfo.rename.get else name
      val subFields = fieldInfo.subFields
      val dataTypeProcessed = dataTypeProcess(dataType)
      if (dataTypeProcessed == "simplearray") {
        arrValue =
          try {
            (jsonValue.getJSONArray(name).toArray.map(_.toString), fieldIndex)
          }
          catch {
            case NonFatal(e) =>
              oneRecord.append(null)
              (null, fieldIndex)
          }
      } else if (dataTypeProcessed == "tuple") {
        val fieldMessage =
          try {
            jsonValue.getString(name)
          }
          catch {
            case NonFatal(e) => null
          }
        if (fieldMessage==null){
          val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
          for (i <- subFieldsInfo.indices) {
            oneRecord.append(null)
          }
        }
        else{
          var splitMark = fieldInfo.separator.get
          if (Array("*", "^", ":", "|", ",", ".").contains(splitMark)) splitMark = "\\" + splitMark
          val splitData = fieldMessage.split(splitMark)
          val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
          for (i <- subFieldsInfo.indices) {
            val sysField = subFieldsInfo(i).rename
//            val subFieldDataType = subFieldsInfo(i).`type`
            if (sysField.isDefined && sysField.get == "ums_ts_")
              oneRecord.append(convertLongTimestamp(splitData(i)).toString)
            else oneRecord.append(splitData(i))
          }}
      }
      else if (dataTypeProcessed == "jsonarray") {
//        val outerIndex=if (outFieldNameSeq.contains(name)) outFieldNameSeq.indexOf(name) else -1
        val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
        var firstField=subFieldsInfo(0)
        while(firstField.subFields.nonEmpty) {firstField=firstField.subFields.get(0)}
        val outFieldIndex=if (fieldNameSeq.contains(firstField.name)) fieldNameSeq.indexOf(firstField.name) else -1
        val arrayParse = jsonValue.getJSONArray(name)
        for (i <- 0 until arrayParse.size()) {
          val record=ArrayBuffer[String]()
          val content = arrayParse.getJSONObject(i)
          def arrayProcess(fieldInfo: FieldInfo, jsonValue: JSONObject): Unit ={
            val name = fieldInfo.name
            val dataType = fieldInfo.`type`
            val umsSysField = if(fieldInfo.rename.isDefined) fieldInfo.rename.get else name
            val subFields = fieldInfo.subFields
            val dataTypeProcessed = dataTypeProcess(dataType)
            if (dataTypeProcessed == "jsonobject") {
              val subFieldsInfo = subFields.get
              val jsonParseRes = jsonValue.getJSONObject(name)
              subFieldsInfo.foreach(subField => {
                arrayProcess(subField, jsonParseRes)
              }
              )
            }
            else if (dataTypeProcessed == "tuple") {
              val fieldMessage =
                try {
                  jsonValue.getString(name)
                }
                catch {
                  case NonFatal(e) => null
                }
              if (fieldMessage==null){
                val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
                for (i <- subFieldsInfo.indices) {
                  record.append(null)
                }
              }
              else{
                var splitMark = fieldInfo.separator.get
                if (Array("*", "^", ":", "|", ",", ".").contains(splitMark)) splitMark = "\\" + splitMark
                val splitData = fieldMessage.split(splitMark)
                val subFieldsInfo: Seq[FieldInfo] = fieldInfo.subFields.get
                for (i <- subFieldsInfo.indices) {
                  val sysField = subFieldsInfo(i).rename
                  //            val subFieldDataType = subFieldsInfo(i).`type`
                  if (sysField.isDefined && sysField.get == "ums_ts_")
                    record.append(convertLongTimestamp(splitData(i)).toString)
                  else record.append(splitData(i))
                }}
            }
            else {
              if (umsSysField.nonEmpty && umsSysField == "ums_ts_"||name=="ums_ts_"){
                record.append(convertLongTimestamp(jsonValue.getString(name)).toString)}
              else if (umsSysField.nonEmpty && umsSysField == "ums_op_"||name=="ums_op_") {
                val mappingRule = fieldInfo.umsSysMapping.get
                val iudMap = mappingRule.split(",").map(_.split("\\:")).map(arr => arr(1) -> arr(0)).toMap
                record.append(iudMap(jsonValue.getString(name)))
              }
              else record.append(jsonValue.getString(name))
            }
          }
          subFieldsInfo.foreach(subField=>
            arrayProcess(subField, content)
          )
          jsonArr+=record
        }
        jsonArrValue=(jsonArr,outFieldIndex)
      }
      else if (dataTypeProcessed == "jsonobject") {
        val subFieldsInfo = subFields.get
        val jsonParseRes = jsonValue.getJSONObject(name)
        subFieldsInfo.foreach(subField => {
          dataProcess(subField, jsonParseRes)
        }
        )
      }
      else {
        if (umsSysField.nonEmpty && umsSysField == "ums_ts_"||name=="ums_ts_"){
          oneRecord.append(convertLongTimestamp(jsonValue.getString(name)).toString)}
        else if (umsSysField.nonEmpty && umsSysField == "ums_op_"||name=="ums_op_") {
          val mappingRule = fieldInfo.umsSysMapping.get
          val iudMap = mappingRule.split(",").map(_.split("\\:")).map(arr => arr(1) -> arr(0)).toMap
          oneRecord.append(iudMap(jsonValue.getString(name)))
        }
        else oneRecord.append(jsonValue.getString(name))
      }
    }
    allFieldsInfo.foreach(fieldInfo => {
      dataProcess(fieldInfo, jsonParse)
    }
    )
    if (arrValue._2 > (-1)&&arrValue._1!=null) {
      arrValue._1.foreach(value => {
        val newRecord: ArrayBuffer[String] = oneRecord.clone()
        newRecord.insert(arrValue._2, value)
        resultSeq.append(UmsTuple(newRecord))
      }
      )
    }
      else if(jsonArrValue._2> (-1)&&jsonArrValue._1!=null){
      jsonArrValue._1.foreach(seqValue=>{
        val newRecord: ArrayBuffer[String] = oneRecord.clone()
        newRecord.insertAll(jsonArrValue._2,seqValue)
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
  def convertLongTimestamp(timestampStr: String) = {
    if (timestampStr.substring(0,2)=="20") {
      dt2timestamp(timestampStr)
    }
    else {
      val timestampLong = (timestampStr+"000000").substring(0,16).toLong
      dt2timestamp(timestampLong)
    }
  }*/


  //  def getFieldContentFromJson(json: String, fieldName: String): String = {
  //    var tmpValue = json
  //    var realKey = null.asInstanceOf[String]
  //    while (tmpValue != null) {
  //      val namespacePosition = tmpValue.indexOf("\"" + fieldName + "\"")
  //      tmpValue = tmpValue.substring(namespacePosition + fieldName.length + 2).trim
  //      if (tmpValue.startsWith(":")) {
  //        val from = tmpValue.indexOf("\"")
  //        val to = tmpValue.indexOf("\"", from + 1)
  //        realKey = tmpValue.substring(from + 1, to)
  //        tmpValue = null
  //      }
  //    }
  //    realKey
  //  }
  //
  //  def getProtocolTypeFromUms(ums: String): String = {
  //    var tmpValue = ums
  //    var realKey = null.asInstanceOf[String]
  //    while (tmpValue != null) {
  //      val strPosition = tmpValue.indexOf("\"protocol\"")
  //      println(strPosition)
  //      tmpValue = tmpValue.substring(strPosition + 10).trim
  //      if (tmpValue.startsWith(":")) {
  //        val from = tmpValue.indexOf("{")
  //        val to = tmpValue.indexOf("}")
  //        val subStr = tmpValue.substring(from, to + 1)
  //        if (subStr.contains("\"type\"")) {
  //          realKey = getFieldContentFromJson(subStr, "type")
  //          tmpValue = null
  //        }
  //      }
  //    }
  //    realKey
  //  }
   def getDefaultKeyConfig(specialConfig: Option[StreamSpecialConfig]): Boolean = {
    //log.info(s"stream special config is $specialConfig")
    try {
      specialConfig match {
        case Some(_) =>
          specialConfig.get.useDefaultKey.getOrElse(false)
        case None =>
          false
      }
    } catch {
      case e: Throwable =>
        log.error("parse stream specialConfig error, ", e)
        false
    }
  }

  def getDefaultKey(key: String, namespaces: Set[String], defaultKey: Boolean): String = {
/*    if(key != null) {
      log.info(s"getDefaultKey: key $key")
    } else {
      log.info(s"getDefaultKey: key null")
    }
    if(namespaces != null) {
      log.info(s"getDefaultKey: namespaces $namespaces, defaultKey $defaultKey")
    } else {
      log.info(s"getDefaultKey: namespaces null, $defaultKey")
    }*/
    if(!isRightKey(key) && null != namespaces && namespaces.nonEmpty && defaultKey) {
      //log.info(s"getDefaultKey: use default namespace ${namespaces.head} as kafka key, all namespace is $namespaces")
      UmsProtocolType.DATA_INCREMENT_DATA.toString + "." + namespaces.head
    } else {
      key
    }
  }

  def isRightKey(key: String): Boolean = {
    if(null == key || key.isEmpty) {
      false
    } else {
      if(key.split(".").length < 5) {
        false
      } else {
        true
      }
    }
  }

}