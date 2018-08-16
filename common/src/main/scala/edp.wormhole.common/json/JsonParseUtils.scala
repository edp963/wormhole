package edp.wormhole.common.json

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.ums.UmsTuple
import edp.wormhole.util.DateUtils
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object JsonParseUtils {

  private lazy val logger = Logger.getLogger(this.getClass)

  def dataTypeProcess(dataType: String): String = {
    //    var result=dataType
    val typeArr: Array[String] = dataType.split("")
    val arrLen = typeArr.length
    if (typeArr.slice(arrLen - 5, arrLen).mkString("") == "array" && dataType != "jsonarray") "simplearray"
    else dataType
  }

  def convertLongTimestamp(timestampStr: String) = {
    if (timestampStr.substring(0,2)=="20") {
      DateUtils.dt2timestamp(timestampStr)
    }
    else {
      val timestampLong = (timestampStr+"000000").substring(0,16).toLong
      DateUtils.dt2timestamp(timestampLong)
    }
  }

  def dataParse(jsonStr: String, allFieldsInfo: Seq[FieldInfo], twoFieldsArr: ArrayBuffer[(String, String)]): Seq[UmsTuple] = {
    //logger.info("----------------to dataParse")
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
      //logger.info("fieldInfo:" + fieldInfo + ";jsonValue" + jsonValue +";----------------dataProcess")
      //logger.info("name:" + name + ";dataTypeProcessed" + dataTypeProcessed + "fieldIndex:" + fieldIndex + ";umsSysField" + umsSysField +  ";subFields" + subFields + ";----------------dataProcess")
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
          oneRecord.append(convertLongTimestamp(jsonValue.getString(name)).toString)
        }
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
}
