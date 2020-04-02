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


package edp.wormhole.sparkx.batchjob

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.sparkextension.udf.UdfRegister
import edp.wormhole.sparkx.batchflow.BatchflowMainProcess.logInfo
import edp.wormhole.sparkx.batchjob.transform.Transform
import edp.wormhole.sparkx.common.SparkUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsDataSystem, UmsSysField}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object BatchJobStarter extends App with EdpLogging {

  println(args(0))
  val base64Decode = new String(new sun.misc.BASE64Decoder().decodeBuffer(args(0).toString.split(" ").mkString("")))
  logInfo(s"config: $base64Decode")
  val batchJobConfig = JsonUtils.json2caseClass[BatchJobConfig](base64Decode)
  logInfo(s" batchJobConfig $batchJobConfig")
  val sourceConfig = batchJobConfig.sourceConfig
  val transformationConfig = batchJobConfig.transformationConfig
  val sinkConfig = batchJobConfig.sinkConfig
  val transformationList = checkAndGetTransformAction()
  val transformSpecialConfig = parseTransformSpecialConfig()
  val sinkSpecialConfig = parseSinkSpecialConfig()
  logInfo(s" sinkSpecialConfig $sinkSpecialConfig")
  val sparkSession = configSparkSession()
  registerUdf()

  val sourceDf = doSource()
  val transformDf = if (transformationList == null) sourceDf else {
    Transform.process(sparkSession, sourceConfig.sourceNamespace, sinkConfig.sinkNamespace, sourceDf, transformationList, Some(transformSpecialConfig.toString))
  }
  val projectionFields: Array[String] = getProjectionFields(transformDf).map(column => s"`$column`")
  var outPutTransformDf = transformDf.select(projectionFields.head, projectionFields.tail: _*)
  println("after!!!!!!!!!!! outPutTransformDf")

  if (sinkConfig.sinkNamespace.split("\\.")(0) == UmsDataSystem.PARQUET.toString) writeParquet()
  else writeSink()

  def writeSink(): Unit = {
    val limit = sinkConfig.maxRecordPerPartitionProcessed
    val sinkClassFullName = sinkConfig.classFullName.get
    val sinkNamespace = sinkConfig.sinkNamespace
    val sourceNamespace = sourceConfig.sourceNamespace
    val sinkConnectionConfig = sinkConfig.connectionConfig
    val sinkProcessConfig = SinkProcessConfig("", sinkConfig.tableKeys, sinkSpecialConfig, None, sinkClassFullName, 1, 1) //todo json to replace none
    val schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)] = SparkUtils.getSchemaMap(outPutTransformDf.schema, sinkProcessConfig.sinkUid)

    val mutationType = getMutationType(sinkClassFullName)
    outPutTransformDf.foreachPartition(partition => {
      val sendList = ListBuffer.empty[Seq[String]]
      val sinkClazz = Class.forName(sinkClassFullName)
      val sinkReflectObject: Any = sinkClazz.newInstance()
      val sinkTransformMethod = sinkClazz.getMethod("process", classOf[String], classOf[String], classOf[SinkProcessConfig], classOf[collection.Map[String, (Int, UmsFieldType, Boolean)]], classOf[Seq[Seq[String]]], classOf[ConnectionConfig])
      while (partition.hasNext) {
        val row = partition.next
        if (sendList.size < limit) {
          sendList += SparkUtils.getRowData(row, schemaMap)
        } else {
          sendList += SparkUtils.getRowData(row, schemaMap)
          val mergeSendList = mergeTuple(sendList, schemaMap, sinkProcessConfig, mutationType)
          sinkTransformMethod.invoke(sinkReflectObject, sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, mergeSendList, sinkConnectionConfig)
          sendList.clear()
          logInfo("do write sink loop")
        }
      }

      val mergeSendListLast = mergeTuple(sendList, schemaMap, sinkProcessConfig, mutationType)
      sinkTransformMethod.invoke(sinkReflectObject, sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, mergeSendListLast, sinkConnectionConfig)
    })
  }

  def getMutationType(classFullName: String): String = {
    val specialConfigJson: JSONObject = if (sinkSpecialConfig.isDefined) JSON.parseObject(sinkSpecialConfig.get) else new JSONObject()
    logInfo(s"specialConfigJson $specialConfigJson")
    if (specialConfigJson.containsKey("mutation_type"))
      specialConfigJson.getString("mutation_type").trim
    else if (classFullName.contains("Kafka"))
      SourceMutationType.INSERT_ONLY.toString
    else SourceMutationType.I_U_D.toString
  }


  def mergeTuple(sendList: Seq[Seq[String]], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], sinkProcessConfig: SinkProcessConfig, mutationType: String): Seq[Seq[String]] = {
   if (SourceMutationType.INSERT_ONLY.toString == mutationType) {
      logInfo("special config is i, merge not happen")
      sendList
    } else {
      logInfo("special config is not i, merge happen")
      SparkUtils.mergeTuple(sendList, schemaMap, sinkProcessConfig.tableKeyList)
    }
  }

  def writeParquet(): Unit = {
    //*   - `overwrite`: overwrite the existing data.
    //*   - `append`: append the data.
    //*   - `ignore`: ignore the operation (i.e. no-op).
    //*   - `error`: default option, throw an exception at runtime.

    var saveMode = "overwrite"
    if (sinkSpecialConfig.nonEmpty && sinkSpecialConfig.get.nonEmpty) {
      val specialJson = JSON.parseObject(sinkSpecialConfig.get)
      if (specialJson.containsKey("savemode")) saveMode = specialJson.getString("savemode")
    }
    outPutTransformDf.write.mode(saveMode).parquet(sinkConfig.connectionConfig.connectionUrl)
  }

  def parseSinkSpecialConfig(): Option[String] = {
    if (sinkConfig.specialConfig.isDefined) {
      val config = new String(new sun.misc.BASE64Decoder().decodeBuffer(sinkConfig.specialConfig.get.toString.split(" ").mkString("")))
      if(JSON.parseObject(config).containsKey("sink_specific_config"))
        Some(JSON.parseObject(config).getString("sink_specific_config"))
      else
        None
    } else None
  }

  def registerUdf(): Unit = {
    if (batchJobConfig.udfConfig.nonEmpty && batchJobConfig.udfConfig.get.nonEmpty) {
      batchJobConfig.udfConfig.get.foreach(udf => {
        val ifLoadJar = false
        UdfRegister.register(udf.udfName, udf.udfClassFullname, null, sparkSession, ifLoadJar)
      })
    }
  }

  def checkAndGetTransformAction(): Array[String] = {
    val transformationList: Array[String] = if (transformationConfig.isDefined && transformationConfig.get.action.isDefined)
      new String(new sun.misc.BASE64Decoder().decodeBuffer(transformationConfig.get.action.get.toString.split(" ").mkString(""))).split(";").map(_.trim) else null
    if (transformationList != null) transformationList.foreach(c =>
      assert(c.startsWith("spark_sql") || c.startsWith("custom_class"), "your actions are not started with spark_sql or custom_class."))
    transformationList
  }

  def parseTransformSpecialConfig(): JSONObject = {
    val transformSpecialConfig: JSONObject =
      if (transformationConfig.isDefined && transformationConfig.get.specialConfig.isDefined) {
        JSON.parseObject(transformationConfig.get.specialConfig.get)
      } else {
        new JSONObject()
      }
    transformSpecialConfig.fluentPut("start_time", sourceConfig.startTime)
    transformSpecialConfig.fluentPut("end_time", sourceConfig.endTime)
    transformSpecialConfig
  }

  def configSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .setMaster(batchJobConfig.jobConfig.master)
      .setAppName(batchJobConfig.jobConfig.appName)

    val executorCores = sparkConf.getOption("spark.executor.cores").get.toInt
    val numExecutors = sparkConf.getOption("spark.executor.instances").get.toInt
    val parallelismNum: Int = executorCores * numExecutors * 3
    sparkConf.set("spark.sql.shuffle.partitions", batchJobConfig.jobConfig.`spark.sql.shuffle.partitions`.getOrElse(parallelismNum).toString)

    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def doSource(): DataFrame = {
    val sourceClazz = Class.forName(sourceConfig.classFullName)
    val sourceReflectObject: Any = sourceClazz.newInstance()
    val sourceTransformMethod = sourceClazz.getMethod("process", classOf[SparkSession], classOf[String], classOf[String], classOf[String], classOf[ConnectionConfig], classOf[Option[String]])
    sourceTransformMethod.invoke(sourceReflectObject, sparkSession, sourceConfig.startTime, sourceConfig.endTime, sourceConfig.sourceNamespace, sourceConfig.connectionConfig, sourceConfig.specialConfig).asInstanceOf[DataFrame]
  }

  def getProjectionFields(transformDf: DataFrame): Array[String] = {
    if (sinkConfig.projection.isDefined) {
      var projectionStr = sinkConfig.projection.get
      if (projectionStr.indexOf(UmsSysField.TS.toString) < 0) {
        projectionStr = projectionStr + "," + UmsSysField.TS.toString
      }
      if (projectionStr.indexOf(UmsSysField.ID.toString) < 0) {
        projectionStr = projectionStr + "," + UmsSysField.ID.toString
      }

      if (projectionStr.indexOf(UmsSysField.UID.toString) < 0) {
        projectionStr = projectionStr + "," + UmsSysField.UID.toString
      }

      if (projectionStr.indexOf(UmsSysField.OP.toString) < 0) {
        projectionStr = projectionStr + "," + UmsSysField.OP.toString
      }
      projectionStr.split(",").map(_.trim.toLowerCase)
    } else transformDf.schema.fieldNames
  }

}



