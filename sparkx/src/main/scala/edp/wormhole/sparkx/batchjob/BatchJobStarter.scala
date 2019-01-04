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
import edp.wormhole.sparkx.batchjob.transform.Transform
import edp.wormhole.sparkx.common.SparkUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.udf.UdfRegister
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsDataSystem, UmsNamespace, UmsProtocolType, UmsSysField}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object BatchJobStarter extends App with EdpLogging {

  println(args(0))
  val base64Decode = new String(new sun.misc.BASE64Decoder().decodeBuffer(args(0).toString.split(" ").mkString("")))

  logInfo("config: " + base64Decode)

  val batchJobConfig = JsonUtils.json2caseClass[BatchJobConfig](base64Decode)
  val sourceConfig = batchJobConfig.sourceConfig
  val transformationConfig = batchJobConfig.transformationConfig
  val sinkConfig = batchJobConfig.sinkConfig

  val transformationList: Array[String] = if (transformationConfig.isDefined && transformationConfig.get.action.isDefined)
    new String(new sun.misc.BASE64Decoder().decodeBuffer(transformationConfig.get.action.get.toString.split(" ").mkString(""))).split(";").map(_.trim) else null
  if (transformationList != null) transformationList.foreach(c =>
    assert(c.startsWith("spark_sql") || c.startsWith("custom_class"), "your actions are not started with spark_sql or custom_class."))

  val transformSpecialConfig =
    if (transformationConfig.isDefined && transformationConfig.get.specialConfig.isDefined) {
      JSON.parseObject(transformationConfig.get.specialConfig.get)
    } else {
      new JSONObject()
    }
  transformSpecialConfig.fluentPut("start_time", sourceConfig.startTime)
  transformSpecialConfig.fluentPut("end_time", sourceConfig.endTime)

  val sparkConf = new SparkConf()
    .setMaster(batchJobConfig.jobConfig.master)
    .setAppName(batchJobConfig.jobConfig.appName)

  val executorCores = sparkConf.getOption("spark.executor.cores").get.toInt
  val numExecutors = sparkConf.getOption("spark.executor.instances").get.toInt
  val parallelismNum: Int = executorCores * numExecutors
  sparkConf.set("spark.sql.shuffle.partitions", batchJobConfig.jobConfig.`spark.sql.shuffle.partitions`.getOrElse(parallelismNum).toString)

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  if (batchJobConfig.udfConfig.nonEmpty && batchJobConfig.udfConfig.get.nonEmpty) {
    batchJobConfig.udfConfig.get.foreach(udf => {
      UdfRegister.register(udf.udfName, udf.udfClassFullname, null, sparkSession, false)
    })
  }

  val sourceClazz = Class.forName(sourceConfig.classFullName)
  val sourceReflectObject: Any = sourceClazz.newInstance()
  val sourceTransformMethod = sourceClazz.getMethod("process", classOf[SparkSession], classOf[String], classOf[String], classOf[String], classOf[ConnectionConfig], classOf[Option[String]])
  val sourceDf = sourceTransformMethod.invoke(sourceReflectObject, sparkSession, sourceConfig.startTime, sourceConfig.endTime, sourceConfig.sourceNamespace, sourceConfig.connectionConfig, sourceConfig.specialConfig).asInstanceOf[DataFrame]

  val transformDf = if (transformationList == null) sourceDf else {
    Transform.process(sparkSession, sourceDf, transformationList, Some(transformSpecialConfig.toString))
  }
  val projectionFields: Array[String] = getProjectionFields(transformDf).map(column => s"`$column`")
  var outPutTransformDf = transformDf.select(projectionFields.head, projectionFields.tail: _*)
  println("after!!!!!!!!!!! outPutTransformDf")

  val specialConfig: Option[String] = if (sinkConfig.specialConfig.isDefined) {
    val config = new String(new sun.misc.BASE64Decoder().decodeBuffer(sinkConfig.specialConfig.get.toString.split(" ").mkString("")))
    Some(JSON.parseObject(config).getString("sink_specific_config"))
  } else None
  if (sinkConfig.sinkNamespace.split("\\.")(0) == UmsDataSystem.PARQUET.toString) {
    //*   - `overwrite`: overwrite the existing data.
    //*   - `append`: append the data.
    //*   - `ignore`: ignore the operation (i.e. no-op).
    //*   - `error`: default option, throw an exception at runtime.

    var saveMode = "overwrite"
    if (specialConfig.nonEmpty && specialConfig.get.nonEmpty) {
      val specialJson = JSON.parseObject(specialConfig.get)
      if (specialJson.containsKey("savemode")) saveMode = specialJson.getString("savemode")
    }
    outPutTransformDf.write.mode(saveMode).parquet(sinkConfig.connectionConfig.connectionUrl)
  } else {
    val schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)] = SparkUtils.getSchemaMap(outPutTransformDf.schema)
    val limit = sinkConfig.maxRecordPerPartitionProcessed
    val sinkClassFullName = sinkConfig.classFullName.get
    val sinkNamespace = sinkConfig.sinkNamespace
    val sourceNamespace = sourceConfig.sourceNamespace
    val sinkConnectionConfig = sinkConfig.connectionConfig
    val sinkProcessConfig = SinkProcessConfig("", sinkConfig.tableKeys, specialConfig, None, sinkClassFullName, 1, 1) //todo json to replace none
    outPutTransformDf.foreachPartition(partition => {
      //println("!!!!TaskContext.getPartitionId:" + TaskContext.getPartitionId)
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
          sinkTransformMethod.invoke(sinkReflectObject, sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, sendList, sinkConnectionConfig)
          sendList.clear()
        }
      }
      sinkTransformMethod.invoke(sinkReflectObject, sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, sendList, sinkConnectionConfig)
    })
    //TODO feedback
    println("finish!!!!")
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



