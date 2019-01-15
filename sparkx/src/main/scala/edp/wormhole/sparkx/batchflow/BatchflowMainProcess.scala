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


package edp.wormhole.sparkx.batchflow

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.common.feedback.FeedbackPriority
import edp.wormhole.common.json.FieldInfo
import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.common.InputDataProtocolBaseType
import edp.wormhole.externalclient.zookeeper.WormholeZkClient
import edp.wormhole.sinks.elasticsearchsink.EsConfig
import edp.wormhole.sinks.mongosink.MongoConfig
import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.sparkx.batchflow.BatchflowStarter.config
import edp.wormhole.sparkx.common._
import edp.wormhole.sparkx.directive.UdfDirective
import edp.wormhole.sparkx.memorystorage.{ConfMemoryStorage, OffsetPersistenceManager}
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.swifts.transform.SwiftsTransform
import edp.wormhole.sparkx.swifts.validity.ValidityAgainstAction
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, ValidityConfig}
import edp.wormhole.sparkx.swifts.validity.ValidityCheckRule
import edp.wormhole.swifts.ConnectionMemoryStorage
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import edp.wormhole.util.{DateUtils, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.WormholeDirectKafkaInputDStream
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.postfixOps

object BatchflowMainProcess extends EdpLogging {

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, kafkaInput: KafkaInputConfig,session: SparkSession,
              appId:String): Unit = {
    var zookeeperFlag = false
    stream.foreachRDD((streamRdd: RDD[ConsumerRecord[String, String]]) => {
      WormholeKafkaProducer.init(config.kafka_output.brokers, config.kafka_output.config,config.kerberos)

      val topics=kafkaInput.kafka_topics.map(config=>JsonUtils.jsonCompact(JsonUtils.caseClass2json[KafkaTopicConfig](config))).mkString("[",",","]")
      val offsetInfo: ArrayBuffer[OffsetRange] = getOffsets(streamRdd)
      val batchId = UUID.randomUUID().toString
      try {
        logInfo("start foreachRDD")
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())

        val rddTs = System.currentTimeMillis
        // val session = SparkSession.builder().config(streamRdd.sparkContext.getConf).getOrCreate()

        logInfo("start doDirectiveTopic")
        val directiveTs = System.currentTimeMillis
        BatchflowDirective.doDirectiveTopic(config, stream)

        logInfo("start Repartition")
        val mainDataTs = System.currentTimeMillis
        //val dt1 =  dt2dateTime(currentyyyyMMddHHmmss)

        val dataRepartitionRdd: RDD[(String, String)] = if (config.rdd_partition_number != -1) streamRdd.map(row => {
          (UmsCommonUtils.checkAndGetKey(row.key, row.value), row.value)
        }).repartition(config.rdd_partition_number) else streamRdd.map(row => {
          (UmsCommonUtils.checkAndGetKey(row.key, row.value), row.value)
        })
        UdfDirective.registerUdfProcess(config.kafka_output.feedback_topic_name, config.kafka_output.brokers, session)
        //        dataRepartitionRdd.cache()
        //        dataRepartitionRdd.count()
        //        val dt2: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("repartition dataRepartitionRdd duration:   " + dt2 + " - "+ dt1 +" = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " seconds."))


        logInfo("start create classifyRdd")
        val classifyRdd: RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])] = getClassifyRdd(dataRepartitionRdd).cache()
        val distinctSchema: mutable.Map[(UmsProtocolType, String), (Seq[UmsField], Long)] = getDistinctSchema(classifyRdd)
        //        classifyRdd.count
        //        val dt3: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("get classifyRdd duration:   " + dt3 + " - "+ dt2 +" = " + (Seconds.secondsBetween(dt2, dt3).getSeconds() % 60 + " seconds."))
        logInfo("start doStreamLookupData")


        //        val streamMergeTs = System.currentTimeMillis
        doStreamLookupData(session, classifyRdd, config, distinctSchema)
        //               val dt4: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //               println("get doStreamLookupData duration:   " + dt4 + " - "+ dt3 +" = " + (Seconds.secondsBetween(dt3, dt4).getSeconds() % 60 + " seconds."))
        logInfo("start doMainData")

        //   val dt5: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        val processedSourceNamespace = doMainData(session, classifyRdd, config, batchId, rddTs, directiveTs, mainDataTs, distinctSchema,topics)
        //        val dt6: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("get doMainData duration:   " + dt6 + " - "+ dt5 +" = " + (Seconds.secondsBetween(dt5, dt6).getSeconds() % 60 + "seconds"))
        //
        logInfo("start doOtherData")
        val nonDataArray = classifyRdd.flatMap(_._3).collect()
        doOtherData(nonDataArray, config, processedSourceNamespace, batchId,topics)

        logInfo("start storeTopicPartition")
        WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config, batchId)

        classifyRdd.unpersist()
      } catch {
        case e: Throwable =>
          logAlert("batch error", e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackStreamBatchError(config.spark_config.stream_id, DateUtils.currentDateTime, UmsFeedbackStatus.FAIL, e.getMessage, batchId), None, config.kafka_output.brokers)
          WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config, batchId)
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetInfo.toArray)
      if(!zookeeperFlag){
        logInfo("write appid to zookeeper,"+appId)
        SparkContextUtils.checkSparkRestart(config.zookeeper_address, config.zookeeper_path, config.spark_config.stream_id, appId)
        SparkContextUtils.deleteZookeeperOldAppidPath(appId, config.zookeeper_address, config.zookeeper_path, config.spark_config.stream_id)
        WormholeZkClient.createPath(config.zookeeper_address, config.zookeeper_path + "/" + config.spark_config.stream_id + "/" + appId)
        zookeeperFlag=true
      }
    }
    )
  }


  private def getClassifyRdd(dataRepartitionRdd: RDD[(String, String)]): RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])] = {
    val streamLookupNamespaceSet = ConfMemoryStorage.getAllLookupNamespaceSet
    val mainNamespaceSet = ConfMemoryStorage.getAllMainNamespaceSet
    val jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])] = ConfMemoryStorage.getAllSourceParseMap
    dataRepartitionRdd.mapPartitions(partition => {
      val mainDataList = ListBuffer.empty[((UmsProtocolType, String), Seq[UmsTuple])]
      val lookupDataList = ListBuffer.empty[((UmsProtocolType, String), Seq[UmsTuple])]
      val otherList = ListBuffer.empty[String]
      val nsSchemaMap = mutable.HashMap.empty[(UmsProtocolType, String), Seq[UmsField]]
      partition.foreach(row => {
        try {
          val (protocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(row._1)
          if (protocolType == UmsProtocolType.DATA_INCREMENT_DATA || protocolType == UmsProtocolType.DATA_BATCH_DATA || protocolType == UmsProtocolType.DATA_INITIAL_DATA) {
            if (ConfMemoryStorage.existNamespace(mainNamespaceSet, namespace)) {
              val schemaValueTuple: (Seq[UmsField], Seq[UmsTuple]) = WormholeUtils.jsonGetValue(namespace, protocolType, row._2, jsonSourceParseMap)
              if (!nsSchemaMap.contains((protocolType, namespace))) nsSchemaMap((protocolType, namespace)) = schemaValueTuple._1.map(f => UmsField(f.name.toLowerCase, f.`type`, f.nullable))
              mainDataList += (((protocolType, namespace), schemaValueTuple._2))
            }
            if (ConfMemoryStorage.existNamespace(streamLookupNamespaceSet, namespace)) {
              //todo change  if back to if, efficiency
              val schemaValueTuple: (Seq[UmsField], Seq[UmsTuple]) = WormholeUtils.jsonGetValue(namespace, protocolType, row._2, jsonSourceParseMap)
              if (!nsSchemaMap.contains((protocolType, namespace))) nsSchemaMap((protocolType, namespace)) = schemaValueTuple._1.map(f => UmsField(f.name.toLowerCase, f.`type`, f.nullable))
              lookupDataList += (((protocolType, namespace), schemaValueTuple._2))
            }
          } else if (checkOtherData(protocolType.toString)) otherList += row._2
          else logDebug("namespace:" + namespace + ", do not config")
        } catch {
          case e1: Throwable => logAlert("do classifyRdd,one data has error,row:" + row, e1)
        }
      })
      List((mainDataList, lookupDataList, otherList, nsSchemaMap.toArray)).toIterator
    })
  }

  private def getOffsets(streamRdd: RDD[ConsumerRecord[String, String]]): ArrayBuffer[OffsetRange] = {
    val offsetInfo: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]
    streamRdd.asInstanceOf[HasOffsetRanges].offsetRanges.copyToBuffer(offsetInfo)
    offsetInfo
  }

  def checkOtherData(protocolType: String): Boolean = {
    protocolType.startsWith("directive_") || protocolType.endsWith("_heartbeat") || protocolType.endsWith("_termination")
  }


  private def getMinMaxTsAndCount(protocolType: UmsProtocolType, sourceNamespace: String, umsRdd: RDD[Seq[String]], fields: Seq[UmsField]): (String, String, Int) = {
    val umsTsIndex = fields.map(_.name).indexOf(UmsSysField.TS.toString)
    val minMaxCountArray: Array[(String, String, Int)] = umsRdd.mapPartitions(partition => {
      var minTs = ""
      var maxTs = ""
      var count = 0
      partition.foreach(umsRow => {
        count += 1
        val dataTs = umsRow(umsTsIndex)
        if (minTs.isEmpty || !SinkCommonUtils.firstTimeAfterSecond(dataTs, minTs)) minTs = dataTs
        if (SinkCommonUtils.firstTimeAfterSecond(dataTs, maxTs)) maxTs = dataTs
      })
      List((minTs, maxTs, count)).toIterator
    }).collect()

    var minTs = ""
    var maxTs = ""
    var count = 0
    minMaxCountArray.foreach(row => {
      if (row._3 > 0) {
        count += row._3
        if (minTs.isEmpty || !SinkCommonUtils.firstTimeAfterSecond(row._1, minTs)) minTs = row._1
        if (SinkCommonUtils.firstTimeAfterSecond(row._2, maxTs)) maxTs = row._2
      }
    })
    (minTs, maxTs, count)
  }


  private def doStreamLookupData(session: SparkSession, allDataRdd: RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])], config: WormholeConfig, distinctSchema: mutable.Map[(UmsProtocolType, String), (Seq[UmsField], Long)]): Unit = {
    try { // join in streaming, file name： sourcenamespace 4 fields _ sinknamespace_lookup namespace 4 fields
      val umsRdd: RDD[(UmsProtocolType, String, ArrayBuffer[Seq[String]])] = formatRdd(allDataRdd, "lookup")
      distinctSchema.foreach(schema => {
        val namespace = schema._1._2
        val matchLookupNamespace = ConfMemoryStorage.getMatchLookupNamespaceRule(namespace)
        if (matchLookupNamespace != null) {
          val protocolType: UmsProtocolType = schema._1._1
          val lookupDf = createSourceDf(session, namespace, schema._2._1, umsRdd.filter(row => {
            row._1 == protocolType && row._2 == namespace
          }).flatMap(_._3))
          ConfMemoryStorage.getSourceAndSinkByStreamLookupNamespace(matchLookupNamespace).foreach {
            case (sourceNs, sinkNs) =>
              val path = config.stream_hdfs_address.get + "/" + "swiftsparquet" + "/" + config.spark_config.stream_id + "/" + sourceNs.replaceAll("\\*", "-") + "/" + sinkNs + "/streamLookupNamespace" + "/" + matchLookupNamespace.replaceAll("\\*", "-")
              lookupDf.write.mode(SaveMode.Append).parquet(path) //if not exists will have "WARN: delete very recently?" it is ok.
          }
        }
      })
    } catch {
      case e: Throwable => logAlert("doStreamLookupData", e)
    }
  }

  private def doMainData(session: SparkSession,
                         mainDataRdd: RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])],
                         config: WormholeConfig,
                         batchId: String,
                         rddTs: Long,
                         directiveTs: Long,
                         mainDataTs: Long,
                         distinctSchema: mutable.Map[(UmsProtocolType, String), (Seq[UmsField], Long)],
                         topics:String): Set[String] = {
    val processedSourceNamespace = mutable.HashSet.empty[String]
    // val dt1: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    val umsRdd: RDD[(UmsProtocolType, String, ArrayBuffer[Seq[String]])] = formatRdd(mainDataRdd, "main").cache
    distinctSchema.foreach(schema => {
      val uuid = UUID.randomUUID().toString
      val protocolType: UmsProtocolType = schema._1._1
      val sourceNamespace: String = schema._1._2
      logInfo(uuid + ",schema loop,sourceNamespace:" + sourceNamespace)
      val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)

      val sourceTupleRDD: RDD[Seq[String]] = umsRdd.filter(row => {
        row._1 == protocolType && row._2 == sourceNamespace
      }).flatMap(_._3).cache

      //  val jsonUmsSysFields: UmsSysRename = if (ConfMemoryStorage.existJsonSourceParseMap(protocolType,sourceNamespace)) ConfMemoryStorage.getJsonUmsFieldsName(protocolType,sourceNamespace) else null
      val (minTs, maxTs, count) = getMinMaxTsAndCount(protocolType, sourceNamespace, sourceTupleRDD, schema._2._1) //,jsonUmsSysFields)
      logInfo(uuid + "sourceNamespace:" + sourceNamespace + ",minTs:" + minTs + ",maxTs:" + maxTs + ",sourceDf.count:" + count)
      if (count > 0) {
        val flowConfigMap = ConfMemoryStorage.getFlowConfigMap(matchSourceNamespace)
        flowConfigMap.foreach(flow => {
          val isProcessed = protocolType match {
            case UmsProtocolType.DATA_INCREMENT_DATA =>
              flow._2._6(InputDataProtocolBaseType.INCREMENT.toString)
            case UmsProtocolType.DATA_INITIAL_DATA =>
              flow._2._6(InputDataProtocolBaseType.INITIAL.toString)
            case UmsProtocolType.DATA_BATCH_DATA =>
              flow._2._6(InputDataProtocolBaseType.BATCH.toString)
          }
          if (isProcessed) {
            val sinkNamespace = flow._1
            logInfo(uuid + ",do flow,matchSourceNamespace:" + matchSourceNamespace + ",sinkNamespace:" + sinkNamespace)
            val swiftsTs = System.currentTimeMillis
            ConfMemoryStorage.setEventTs(matchSourceNamespace, sinkNamespace, minTs)
            val (swiftsProcessConfig: Option[SwiftsProcessConfig], sinkProcessConfig, _, _, _, _) = flow._2
            logInfo(uuid + ",start swiftsProcess")

            var sinkFields: Seq[UmsField] = schema._2._1
            var sinkRDD: RDD[Seq[String]] = sourceTupleRDD
            var afterUnionDf: DataFrame = null

            if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.swiftsSql.nonEmpty) {

              val (returnUmsFields, tuplesRDD, unionDf) = swiftsProcess(swiftsProcessConfig, uuid, session, sourceTupleRDD, config, sourceNamespace, sinkNamespace, minTs, maxTs, count, sinkFields, batchId)
              sinkFields = returnUmsFields
              sinkRDD = tuplesRDD
              afterUnionDf = unionDf
            }

            val sinkTs = System.currentTimeMillis
            if (sinkRDD != null) {
              try {
                validityAndSinkProcess(protocolType, sourceNamespace, sinkNamespace, session, sinkRDD, sinkFields, afterUnionDf, swiftsProcessConfig, sinkProcessConfig, config, minTs, maxTs, uuid) //,jsonUmsSysFields)
              }
              catch {
                case e: Throwable =>
                  logAlert("sink,sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + ",count=" + count, e)
                  WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackFlowError(sourceNamespace, config.spark_config.stream_id, DateUtils.currentDateTime, sinkNamespace, UmsWatermark(maxTs), UmsWatermark(minTs), count, "", batchId), None, config.kafka_output.brokers)
              }
            } else logWarning("sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + "there is nothing to sinkProcess")

            if (afterUnionDf != null) afterUnionDf.unpersist()
            val doneTs = System.currentTimeMillis
            processedSourceNamespace.add(sourceNamespace)
            WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
              UmsProtocolUtils.feedbackFlowStats(sourceNamespace, protocolType.toString, DateUtils.currentDateTime, config.spark_config.stream_id, batchId, sinkNamespace,topics,
                count, DateUtils.dt2date(maxTs.split("\\+")(0).replace("T", " ")).getTime, rddTs, directiveTs, mainDataTs, swiftsTs, sinkTs, doneTs.toString), None, config.kafka_output.brokers)
          }
        }
        )
      }
      sourceTupleRDD.unpersist()
    }

    )
    umsRdd.unpersist()
    processedSourceNamespace.toSet
  }

  private def unionParquetNonTimeoutDf(swiftsProcessConfig: Option[SwiftsProcessConfig],
                                       uuid: String,
                                       session: SparkSession,
                                       sourceDf: DataFrame,
                                       config: WormholeConfig,
                                       sourceNamespace: String,
                                       sinkNamespace: String
                                      ): DataFrame = {
    if (swiftsProcessConfig.get.validityConfig.isDefined) {
      val parquetAddr = config.stream_hdfs_address.get + "/" + "swiftsparquet" + "/" + config.spark_config.stream_id + "/" + sourceNamespace + "/" + sinkNamespace + "/mainNamespace"
      val configuration = new Configuration()
      if (HdfsUtils.isParquetPathReady(configuration, parquetAddr)) {
        logInfo(uuid + ",swiftsProcessConfig.nonEmpty,and readMainParquetDf")
        sourceDf.union(session.read.parquet(parquetAddr))
      }
      else {
        logInfo(uuid + ",swiftsProcessConfig.nonEmpty,but parquet path not ready")
        sourceDf
      }
    } else {
      logInfo(uuid + ",swiftsProcessConfig.nonEmpty,but do not read parquet")
      sourceDf
    }
  }


  private def swiftsProcess(swiftsProcessConfig: Option[SwiftsProcessConfig],
                            uuid: String,
                            session: SparkSession,
                            sourceTupleRDD: RDD[Seq[String]],
                            config: WormholeConfig,
                            sourceNamespace: String,
                            sinkNamespace: String,
                            minTs: String,
                            maxTs: String,
                            count: Int,
                            umsFields: Seq[UmsField],
                            batchId: String): (Seq[UmsField], RDD[Seq[String]], DataFrame) = {
    val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)
    val sourceDf = createSourceDf(session, sourceNamespace, umsFields, sourceTupleRDD)
    val dataSetShow = swiftsProcessConfig.get.datasetShow
    if (dataSetShow.get) {
      sourceDf.show(swiftsProcessConfig.get.datasetShowNum.get)
    }

    val afterUnionDf = unionParquetNonTimeoutDf(swiftsProcessConfig, uuid, session, sourceDf, config, sourceNamespace, sinkNamespace).cache
    //println("sourceNamespace=" + sourceNamespace + ",afterUnionDf.count" + afterUnionDf.count)

    try {
      val swiftsDf: DataFrame = SwiftsTransform.transform(session, sourceNamespace, sinkNamespace, afterUnionDf, matchSourceNamespace, config)
      val resultSchema = swiftsDf.schema
      val nameIndex: Array[(String, Int, DataType)] = resultSchema.fieldNames.map(name => (name, resultSchema.fieldIndex(name), resultSchema.apply(resultSchema.fieldIndex(name)).dataType)).sortBy(_._2)

      import session.implicits._

      val umsFields: Seq[UmsField] = nameIndex.map(t => {
        UmsField(t._1, SparkUtils.sparkSqlType2UmsFieldType(t._3.toString), Some(true))
      }).toSeq
      val tuples: RDD[Seq[String]] = swiftsDf.map {
        row =>
          nameIndex.map { case (_, index, dataType) => {
            val value = SparkUtils.sparkValue2Object(row.get(index), dataType)
            if (value == null) null else value.toString
          }
          }.toSeq
      }.rdd
      (umsFields, tuples, afterUnionDf)
    } catch {
      case e: Throwable =>
        logAlert(uuid + "swifts,sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + ",count=" + count, e)
        WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackFlowError(sourceNamespace, config.spark_config.stream_id, DateUtils.currentDateTime, sinkNamespace, UmsWatermark(maxTs), UmsWatermark(minTs), count, "", batchId), None, config.kafka_output.brokers)
        (null, null, afterUnionDf)
    }
  }


  private def createSourceDf(session: SparkSession, sourceNamespace: String, fields: Seq[UmsField], sourceTupleRDD: RDD[Seq[String]]) = {
    val rowRdd: RDD[Row] = sourceTupleRDD.flatMap(row => SparkUtils.umsToSparkRowWrapper(sourceNamespace, fields, row))
    //    fields.foreach(field=>{
    //      logInfo("schema:::"+field.name)
    //    })
    //    rowRdd.collect().foreach(row=>{
    //      logInfo("content1:::"+row.toSeq)
    //      logInfo("content2:::"+row.get(0)+","+row.get(1))
    //    })
    SparkSchemaUtils.createDf(session, fields, rowRdd)
  }

  private def getDistinctSchema(umsRdd: RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])]): mutable.Map[(UmsProtocolType.UmsProtocolType, String), (Seq[UmsField], Long)] = {
    val schemaMap = mutable.HashMap.empty[(UmsProtocolType, String), (Seq[UmsField], Long)]
    umsRdd.map(_._4).collect().foreach(_.foreach {
      case ((protocol, ns), schema) =>
        if (!schemaMap.contains((protocol, ns))) {
          val matchSourceNs = ConfMemoryStorage.getMatchSourceNamespaceRule(ns)
          val directiveId = ConfMemoryStorage.getFlowConfigMap(matchSourceNs).head._2._3
          schemaMap((protocol, ns)) = (schema, directiveId)
        }
    })
    mutable.LinkedHashMap(schemaMap.toSeq.sortBy(_._2._2): _*)
  }

  private def formatRdd(allDataRdd: RDD[(ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[((UmsProtocolType, String), Seq[UmsTuple])], ListBuffer[String], Array[((UmsProtocolType, String), Seq[UmsField])])], dataType: String): RDD[(UmsProtocolType, String, ArrayBuffer[Seq[String]])] = {
    allDataRdd.mapPartitions(par => {
      val namespace2ValueMap = mutable.HashMap.empty[(UmsProtocolType, String), ArrayBuffer[Seq[String]]] //[(protocoltype,namespace),(seq[umsfield],array[seq[string]]

      par.foreach(allList => {
        val formatList: mutable.Seq[((UmsProtocolType, String), Seq[UmsTuple])] = if (dataType == "main") allList._1 else allList._2
        formatList.foreach(row => {
          if (namespace2ValueMap.contains((row._1._1, row._1._2))) {
            namespace2ValueMap((row._1._1, row._1._2)) ++= row._2.map(_.tuple)
          } else {
            val tuple = ArrayBuffer.empty[Seq[String]]
            tuple ++= row._2.map(_.tuple)
            namespace2ValueMap((row._1._1, row._1._2)) = tuple
          }
        })
      })
      namespace2ValueMap.map(ele => {
        (ele._1._1, ele._1._2, ele._2)
      }).toIterator
    })
  }

  private def mergeTuple(dataSeq: Seq[Seq[String]], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], tableKeyList: List[String]): Seq[Seq[String]] = {
    val keys2TupleMap = new mutable.HashMap[String, Seq[String]] //[keys,tuple]
    dataSeq.foreach(dataArray => {
      val opValue = SinkCommonUtils.fieldValue(UmsSysField.OP.toString, schemaMap, dataArray)
      if (UmsOpType.BEFORE_UPDATE.toString != opValue) {
        val keyValues = SinkCommonUtils.keyList2values(tableKeyList, schemaMap, dataArray)
        val idInTuple = dataArray(schemaMap(UmsSysField.ID.toString)._1).toLong
        if (!keys2TupleMap.contains(keyValues) || (idInTuple > keys2TupleMap(keyValues)(schemaMap(UmsSysField.ID.toString)._1).toLong)) {
          keys2TupleMap(keyValues) = dataArray
        }
      }
    })
    keys2TupleMap.values.toList
  }

  private def validityAndSinkProcess(protocolType: UmsProtocolType,
                                     sourceNamespace: String,
                                     sinkNamespace: String,
                                     session: SparkSession,
                                     sinkRDD: RDD[Seq[String]],
                                     sinkFields: Seq[UmsField],
                                     streamUnionParquetDf: DataFrame,
                                     swiftsProcessConfig: Option[SwiftsProcessConfig],
                                     sinkProcessConfig: SinkProcessConfig,
                                     config: WormholeConfig,
                                     minTs: String,
                                     maxTs: String,
                                     uuid: String) = {
    val connectionConfig = ConnectionMemoryStorage.getDataStoreConnectionConfig(sinkNamespace)
    val (resultSchemaMap: Map[String, (Int, UmsFieldType, Boolean)], originalSchemaMap: Map[String, (Int, UmsFieldType, Boolean)], renameMap: Option[Map[String, String]]) = SparkUtils.getSchemaMap(sinkFields, sinkProcessConfig.sinkOutput)
    logInfo(uuid + s",$sinkNamespace schemaMap:" + resultSchemaMap)
    val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)

    val specialConfigJson: JSONObject = if (sinkProcessConfig.specialConfig.isDefined) JSON.parseObject(sinkProcessConfig.specialConfig.get) else new JSONObject()

    val mutationType =
      if (specialConfigJson.containsKey("mutation_type")) specialConfigJson.getString("mutation_type").trim
      else if (sinkProcessConfig.classFullname.contains("Kafka")) SourceMutationType.INSERT_ONLY.toString
      else SourceMutationType.I_U_D.toString

    val repartitionRDD = if (SourceMutationType.INSERT_ONLY.toString != mutationType) {
      val ids = if (sinkNamespace.startsWith(UmsDataSystem.ES.toString)) JsonUtils.json2caseClass[EsConfig](sinkProcessConfig.specialConfig.get).`_id.get`.toList
      else if (sinkNamespace.startsWith(UmsDataSystem.MONGODB.toString)) JsonUtils.json2caseClass[MongoConfig](sinkProcessConfig.specialConfig.get).`_id.get`.toList
      else sinkProcessConfig.tableKeyList
      logInfo("sinkProcessConfig.tableKeys.nonEmpty")
      val columnsIndex: List[Int] = ids.map(name => originalSchemaMap(name)._1)
      sinkRDD.map(t => (columnsIndex.map(x => t(x)).mkString("_"), t)).partitionBy(new HashPartitioner(config.rdd_partition_number)).map(_._2)
    } else {
      logInfo("SourceMutationType.INSERT_ONLY.toString == mutationType")
      sinkRDD
    }

    val send2saveData: RDD[(Seq[Seq[String]], ListBuffer[String])] = repartitionRDD.mapPartitions((partition: Iterator[Seq[String]]) => {

      if (partition.nonEmpty) {
        logInfo(uuid + ",partition.nonEmpty")

        val (sendList: ListBuffer[Seq[String]], saveList: ListBuffer[String]) = doValidityAndGetData(swiftsProcessConfig, partition, resultSchemaMap, originalSchemaMap, renameMap, minTs, sourceNamespace, sinkNamespace) //,jsonUmsSysFields)

        //        sendList.foreach(data => logInfo("before merge:" + data))
        logInfo(uuid + ",@sendList size: " + sendList.size + " saveList size: " + saveList.size)
        val mergeSendList: Seq[Seq[String]] = if (SourceMutationType.INSERT_ONLY.toString == mutationType) {
          logInfo(uuid + "special config is i, merge not happen")
          sendList
        } else {
          logInfo(uuid + "special config not i, merge happen")
          mergeTuple(sendList, resultSchemaMap, sinkProcessConfig.tableKeyList)
        }
        logInfo(uuid + ",@mergeSendList size: " + mergeSendList.size)


        //        val (sinkObject, sinkMethod) = ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
        //
        //        sinkMethod.invoke(sinkObject,session, protocolType, sourceNamespace, sinkNamespace, sinkProcessConfig, resultSchemaMap, mergeSendList, connectionConfig)
        //todo add rename mapping to sink, and revise sink part

        List((mergeSendList, saveList)).toIterator
      } else {
        logInfo(uuid + ",partition data(payload) size is 0,do not process sink")
        List.empty[(ListBuffer[Seq[String]], ListBuffer[String])].toIterator
      }
    }).cache()

    send2saveData.foreachPartition(partition => {
      val (sinkObject, sinkMethod) = ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)
      sinkMethod.invoke(sinkObject, sourceNamespace, sinkNamespace, sinkProcessConfig, resultSchemaMap, partition.flatMap(_._1).toList, connectionConfig)

    })


    val nonTimeoutUids: Array[String] = send2saveData.mapPartitions(par => {
      par.flatMap(_._2)
    }).collect()


    send2saveData.unpersist()

    // val dt3: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    //  println("In validityAndSinkProcess, writetoSInk duration:   " + dt3 + " - "+ dt2 +" = " + (Seconds.secondsBetween(dt2, dt3).getSeconds() % 60 + " seconds."))

    if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.validityConfig.nonEmpty) {
      if (nonTimeoutUids != null && nonTimeoutUids.length > 0)
        failureAndNonTimeoutProcess(sourceNamespace, sinkNamespace, nonTimeoutUids, streamUnionParquetDf, config)
    }
    if (ConfMemoryStorage.existEventTs(matchSourceNamespace, sinkNamespace)) {
      val currentMinTs = ConfMemoryStorage.getEventTs(matchSourceNamespace, sinkNamespace)
      val minTime = if (SinkCommonUtils.firstTimeAfterSecond(minTs, currentMinTs)) currentMinTs else minTs
      if (ConfMemoryStorage.existStreamLookup(matchSourceNamespace, sinkNamespace))
        streamJoinTimeoutProcess(matchSourceNamespace, sinkNamespace, config, minTime, session) //,jsonUmsSysFields)
    }
  }


  private def checkValidity(validityConfig: ValidityConfig, originalDataArray: ArrayBuffer[String], originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): Boolean

  = {
    val ifValidity = if (ValidityCheckRule.OR.toString == validityConfig.checkRule) false else true
    validityConfig.checkColumns.foreach(checkColumn => {
      val checkData = originalDataArray(originalSchemaMap(checkColumn)._1)
      if (ValidityCheckRule.AND.toString == validityConfig.checkRule) {
        if (checkData == null || checkData == "") return false
      } else {
        if (checkData != null && checkData != "") return true
      }
    })
    ifValidity
  }

  private def doValidityAndGetData(swiftsProcessConfig: Option[SwiftsProcessConfig],
                                   dataSeq: Iterator[Seq[String]],
                                   resultSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                                   originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                                   renameMap: Option[Map[String, String]],
                                   minTs: String,
                                   sourceNamespace: String,
                                   sinkNamespace: String): (mutable.ListBuffer[Seq[String]], mutable.ListBuffer[String])

  = {
    val sendList = ListBuffer.empty[Seq[String]]
    val saveList = ListBuffer.empty[String]
    if (swiftsProcessConfig.nonEmpty) {
      //has swifts process
      if (swiftsProcessConfig.get.validityConfig.nonEmpty) {
        val validityConfig: ValidityConfig = swiftsProcessConfig.get.validityConfig.get
        dataSeq.foreach(row => {
          val originalDataArray = SparkUtils.getRowData(row, originalSchemaMap, originalSchemaMap, renameMap)
          val ifValidity = checkValidity(validityConfig, originalDataArray, originalSchemaMap)
          if (ifValidity) sendList += SparkUtils.getRowData(row, resultSchemaMap, originalSchemaMap, renameMap)
          else {
            val reduceTime = DateUtils.yyyyMMddHHmmss(DateUtils.dt2dateTime(minTs).minusSeconds(validityConfig.ruleParams.toInt))
            val dataUmsts = DateUtils.yyyyMMddHHmmss(DateUtils.dt2dateTime(originalDataArray(originalSchemaMap(UmsSysField.TS.toString)._1)))
            val uid = originalDataArray(originalSchemaMap(UmsSysField.UID.toString)._1)
            if (SinkCommonUtils.firstTimeAfterSecond(reduceTime, dataUmsts)) {
              //timeout
              ValidityAgainstAction.toValidityAgainstAction(validityConfig.againstAction) match {
                case ValidityAgainstAction.DROP =>
                  logWarning(sourceNamespace + ":" + sinkNamespace + ":uid=" + uid + " not be joined and dropped")
                case ValidityAgainstAction.ALERT =>
                  logAlert(sourceNamespace + ":" + sinkNamespace + ":uid=" + uid + " not be joined and alerted")
                case ValidityAgainstAction.SEND =>
                  logWarning(sourceNamespace + ":" + sinkNamespace + ":uid=" + uid + " not be joined and sent")
                  sendList += SparkUtils.getRowData(row, resultSchemaMap, originalSchemaMap, renameMap)
                case _ => throw new Exception("join failed Df, " + validityConfig.againstAction + " is not supported")
              }
            } else {
              //not timeout
              saveList += uid
            }
          }
        })
      } else sendList ++= dataSeq.map(row => SparkUtils.getRowData(row, resultSchemaMap, originalSchemaMap, renameMap)) //has swifts process and not lack column and not need validity
    } else sendList ++= dataSeq.map(row => SparkUtils.getRowData(row, resultSchemaMap, originalSchemaMap, renameMap)) //not swifts process
    logInfo(sourceNamespace + ":" + sinkNamespace + ",sendList.size=" + sendList.size + ",saveList.size=" + saveList.size)
    (sendList, saveList)
  }


  private def streamJoinTimeoutProcess(matchSourceNamespace: String,
                                       sinkNamespace: String,
                                       config: WormholeConfig,
                                       minTs: String,
                                       session: SparkSession
                                      )

  = {
    ConfMemoryStorage.getStreamLookupNamespaceAndTimeout(matchSourceNamespace, sinkNamespace).foreach {
      case (lookupNamespace, timeout) =>
        val parquetAddr = config.stream_hdfs_address.get + "/" + "swiftsparquet" + "/" + config.spark_config.stream_id + "/" + matchSourceNamespace.replaceAll("\\*", "-") + "/" + sinkNamespace + "/streamLookupNamespace" + "/" + lookupNamespace.replaceAll("\\*", "-")
        val configuration = new Configuration()
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
        if (HdfsUtils.isParquetPathReady(configuration, parquetAddr)) {
          val lookupDf = session.read.parquet(parquetAddr)
          val timeThreshold = DateUtils.dt2timestamp(DateUtils.dt2dateTime(minTs).minusSeconds(timeout))

          val condition = UmsSysField.TS.toString + " >= cast (\'" + timeThreshold + "\' as TIMESTAMP)"
          //else  jsonUmsSysFields.umsSysTs + " >= cast (\'" + timeThreshold + "\' as TIMESTAMP)"
          val validDf = lookupDf.filter(condition)
          val parquetAddrTmp = parquetAddr + "_tmp"
          validDf.write.mode(SaveMode.Overwrite).parquet(parquetAddrTmp)
          HdfsUtils.deletePath(configuration, parquetAddr)
          HdfsUtils.renamePath(configuration, parquetAddrTmp, parquetAddr)
        }
    }
  }

  private def failureAndNonTimeoutProcess(sourceNamespace: String,
                                          sinkNamespace: String,
                                          uidArray: Array[String],
                                          sourceDf: DataFrame,
                                          config: WormholeConfig)

  = {
    val configuration = new Configuration()
    val parquetAddr = config.stream_hdfs_address.get + "/" + "swiftsparquet" + "/" + config.spark_config.stream_id + "/" + sourceNamespace + "/" + sinkNamespace + "/mainNamespace"
    if (uidArray.nonEmpty) {
      val uids = uidArray.map(t => "\'" + t + "\'").mkString(",")
      val condition: String = UmsSysField.UID.toString + " in (" + uids + ")" //else jsonUmsSysFields.umsSysUid.get + " in (" + uids + ")"
      val failureAndNonTimeoutSourceDf = sourceDf.where(condition).coalesce(config.rdd_partition_number).cache
      val parquetAddrTmp = parquetAddr + "_tmp"
      configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
      failureAndNonTimeoutSourceDf.write.mode(SaveMode.Overwrite).parquet(parquetAddrTmp)
      HdfsUtils.deletePath(configuration, parquetAddr)
      HdfsUtils.renamePath(configuration, parquetAddrTmp, parquetAddr)
      failureAndNonTimeoutSourceDf.unpersist
    } else HdfsUtils.deletePath(configuration, parquetAddr)
  }

  def doOtherData(otherDataArray: Array[String], config: WormholeConfig, processedSourceNamespace: Set[String], batchId: String, topics:String): Unit = {
    if (otherDataArray.nonEmpty) {
      otherDataArray.foreach(
        row => {
          val ums = UmsCommonUtils.json2Ums(row)
          val umsTsIndex = ums.schema.fields.get.zipWithIndex.filter(_._1.name == UmsSysField.TS.toString).head._2
          val namespace = ums.schema.namespace
          val umsts = ums.payload_get.head.tuple(umsTsIndex)
          ums.protocol.`type` match {
            case UmsProtocolType.DATA_BATCH_TERMINATION =>
              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority1,
                WormholeUms.feedbackDataBatchTermination(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
            //              logAlert("Receive DATA_BATCH_TERMINATION, kill the application")
            //              val pb = new ProcessBuilder("yarn","application", "-kill", SparkUtils.getAppId())
            //              pb.start()
            case UmsProtocolType.DATA_INCREMENT_TERMINATION =>
              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority1,
                WormholeUms.feedbackDataIncrementTermination(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
            case UmsProtocolType.DATA_INCREMENT_HEARTBEAT =>

              val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(namespace)
              if (matchSourceNamespace != null) {
                val sinkNamespaceMap = ConfMemoryStorage.getFlowConfigMap(matchSourceNamespace)
                sinkNamespaceMap.foreach {
                  case (sinkNamespace, _) =>
                    if (!processedSourceNamespace(namespace)) {
                      val currentTs = System.currentTimeMillis()
                      WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
                        UmsProtocolUtils.feedbackFlowStats(namespace, UmsProtocolType.DATA_INCREMENT_DATA.toString, DateUtils.currentDateTime, config.spark_config.stream_id, batchId, sinkNamespace,topics,
                          0, DateUtils.dt2date(umsts).getTime, currentTs, currentTs, currentTs, currentTs, currentTs, currentTs.toString), None, config.kafka_output.brokers)
                    }
                }
              }
              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority2,
                WormholeUms.feedbackDataIncrementHeartbeat(namespace, umsts, config.spark_config.stream_id), None, config.kafka_output.brokers)
            case _ => logWarning(ums.protocol.`type`.toString + " is not supported")
          }
        }
      )
    }
  }
}



