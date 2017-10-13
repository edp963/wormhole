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


package edp.wormhole.batchflow

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.common.SparkSchemaUtils._
import edp.wormhole.common.WormholeUtils.json2Ums
import edp.wormhole.common._
import edp.wormhole.common.hadoop.HdfsUtils
import edp.wormhole.common.util.DateUtils
import edp.wormhole.common.util.DateUtils._
import edp.wormhole.core.{InputDataRequirement, UdfDirective, WormholeConfig}
import edp.wormhole.kafka.WormholeKafkaProducer
import edp.wormhole.memorystorage.ConfMemoryStorage
import edp.wormhole.sinks.{SinkProcessConfig, SourceMutationType}

//import scala.collection.mutable
//import edp.wormhole.sinks.hbasesink.HbaseConnection
import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.swifts.parse.SwiftsProcessConfig
import edp.wormhole.swifts.transform.SwiftsTransform
import edp.wormhole.swifts.validity.{ValidityAgainstAction, ValidityCheckRule, ValidityConfig}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange, WormholeDirectKafkaInputDStream}
//import org.joda.time._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.postfixOps
//import scala.util.control.NonFatal
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.Future

object BatchflowMainProcess extends EdpLogging {

  def process(stream: WormholeDirectKafkaInputDStream[String, String], config: WormholeConfig, session: SparkSession): Unit = {
    stream.foreachRDD((streamRdd: RDD[ConsumerRecord[String, String]]) => {
      val offsetInfo: ArrayBuffer[OffsetRange] = getOffsets(streamRdd)
      try {
        logInfo("start foreachRDD")
        if (SparkUtils.isLocalMode(config.spark_config.master)) logWarning("rdd count ===> " + streamRdd.count())
        val statsId = UUID.randomUUID().toString
        val rddTs = System.currentTimeMillis
        // val session = SparkSession.builder().config(streamRdd.sparkContext.getConf).getOrCreate()

        logInfo("star" +
          "t doDirectiveTopic")
        val directiveTs = System.currentTimeMillis
        BatchflowDirective.doDirectiveTopic(config, stream)

        logInfo("start Repartition")
        val mainDataTs = System.currentTimeMillis
        //val dt1 =  dt2dateTime(currentyyyyMMddHHmmss)
        val dataRepartitionRdd: RDD[(String, String)] = if (config.rdd_partition_number != -1) streamRdd.map(row => (row.key, row.value)).repartition(config.rdd_partition_number) else streamRdd.map(row => (row.key, row.value))
        UdfDirective.registerUdfProcess(config.kafka_output.feedback_topic_name,config.kafka_output.brokers, session)
        //        dataRepartitionRdd.cache()
        //        dataRepartitionRdd.count()
        //        val dt2: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("repartition dataRepartitionRdd duration:   " + dt2 + " - "+ dt1 +" = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " seconds."))
        logInfo("start create classifyRdd")
        val classifyRdd: RDD[(ListBuffer[((String, String), Ums)], ListBuffer[((String, String), Ums)], ListBuffer[String])] = getClassifyRdd(dataRepartitionRdd).cache()
        //        classifyRdd.count
        //        val dt3: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("get classifyRdd duration:   " + dt3 + " - "+ dt2 +" = " + (Seconds.secondsBetween(dt2, dt3).getSeconds() % 60 + " seconds."))
        logInfo("start doStreamLookupData")
        //        val streamMergeTs = System.currentTimeMillis
        doStreamLookupData(session, classifyRdd, config)
        //               val dt4: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //               println("get doStreamLookupData duration:   " + dt4 + " - "+ dt3 +" = " + (Seconds.secondsBetween(dt3, dt4).getSeconds() % 60 + " seconds."))
        logInfo("start doMainData")

        //   val dt5: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        val processedSourceNamespace = doMainData(session, classifyRdd, config, statsId, rddTs, directiveTs, mainDataTs)
        //        val dt6: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        //        println("get doMainData duration:   " + dt6 + " - "+ dt5 +" = " + (Seconds.secondsBetween(dt5, dt6).getSeconds() % 60 + "seconds"))
        //
        logInfo("start doOtherData")
        val nonDataArray = classifyRdd.flatMap(_._3).collect()
        doOtherData(nonDataArray, config, processedSourceNamespace, statsId)

        logInfo("start storeTopicPartition")
        WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)

        classifyRdd.unpersist()
      }
      catch {
        case e: Throwable =>
          logAlert("batch error", e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackStreamBatchError(config.spark_config.stream_id, currentDateTime, UmsFeedbackStatus.FAIL, e.getMessage), None, config.kafka_output.brokers)
          WormholeUtils.sendTopicPartitionOffset(offsetInfo, config.kafka_output.feedback_topic_name, config)
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetInfo.toArray)
    }
    )
  }

  private def getClassifyRdd(dataRepartitionRdd: RDD[(String, String)]): RDD[(ListBuffer[((String, String), Ums)], ListBuffer[((String, String), Ums)], ListBuffer[String])] = {
    val streamLookupNamespaceSet = ConfMemoryStorage.getAllLookupNamespaceSet
    val mainNamespaceSet = ConfMemoryStorage.getAllMainNamespaceSet
    dataRepartitionRdd.mapPartitions(partition => {

      val mainDataList = ListBuffer.empty[((String, String), Ums)]
      val lookupDataList = ListBuffer.empty[((String, String), Ums)]
      val otherList = ListBuffer.empty[String]
      partition.foreach(row => {
        try {
          val (protocolType, namespace) = WormholeUtils.getTypeNamespaceFromKafkaKey(row._1)
          if (protocolType == UmsProtocolType.DATA_INCREMENT_DATA.toString || protocolType == UmsProtocolType.DATA_BATCH_DATA.toString || protocolType == UmsProtocolType.DATA_INITIAL_DATA.toString) {
            if (ConfMemoryStorage.existSourceNamespace(mainNamespaceSet, namespace)) mainDataList += (((protocolType, namespace), WormholeUtils.json2Ums(row._2)))
            else if (ConfMemoryStorage.existLookupNamespace(streamLookupNamespaceSet, namespace)) lookupDataList += (((protocolType, namespace), WormholeUtils.json2Ums(row._2)))
          } else if (checkOtherData(protocolType)) otherList += row._2
          else logDebug("namespace:" + namespace + ", do not config")
        } catch {
          case e1: Throwable => logAlert("do classifyRdd,one data has error,row:" + row, e1)
        }
      })
      List((mainDataList, lookupDataList, otherList)).toIterator
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


  private def getMinMaxTsAndCount(protocolType: UmsProtocolType, sourceNamespace: String, umsRdd: RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])]): (String, String, Int) = {
    val minMaxCountArray: Array[(String, String, Int)] = umsRdd.mapPartitions(partition => {
      var minTs = ""
      var maxTs = ""
      var count = 0
      partition.foreach(umsRow => {
        if (umsRow._1 == protocolType && umsRow._2 == sourceNamespace) {
          count += umsRow._4.length
          val umsTsIndex = umsRow._3.map(_.name).indexOf(UmsSysField.TS.toString)
          umsRow._4.foreach(tuple => {
            val dataTs = tuple(umsTsIndex)
            if (minTs.isEmpty || !SinkCommonUtils.firstTimeAfterSecond(dataTs, minTs)) minTs = dataTs
            if (SinkCommonUtils.firstTimeAfterSecond(dataTs, maxTs)) maxTs = dataTs
          })
        }
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


  private def doStreamLookupData(session: SparkSession, allDataRdd: RDD[(ListBuffer[((String, String), Ums)], ListBuffer[((String, String), Ums)], ListBuffer[String])], config: WormholeConfig) = {
    try { // join in streaming, file name： sourcenamespace 4 fields _ sinknamespace_lookup namespace 4 fields
      val umsRdd: RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])] = formatRdd(allDataRdd, "lookup")
      val schemaArray = getDistinctSchema(umsRdd)
      schemaArray.foreach(schema => {
        val protocolType: UmsProtocolType = schema._1
        val namespace = schema._2
        val matchLookupNamespace = ConfMemoryStorage.getMatchLookupNamespaceRule(schema._2)
        val lookupDf = createSourceDf(session, namespace, schema._3, protocolType, umsRdd)

        val filterDf = lookupDf.filter("ums_op_ != 'b'")
        ConfMemoryStorage.getSourceAndSinkByStreamLookupNamespace(matchLookupNamespace).foreach {
          case (sourceNs, sinkNs) =>
            val path = config.stream_hdfs_address.get +"/" + "swiftsparquet"+ "/" + config.spark_config.stream_id + "/" + sourceNs.replaceAll("\\*", "-") + "/" + sinkNs + "/streamLookupNamespace" + "/" + matchLookupNamespace.replaceAll("\\*", "-")
            filterDf.write.mode(SaveMode.Append).parquet(path) //if not exists will have "WARN: delete very recently?" it is ok.
        }
      })
    } catch {
      case e: Throwable => logAlert("doStreamLookupData", e)
    }
  }

  private def doMainData(session: SparkSession,
                         mainDataRdd: RDD[(ListBuffer[((String, String), Ums)], ListBuffer[((String, String), Ums)], ListBuffer[String])],
                         config: WormholeConfig,
                         statsId: String,
                         rddTs: Long,
                         directiveTs: Long,
                         mainDataTs: Long): Set[String] = {
    val processedsourceNamespace = mutable.HashSet.empty[String]
    // val dt1: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    val umsRdd: RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])] = formatRdd(mainDataRdd, "main").cache
    // umsRdd.count()
    // val dt2: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    // println("In doMainData, get umsRdd duration:   " + dt2 + " - "+ dt1 +" = " + (Seconds.secondsBetween(dt1, dt2).getSeconds() % 60 + " 秒."))

    val schemaArray: Array[(UmsProtocolType, String, Seq[UmsField])] = getDistinctSchema(umsRdd)
    // val dt3: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    // println("In doMainData, get schemaArray duration:   " + dt3 + " - "+ dt2 +" = " + (Seconds.secondsBetween(dt2, dt3).getSeconds() % 60 + " 秒."))
    schemaArray.foreach(schema => {
        val uuid = UUID.randomUUID().toString
        val protocolType: UmsProtocolType = schema._1
        val sourceNamespace: String = schema._2
        logInfo(uuid + ",schema loop,sourceNamespace:" + sourceNamespace)
        val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)
        val sourceDf = createSourceDf(session, sourceNamespace, schema._3, protocolType, umsRdd)
        //.filter("ums_op_ != 'b'")
        //.cache //.filter(UmsSysField.OP.toString +" <> "+ UmsOpType.BEFORE_UPDATE.toString).cache()
        //println("sourceDf.count:" + sourceDf.count)
        //  sourceDf.count
        //  val dt4: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
        // println("In doMainData, createSourceDf duration:   " + dt4 + " - "+ dt3 +" = " + (Seconds.secondsBetween(dt3, dt4).getSeconds() % 60 + " seconds."))
        val (minTs, maxTs, count) = getMinMaxTsAndCount(protocolType, sourceNamespace, umsRdd)
        logInfo(uuid + "sourceNamespace:" + sourceNamespace + ",minTs:" + minTs + ",maxTs:" + maxTs + ",sourceDf.count:" + count)
        if (count > 0) {
          val flowConfigMap = ConfMemoryStorage.getFlowConfigMap(matchSourceNamespace)
          flowConfigMap.foreach(flow => {
            val isProcessed = protocolType match {
              case UmsProtocolType.DATA_INCREMENT_DATA =>
                flow._2._6(InputDataRequirement.INCREMENT.toString)
              case UmsProtocolType.DATA_INITIAL_DATA =>
                flow._2._6(InputDataRequirement.INITIAL.toString)
              case UmsProtocolType.DATA_BATCH_DATA =>
                flow._2._6(InputDataRequirement.BATCH.toString)
            }
            if (isProcessed) {
              val sinkNamespace = flow._1
              logInfo(uuid + ",do flow,matchSourceNamespace:" + matchSourceNamespace + ",sinkNamepace:" + sinkNamespace)
              val swiftsTs = System.currentTimeMillis
              ConfMemoryStorage.setEventTs(matchSourceNamespace, sinkNamespace, minTs)
              val (swiftsProcessConfig: Option[SwiftsProcessConfig], sinkProcessConfig, _, _, _, _) = flow._2
              logInfo(uuid + ",start swiftsProcess")

              val afterUnionDf = unionParquetNonTimeoutDf(swiftsProcessConfig, uuid, session, sourceDf, config, sourceNamespace, sinkNamespace).cache
              println("sourceNamespace=" + sourceNamespace + ",afterUnionDf.count" + afterUnionDf.count)
              val swiftsDf = swiftsProcess(swiftsProcessConfig, uuid, session, afterUnionDf, config, sourceNamespace, sinkNamespace, minTs, maxTs, count)


              val sinkTs = System.currentTimeMillis
              if (swiftsDf != null) {
                try {
                  validityAndSinkProcess(protocolType, sourceNamespace, sinkNamespace, session, swiftsDf, afterUnionDf, swiftsProcessConfig, sinkProcessConfig, config, minTs, maxTs, uuid)
                } catch {
                  case e: Throwable =>
                    logAlert("sink,sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + ",count=" + count, e)
                    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackFlowError(sourceNamespace, config.spark_config.stream_id, currentDateTime, sinkNamespace, UmsWatermark(maxTs), UmsWatermark(minTs), count, ""), None, config.kafka_output.brokers)
                }
                swiftsDf.unpersist
              } else logWarning("sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + "there is nothing to sinkProcess")
              afterUnionDf.unpersist

              val doneTs = System.currentTimeMillis
              processedsourceNamespace.add(sourceNamespace)
              WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
                UmsProtocolUtils.feedbackFlowStats(sourceNamespace, protocolType.toString, currentDateTime, config.spark_config.stream_id, statsId, sinkNamespace,
                  count, DateUtils.dt2date(maxTs).getTime, rddTs, directiveTs, mainDataTs, swiftsTs, sinkTs, doneTs), None, config.kafka_output.brokers)
            }
          }
          )
          //sourceDf.unpersist()
        }
      })
      umsRdd.unpersist()
      processedsourceNamespace.toSet
  }

  private def unionParquetNonTimeoutDf(swiftsProcessConfig: Option[SwiftsProcessConfig],
                                       uuid: String,
                                       session: SparkSession,
                                       sourceDf: DataFrame,
                                       config: WormholeConfig,
                                       sourceNamespace: String,
                                       sinkNamespace: String
                                      ): DataFrame = {
    if (swiftsProcessConfig.nonEmpty) {
      if (swiftsProcessConfig.get.validityConfig.isDefined) {
        val parquetAddr = config.stream_hdfs_address.get +"/" + "swiftsparquet"+ "/" + config.spark_config.stream_id + "/" + sourceNamespace + "/" + sinkNamespace + "/mainNamespace"
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
    } else {
      logInfo(uuid + ",swiftsProcessConfig.empty")
      sourceDf
    }
  }


  private def swiftsProcess(swiftsProcessConfig: Option[SwiftsProcessConfig],
                            uuid: String,
                            session: SparkSession,
                            afterUnionDf: DataFrame,
                            config: WormholeConfig,
                            sourceNamespace: String,
                            sinkNamespace: String,
                            minTs: String,
                            maxTs: String,
                            count: Int): DataFrame = {
    val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)
    if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.swiftsSql.nonEmpty) {
      logInfo(uuid + ",swiftsProcessConfig.nonEmpty,and readMainParquetDf")
      try {
        SwiftsTransform.transform(session, sourceNamespace, sinkNamespace, afterUnionDf, matchSourceNamespace, config)
      } catch {
        case e: Throwable =>
          logAlert(uuid + "swifts,sourceNamespace=" + sourceNamespace + ",sinkNamespace=" + sinkNamespace + ",count=" + count, e)
          WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority3, UmsProtocolUtils.feedbackFlowError(sourceNamespace, config.spark_config.stream_id, currentDateTime, sinkNamespace, UmsWatermark(maxTs), UmsWatermark(minTs), count, ""), None, config.kafka_output.brokers)
          null
      }
    } else {
      logInfo(uuid + ",swiftsProcessConfig.empty")
      afterUnionDf
    }
  }

  //    private def readMainParquetDf(session: SparkSession, sourceNamespace: String, sinkNamespace: String, config: WormholeConfig): DataFrame = {
  //      val parquetAddr = config.stream_hdfs_address.get + "/" + config.spark_config.stream_id + "/" + sourceNamespace + "/" + sinkNamespace + "/mainNamespace"
  //      val configuration = new Configuration()
  //      if (HdfsUtils.isParquetPathReady(configuration, parquetAddr)) {
  //        session.read.parquet(parquetAddr)
  //      }
  //      else {
  //        null.asInstanceOf[DataFrame]
  //      }
  //    }

  private def createSourceDf(session: SparkSession, sourceNamespace: String, fields: Seq[UmsField], protocolType: UmsProtocolType,
                             umsRdd: RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])]) = {
    val sourceRdd: RDD[Seq[String]] = umsRdd.filter(row => {
      row._1 == protocolType && row._2 == sourceNamespace
    }).flatMap(_._4)
    val rowRdd: RDD[Row] = sourceRdd.flatMap(row => SparkUtils.umsToSparkRowWrapper(sourceNamespace, fields, row))
    createDf(session, fields, rowRdd)
  }

  private def getDistinctSchema(umsRdd: RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])]): Array[(UmsProtocolType, String, Seq[UmsField])] = {
    val schemaArray = umsRdd.mapPartitions(partition => {
      val schemaMap = mutable.HashMap.empty[(UmsProtocolType, String), (UmsProtocolType, String, Seq[UmsField])]
      partition.foreach(row => {
        if (!schemaMap.contains((row._1, row._2))) {
          schemaMap((row._1, row._2)) = (row._1, row._2, row._3)
        }
      })
      schemaMap.values.toIterator
    }).collect()

    val schemaMap = mutable.HashMap.empty[(UmsProtocolType, String), (UmsProtocolType, String, Seq[UmsField])]
    schemaArray.foreach(schema => {
      if (!schemaMap.contains((schema._1, schema._2))) {
        schemaMap((schema._1, schema._2)) = (schema._1, schema._2, schema._3)
      }
    })

    schemaMap.values.toArray
  }

  private def formatRdd(allDataRdd: RDD[(ListBuffer[((String, String), Ums)], ListBuffer[((String, String), Ums)], ListBuffer[String])], dataType: String): RDD[(UmsProtocolType, String, Seq[UmsField], ArrayBuffer[Seq[String]])] = {
    allDataRdd.mapPartitions(par => {
      val namespace2umsMap = mutable.HashMap.empty[(UmsProtocolType, String), (Seq[UmsField], ArrayBuffer[Seq[String]])] //[(protocoltype,namespace),(seq[umsfield],array[seq[string]]

      par.foreach(allList => {
        val formatList: mutable.Seq[((String, String), Ums)] = if (dataType == "main") allList._1 else allList._2
        formatList.foreach(row => {
          val ums: Ums = row._2
          if (namespace2umsMap.contains((ums.protocol.`type`, ums.schema.namespace))) {
            namespace2umsMap((ums.protocol.`type`, ums.schema.namespace))._2 ++= ums.payload_get.map(_.tuple)
          } else {
            val tuple = ArrayBuffer.empty[Seq[String]]
            tuple ++= ums.payload_get.map(_.tuple)
            namespace2umsMap += (ums.protocol.`type`, ums.schema.namespace) -> (ums.schema.fields_get, tuple)
          }
        })
      })
      namespace2umsMap.map(ele => {
        (ele._1._1, ele._1._2, ele._2._1, ele._2._2)
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
                                     swiftsDf: DataFrame,
                                     streamUnionParquetDf: DataFrame,
                                     swiftsProcessConfig: Option[SwiftsProcessConfig],
                                     sinkProcessConfig: SinkProcessConfig,
                                     config: WormholeConfig,
                                     minTs: String,
                                     maxTs: String,
                                     uuid: String) = {
    val connectionConfig = ConfMemoryStorage.getDataStoreConnectionsMap(sinkNamespace)
    val schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)] = SparkUtils.getSchemaMap(swiftsDf.schema)
    logInfo(uuid + ",schemaMap:" + schemaMap)
    val matchSourceNamespace = ConfMemoryStorage.getMatchSourceNamespaceRule(sourceNamespace)

    val dataSysType = UmsDataSystem.dataSystem(sinkNamespace.split("\\.")(0))
    val repartitionDf = if (dataSysType == UmsDataSystem.MYSQL || dataSysType == UmsDataSystem.ORACLE||dataSysType == UmsDataSystem.POSTGRESQL) {
      val specialConfigJson: JSONObject = if (sinkProcessConfig.specialConfig.isDefined) JSON.parseObject(sinkProcessConfig.specialConfig.get) else new JSONObject()

      if (specialConfigJson.containsKey("db.mutation.type") && specialConfigJson.getString("db.mutation.type").nonEmpty) {
        val mutationType = specialConfigJson.getString("db.mutation.type").trim
        if (SourceMutationType.INSERT_ONLY.toString != mutationType) {
          if (sinkProcessConfig.tableKeys.nonEmpty) {
            logInfo("sinkProcessConfig.tableKeys.nonEmpty")
            val columns = sinkProcessConfig.tableKeys.get.split(",").map(name => new Column(name))
            swiftsDf.repartition(config.rdd_partition_number, columns: _*)
          } else {
            logInfo("sinkProcessConfig.tableKeys.isEmpty")
            swiftsDf
          }
        } else {
          logInfo("SourceMutationType.INSERT_ONLY.toString == mutationType")
          swiftsDf
        }
      } else {
        logInfo("specialConfigJson.containsKey(mutation.type)")
        swiftsDf
      }
    } else {
      logInfo("dataSysType is not db")
      swiftsDf
    }
    //          case UmsDataSystem.HBASE => HbaseConnection.initHbaseConfig(sinkNamespace, sinkProcessConfig, connectionConfig)
    //          case UmsDataSystem.KAFKA => WormholeKafkaProducer.init(connectionConfig.connectionUrl, connectionConfig.parameters)
    //          case _ =>
    //        }


    import session.implicits._
    val nonTimeoutUids = repartitionDf.mapPartitions((partition: Iterator[Row]) => {
      if (partition.nonEmpty) {
        logInfo(uuid + ",partition.nonEmpty")

        val (projectSchemaMap, sendList, saveList) = doValidityAndGetData(swiftsProcessConfig, partition, schemaMap, minTs, sourceNamespace, sinkNamespace)

        //  sendList.foreach(data=>logInfo("before merge:"+data))
        logInfo(uuid + ",@sendList size: " + sendList.size + " saveList size: " + saveList.size)
        val mergeSendList: Seq[Seq[String]] = mergeTuple(sendList, projectSchemaMap, sinkProcessConfig.tableKeyList)
        logInfo(uuid + ",@mergeSendList size: " + mergeSendList.size)
        //        mergeSendList.foreach(data=>logInfo("after merge:"+data))

        val (sinkObject, sinkMethod) = ConfMemoryStorage.getSinkTransformReflect(sinkProcessConfig.classFullname)

        //        import scala.concurrent.duration._
        //        val result: Future[Any] = RetryUtils.retry(sinkProcessConfig.retryTimes, Some(sinkProcessConfig.retrySeconds.seconds.fromNow)) {
        //          try {
        //            logInfo(uuid + ",sink process write count:" + mergeSendList.size+",sinkProcessConfig.retrySeconds:"+sinkProcessConfig.retrySeconds+",sinkProcessConfig.retrySeconds.seconds.fromNow:"+sinkProcessConfig.retrySeconds.seconds.fromNow)
        sinkMethod.invoke(sinkObject, protocolType, sourceNamespace, sinkNamespace, sinkProcessConfig, projectSchemaMap, mergeSendList, connectionConfig)
        //          } catch {
        //            case e: Throwable =>
        //              logError(uuid + ",retry ERROR: ", e)
        //              throw e
        //          }
        //        }
        //        result onSuccess {
        //          case _ => logInfo(uuid + ",sink success!")
        //        }
        //        result onFailure {
        //          case _ => throw new Exception(uuid + ",retry " + sinkProcessConfig.retrySeconds + " times failure")
        //        }

        saveList.toIterator
      } else {
        logInfo(uuid + ",partition data(payload) size is 0,do not process sink")
        List.empty[String].toIterator
      }
    }).collect()
    // val dt3: DateTime =  dt2dateTime(currentyyyyMMddHHmmss)
    //  println("In validityAndSinkProcess, writetoSInk duration:   " + dt3 + " - "+ dt2 +" = " + (Seconds.secondsBetween(dt2, dt3).getSeconds() % 60 + " seconds."))

    if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.validityConfig.nonEmpty) {
      failureAndNonTimeoutProcess(sourceNamespace, sinkNamespace, nonTimeoutUids, streamUnionParquetDf, config)
    }
    if (ConfMemoryStorage.existEventTs(matchSourceNamespace, sinkNamespace)) {
      val currentMinTs = ConfMemoryStorage.getEventTs(matchSourceNamespace, sinkNamespace)
      val minTime = if (SinkCommonUtils.firstTimeAfterSecond(minTs, currentMinTs)) currentMinTs else minTs
      if (ConfMemoryStorage.existStreamLookup(matchSourceNamespace, sinkNamespace))
        streamJoinTimeoutProcess(matchSourceNamespace, sinkNamespace, config, minTime, session)
    }
  }

  private def getProjectionSchemaMap(swiftsProcessConfig: Option[SwiftsProcessConfig],
                                     filterUmsUidSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                                     sinkNamespace: String): collection.Map[String, (Int, UmsFieldType, Boolean)]

  = {

    if (swiftsProcessConfig.nonEmpty && swiftsProcessConfig.get.projection != null && swiftsProcessConfig.get.projection != "") {
      val newSchemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
      var index = 0
      swiftsProcessConfig.get.projection.split(",").foreach(column => {
       // if (column != UmsSysField.UID.toString || UmsNamespace(sinkNamespace).dataSys == UmsDataSystem.KAFKA) {
          newSchemaMap(column) = (index, filterUmsUidSchemaMap(column)._2, filterUmsUidSchemaMap(column)._3)
          index += 1
       // }
      })
      newSchemaMap
    } else filterUmsUidSchemaMap
  }

  private def checkLackColumn(swiftsProcessConfig: Option[SwiftsProcessConfig],
                              originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                              sourceNamespace: String,
                              sinkNamespace: String): Boolean

  = {
    var lackColumn = false
    if (swiftsProcessConfig.get.projection.nonEmpty) {
      val checkColumns = swiftsProcessConfig.get.projection.split(",")
      checkColumns.foreach(column => {
        if (!originalSchemaMap.contains(column)) {
          lackColumn = true
          logWarning(sourceNamespace + ":" + sinkNamespace + ",lack column:" + column)
        }
      })
    }
    lackColumn
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
                                   dataSeq: Iterator[Row],
                                   originalSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                                   minTs: String,
                                   sourceNamespace: String,
                                   sinkNamespace: String): (collection.Map[String, (Int, UmsFieldType, Boolean)], mutable.ListBuffer[Seq[String]], mutable.ListBuffer[String])

  = {
    val sendList = ListBuffer.empty[Seq[String]]
    val saveList = ListBuffer.empty[String]
    var projectionSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)] = originalSchemaMap
    val filterUmsUidSchemaMap = originalSchemaMap.filterKeys(_ != UmsSysField.UID.toString)
    if (swiftsProcessConfig.nonEmpty) {
      //has swifts process
      val lackColumn = checkLackColumn(swiftsProcessConfig, originalSchemaMap, sourceNamespace, sinkNamespace)
      if (!lackColumn) {
        projectionSchemaMap = getProjectionSchemaMap(swiftsProcessConfig, filterUmsUidSchemaMap, sinkNamespace)
        //has swifts process and not lack column
        if (swiftsProcessConfig.get.validityConfig.nonEmpty) {
          //has swifts process and not lack column and need validity
          val validityConfig: ValidityConfig = swiftsProcessConfig.get.validityConfig.get
          dataSeq.foreach(row => {
            val originalDataArray = SparkUtils.getRowData(row, originalSchemaMap)
            val ifValidity = checkValidity(validityConfig, originalDataArray, originalSchemaMap)
            if (ifValidity) sendList += SparkUtils.getRowData(row, projectionSchemaMap)
            else {
              val reduceTime = yyyyMMddHHmmss(dt2dateTime(minTs).minusSeconds(validityConfig.ruleParams.toInt))
              val dataUmsts = yyyyMMddHHmmss(dt2dateTime(originalDataArray(originalSchemaMap(UmsSysField.TS.toString)._1)))
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
                    sendList += SparkUtils.getRowData(row, projectionSchemaMap)
                  case _ => throw new Exception("join failed Df, " + validityConfig.againstAction + " is not supported")
                }
              } else {
                //not timeout
                saveList += uid
              }
            }
          })
        } else sendList ++= dataSeq.map(row => SparkUtils.getRowData(row, projectionSchemaMap)) //has swifts process and not lack column and not need validity
      } else {
        //has swifts process and lack column
        saveList ++= dataSeq.map(row => {
          val originalDataArray = SparkUtils.getRowData(row, originalSchemaMap)
          val uid = originalDataArray(originalSchemaMap(UmsSysField.UID.toString)._1)
          uid
        })
      }
    } else sendList ++= dataSeq.map(row => SparkUtils.getRowData(row, filterUmsUidSchemaMap)) //not swifts process
    logInfo(sourceNamespace + ":" + sinkNamespace + ",sendList.size=" + sendList.size + ",saveList.size=" + saveList.size)
    (filterUmsUidSchemaMap, sendList, saveList)
  }

  //  private def getRowData(row: Row, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): ArrayBuffer[String] = {
  //    val dataArray = ArrayBuffer.fill(schemaMap.size) {
  //      ""
  //    }
  //    schemaMap.foreach(column => {
  //      val nameToIndex = row.asInstanceOf[GenericRowWithSchema].schema.fields.map(_.name.toLowerCase).zipWithIndex.toMap
  //      val data = row.get(nameToIndex(column._1))
  //      if (column._2._2 == UmsFieldType.BINARY) {
  //        dataArray(column._2._1) = if (null != data) {
  //          if (data != null) new String(data.asInstanceOf[Array[Byte]]) else null.asInstanceOf[String]
  //        } else null.asInstanceOf[String]
  //      } else dataArray(column._2._1) = if (data != null) data.toString else null.asInstanceOf[String]
  //    })
  //    dataArray
  //  }

  //  private def getSchemaMap(schema: StructType): Map[String, (Int, UmsFieldType, Boolean)] = {
  //    var index = -1
  //    val schemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
  //    schema.fields.foreach(field => {
  //      if (!schemaMap.contains(field.name.toLowerCase)) {
  //        index += 1
  //        schemaMap(field.name.toLowerCase) = (index, SparkSchemaUtils.spark2umsType(field.dataType), field.nullable)
  //      }
  //    })
  //    schemaMap.toMap
  //  }

  private def streamJoinTimeoutProcess(matchSourceNamespace: String,
                                       sinkNamespace: String,
                                       config: WormholeConfig,
                                       minTs: String,
                                       session: SparkSession)

  = {
    ConfMemoryStorage.getStreamLookupNamespaceAndTimeout(matchSourceNamespace, sinkNamespace).foreach {
      case (lookupNamespace, timeout) =>
        val parquetAddr = config.stream_hdfs_address.get +"/" + "swiftsparquet"+ "/" + config.spark_config.stream_id + "/" + matchSourceNamespace.replaceAll("\\*", "-") + "/" + sinkNamespace + "/streamLookupNamespace" + "/" + lookupNamespace.replaceAll("\\*", "-")
        val configuration = new Configuration()
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
        if (HdfsUtils.isParquetPathReady(configuration, parquetAddr)) {
          val lookupDf = session.read.parquet(parquetAddr)
          val timeThreshold = dt2timestamp(dt2dateTime(minTs).minusSeconds(timeout))
          val validDf = lookupDf.filter("ums_ts_ >= " + "cast (\'" + timeThreshold + "\' as TIMESTAMP)")
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
    val parquetAddr = config.stream_hdfs_address.get +"/" + "swiftsparquet"+ "/" + config.spark_config.stream_id + "/" + sourceNamespace + "/" + sinkNamespace + "/mainNamespace"
    if (uidArray.nonEmpty) {
      val uids = uidArray.map(t => "\'" + t + "\'").mkString(",")
      val condition: String = UmsSysField.UID.toString + " in (" + uids + ")"
      val failureAndNonTimeoutSourceDf = sourceDf.where(condition).coalesce(config.rdd_partition_number).cache
      val parquetAddrTmp = parquetAddr + "_tmp"
      configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
      failureAndNonTimeoutSourceDf.write.mode(SaveMode.Overwrite).parquet(parquetAddrTmp)
      HdfsUtils.deletePath(configuration, parquetAddr)
      HdfsUtils.renamePath(configuration, parquetAddrTmp, parquetAddr)
      failureAndNonTimeoutSourceDf.unpersist
    } else HdfsUtils.deletePath(configuration, parquetAddr)
  }

  def doOtherData(otherDataArray: Array[String], config: WormholeConfig, processedsourceNamespace: Set[String], statsId: String): Unit = {
    if (otherDataArray.nonEmpty) {
      otherDataArray.foreach(
        row => {
          val ums = json2Ums(row)
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
                sinkNamespaceMap.foreach { case (sinkNamespace, _) =>
                  if (!processedsourceNamespace(namespace)) {
                    val currentTs = System.currentTimeMillis()
                    WormholeKafkaProducer.sendMessage(config.kafka_output.feedback_topic_name, FeedbackPriority.FeedbackPriority4,
                      UmsProtocolUtils.feedbackFlowStats(namespace, UmsProtocolType.DATA_INCREMENT_DATA.toString, currentDateTime, config.spark_config.stream_id, statsId, sinkNamespace,
                        0, DateUtils.dt2date(umsts).getTime, currentTs, currentTs, currentTs, currentTs, currentTs, currentTs), None, config.kafka_output.brokers)
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



