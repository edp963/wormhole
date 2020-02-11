package edp.wormhole.sinks.clickhousesink

import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.sinks.SourceMutationType.SourceMutationType
import edp.wormhole.sinks.dbsink.SqlProcessor
import edp.wormhole.ums.UmsDataSystem.UmsDataSystem
import edp.wormhole.ums.UmsFieldType.{UmsFieldType, _}
import edp.wormhole.ums.{UmsFieldType, UmsNamespace, UmsOpType, UmsSysField}
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.JsonUtils._
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger
import org.joda.time.{DateTime, Seconds}

import scala.collection.mutable

class Data2ClickhouseSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleListOrig: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {

    logger.info(s"process data2chsink, size is ${tupleListOrig.size}")
    logger.info("data2chsink sink config: " + sinkProcessConfig)

    val tupleList = setDefaultValue(schemaMap, tupleListOrig)

    val dt1: DateTime = dt2dateTime(currentyyyyMMddHHmmss)

    val sinkSpecificConfig =
      if (sinkProcessConfig.specialConfig.isDefined)
        json2caseClass[ClickhouseConfig](sinkProcessConfig.specialConfig.get)
      else ClickhouseConfig()

    val systemFieldsRename: String = sinkSpecificConfig.system_fields_rename
    val systemRenameMutableMap = mutable.HashMap.empty[String, String]
    if (systemFieldsRename.nonEmpty) {
      systemFieldsRename.split(",").foreach(t => {
        val keyValue = t.split(":").map(_.trim)
        systemRenameMutableMap(keyValue(0)) = keyValue(1)
      })
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.ACTIVE.toString)) {
      systemRenameMutableMap(UmsSysField.ACTIVE.toString) = UmsSysField.ACTIVE.toString
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.ID.toString)) {
      systemRenameMutableMap(UmsSysField.ID.toString) = UmsSysField.ID.toString
    }
    if (systemRenameMutableMap.isEmpty || !systemRenameMutableMap.contains(UmsSysField.TS.toString)) {
      systemRenameMutableMap(UmsSysField.TS.toString) = UmsSysField.TS.toString
    }

    val systemRenameMap = systemRenameMutableMap.toMap
    val batchSize = sinkSpecificConfig.`ch.sql_batch_size.get`
    val errorListBuffer = new mutable.HashMap[String, Long]
    val renameSchemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)] = getRenameMap(schemaMap, systemRenameMap)

    val tuplesShardMap: Map[_ <: ConnectionConfig, Seq[Seq[String]]] = if(sinkSpecificConfig.clickhouse_engine == ClickhouseEngine.MERGETREE.toString) {
      logger.info("Clickhouse Engine is mergetree")
      ClickhouseUtils.getTuplesShardMap(tupleList, connectionConfig, sinkProcessConfig.tableKeyList, renameSchemaMap)
    } else {
      logger.info("Clickhouse Engine is distributed")
      val map = mutable.HashMap.empty[ConnectionConfig, Seq[Seq[String]]]
      map.put(connectionConfig, tupleList)
      map.toMap
    }

    tuplesShardMap.foreach(tuplesShard => {
      val curConnectionConfig = tuplesShard._1
      val curTupleList = tuplesShard._2
      logger.info(s"insert begin, curTupleList ${curTupleList.size}, batchSize $batchSize")
      curTupleList.grouped(batchSize).map(tuples => {
        insertData(tuples, sinkProcessConfig, sinkSpecificConfig, sinkNamespace, curConnectionConfig, schemaMap, systemRenameMap, renameSchemaMap, batchSize)
      }).foreach(errorSeq => {
        if (errorListBuffer.contains(errorSeq._2.toString))
          errorListBuffer(errorSeq._2.toString) += errorSeq._1
        else
          errorListBuffer(errorSeq._2.toString) = errorSeq._1
      })
      logger.info("insert end")
    })

    if (errorListBuffer.nonEmpty && errorListBuffer.values.headOption.isDefined && errorListBuffer.values.headOption.get > 0) throw new Exception(errorListBuffer.keySet.headOption.get + ",some data error ,data records=" + errorListBuffer.values.headOption.get)
    val dt2: DateTime = dt2dateTime(currentyyyyMMddHHmmss)
    println("db duration:   " + dt2 + " - " + dt1 + " = " + (Seconds.secondsBetween(dt1, dt2).getSeconds % 60 + " seconds."))

  }

  def insertData(tupleList: Seq[Seq[String]],
                 sinkProcessConfig: SinkProcessConfig,
                 sinkSpecificConfig: ClickhouseConfig,
                 sinkNamespace: String,
                 connectionConfig: ConnectionConfig,
                 schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                 systemRenameMap: collection.immutable.Map[String, String],
                 renameSchemaMap:  collection.Map[String, (Int, UmsFieldType, Boolean)],
                 batchSize: Int): (Long, SourceMutationType) = {
    val namespace = UmsNamespace(sinkNamespace)
    val dataSys: UmsDataSystem = namespace.dataSys
    val tableName: String = namespace.table
    val allFieldNames: Seq[String] = schemaMap.keySet.toList
    val tableKeyNames: Seq[String] = sinkProcessConfig.tableKeyList
    val sysIdName = systemRenameMap(UmsSysField.ID.toString)
    val sourceMutationType = SourceMutationType.sourceMutationType(sinkSpecificConfig.`mutation_type.get`)
    sourceMutationType match {
      case SourceMutationType.INSERT_ONLY =>
        logger.info("INSERT_ONLY: " + sourceMutationType.toString)
        val insertSql = SqlProcessor.getInsertSql(sourceMutationType, dataSys, tableName, systemRenameMap, allFieldNames)
        logger.info(s"insertSql $insertSql")
        val errorList = SqlProcessor.executeProcess(tupleList, insertSql, batchSize, UmsOpType.INSERT, sourceMutationType, connectionConfig, allFieldNames,
          renameSchemaMap, systemRenameMap, tableKeyNames, sysIdName)
        (errorList.length, SourceMutationType.INSERT_ONLY)
      case _ =>
        logger.error("mutation_type " + sourceMutationType.toString + " not support yet")
        (tupleList.length, sourceMutationType)
    }
  }

  def getRenameMap(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                   systemRenameMap: collection.immutable.Map[String, String]): collection.Map[String, (Int, UmsFieldType, Boolean)]= {
    schemaMap.map { case (name, (index, umsType, nullable)) =>
      name match {
        case "ums_id_" => (systemRenameMap(name), (index, umsType, nullable))
        case "ums_ts_" => (systemRenameMap(name), (index, umsType, nullable))
        case _ => (name, (index, umsType, nullable))
      }
    }.toMap
  }

  def setDefaultValue(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                      tupleList: Seq[Seq[String]]): Seq[Seq[String]] = {
    val schemaTypeMap: Map[Int, UmsFieldType] = schemaMap.map{ case (name, (index, umsType, nullable)) =>
      (index, umsType)
    }.toMap
    tupleList.map(tuple => {
      var index = -1
      tuple.map(value => {
        index = index + 1
        val schemaType = schemaTypeMap.getOrElse(index, UmsFieldType.STRING)
        val processedValue = setDefaultValueByType(value, schemaType)
        //logger.info(s"setDefaultValue---value: $value, processedValue: $processedValue, schemaType: $schemaType")
        processedValue
      })
    })
  }

  def setDefaultValueByType(value: String,
                      valueType: UmsFieldType): String = {
    if((value == null) || (value.isEmpty && valueType != UmsFieldType.STRING)) valueType match {
      case UmsFieldType.STRING => ""
      case UmsFieldType.INT => "0"
      case UmsFieldType.LONG => "0"
      case UmsFieldType.FLOAT => "0"
      case UmsFieldType.DOUBLE => "0"
      //case UmsFieldType.BINARY => "0"
      case UmsFieldType.DECIMAL => "0"
      case UmsFieldType.BOOLEAN => "0"
      case UmsFieldType.DATE => "0000-00-00"
      case UmsFieldType.DATETIME => "0000-00-00 00:00:00"
      case _ => throw new UnsupportedOperationException(s"Unknown Type: $valueType")
    } else {
      value
    }
  }
}
