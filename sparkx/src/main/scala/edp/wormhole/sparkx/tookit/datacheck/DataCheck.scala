///*-
// * <<
// * wormhole
// * ==
// * Copyright (C) 2016 - 2017 EDP
// * ==
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * >>
// */
//
//
//package edp.wormhole.toolkit.datacheck
//
//import java.io.File
//import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
//import java.util.Properties
//
//import edp.wormhole.common.{WhHdfsLogPathBase, WhHdfsLogPathUtils, WormholeUtils}
//import edp.wormhole.common.util.{DateUtils, FileUtils, JsonUtils}
//import edp.wormhole.spark.log.EdpLogging
//import edp.wormhole.ums.{UmsDataSystem, UmsNamespace}
//import org.apache.spark.rdd.JdbcRDD
//import org.apache.spark.sql._
//import org.apache.spark.sql.types._
//
//import scala.io.Source
//import scala.language.postfixOps
//import org.apache.spark.sql.execution.datasources.hbase._
//
//object DataCheck extends App with EdpLogging {
//
//  private def getDfFromDb(dataCheckConfig: DataCheckConfig, tableName: String, jdbcType: String, jdbcUrl: String, schema: StructType, sourceOrSinkType: String): DataFrame = {
//    val resultSchema = sourceOrSinkType match {
//      case SOURCEDATA => schema
//      case _ => schema.add(StructField("ums_active_", IntegerType, nullable = true))
//
//    }
//    val endTime = dataCheckConfig.`compare.end_ts`
//    val startTime = dataCheckConfig.`compare.start_ts`
//    val endTs: Timestamp = DateUtils.dt2timestamp(endTime)
//    val startTs = DateUtils.dt2timestamp(startTime)
//    val update_col = processTimeCol(dataCheckConfig.`table.update_ts_col`, jdbcType)
//    val create_col = processTimeCol(dataCheckConfig.`table.create_ts_col`, jdbcType)
//    val selectedColumns = dataCheckConfig.`table.columns` //may config part fields compare
//    val allOrPartColumn = if (selectedColumns.length > 0) {
//        selectedColumns
//      }
//      else {
//        "*"
//      }
//
//    val sqlComment = if (jdbcType == "mysql") {
//      if (update_col.length > 0) {
//        "select " + allOrPartColumn + " from " + tableName + " where ifnull(" + update_col + "," + create_col + ")" + ">=" + "'" + startTs + "' and  ifnull(" + update_col + "," + create_col + ")" + "<" + "'" + endTs + "' and 1=? and 2=?"
//      }
//      else if (create_col.length > 0) {
//        "select " + allOrPartColumn + " from  " + tableName + " where " + create_col + ">=" + "'" + startTs + "' and  " + create_col + "<'" + endTs + "' and 1=? and 2=?"
//      }
//      else {
//        "select  " + allOrPartColumn + "  from " + tableName + " where 1=? and 2=?"
//      }
//    }
//    else {
//      if (update_col.length > 0) {
//        "select  " + allOrPartColumn + "  from " + tableName + " where nvl(" + update_col + "," + create_col + ")" + ">=" + "TO_DATE('" + startTime + "','YYYYMMDDHH24MISS') and  nvl(" + update_col + "," + create_col + ")" + "<" + " TO_DATE('" + endTime + "','YYYYMMDDHH24MISS') and 1=? and 2=?"
//      }
//      else if (create_col.length > 0) {
//        "select  " + allOrPartColumn + "  from  " + tableName + " where " + create_col + ">=" + "TO_DATE('" + startTime + "','YYYYMMDDHH24MISS') and  " + create_col + "<" + " TO_DATE('" + endTime + "','YYYYMMDDHH24MISS') and 1=? and 2=?"
//      }
//      else {
//        "select  " + allOrPartColumn + "  from " + tableName + " where 1=? and 2=?"
//      }
//    }
//
//    println(sqlComment)
//
//    val jdbcRdd: JdbcRDD[Row] = new JdbcRDD(
//      sparkSession.sparkContext,
//      getConn(jdbcUrl, jdbcType),
//      sqlComment,
//      1, 2, 1,
//      rs => {
//        getRow(rs, resultSchema.fields)
//      }
//    )
//
//    val jdbcDf = sparkSession.createDataFrame(jdbcRdd, resultSchema)
//    jdbcDf
//
//  }
//
//  private def getRow(rs: ResultSet, fields: Seq[StructField]): Row = {
//    val results: Seq[Any] = fields.map(field => {
//      val fieldContent: Any = field.dataType.typeName.split("\\(")(0) match {
//        case "string" => if (rs.getString(field.name) == null) null else rs.getString(field.name).trim
//        case "integer" => rs.getInt(field.name)
//        case "long" => rs.getLong(field.name)
//        case "float" => rs.getFloat(field.name)
//        case "double" => rs.getDouble(field.name)
//        case "boolean" => rs.getBoolean(field.name)
//        case "date" => rs.getDate(field.name)
//        case "timestamp" => rs.getTimestamp(field.name)
//        case "decimal" => rs.getBigDecimal(field.name)
//        case "binary" => rs.getBytes(field.name)
//      }
//      fieldContent
//    })
//
//    Row(results: _*)
//  }
//
//  private def oracle2MysqlFormat(oracleFormat: String) = {
//    oracleFormat match {
//      case "yyyy-MM-dd HH24:mi:ss" => "%Y-%m-%d %T"
//      case "yyyy/MM/dd HH24:mi:ss" => "%Y/%m/%d %T"
//      case "MM-dd-yyyy HH24:mi:ss" => "%m-%d-%Y %T"
//    }
//  }
//
//  private def processTimeCol(timeCol: String, jdbcType: String): String = {
//    val timeList = timeCol.split(",")
//    if (timeList.length > 1) {
//      if (timeList(1) != "string") {
//        logInfo("This can't support another type except for datetype and stringtype.")
//        sparkSession.stop()
//      }
//      if (jdbcType == "mysql") {
//        "str_to_date(" + timeList(0) + ",'" + oracle2MysqlFormat(timeList(2)) + "')"
//      }
//      else "to_date(" + timeList(0) + ",'" + timeList(2) + "')"
//
//    }
//    else timeCol
//  }
//
//  private def transformDataType: PartialFunction[DataType, String] = {
//    case StringType => "string"
//    case LongType => "long"
//    case IntegerType => "int"
//    case BooleanType => "boolean"
//    case DateType => "long"
//    case DoubleType => "double"
//    case FloatType => "float"
//    case TimestampType => "long"
//  }
//
//  private def generateColumn(schema: StructType, columnFamily: String, saveMode: String): String = {
//    //val columnFamily = "CF"
//    val str = "string"
//    val schemaArr: Array[(String, String)] = schema.fieldNames.map(name => (name, transformDataType(schema.apply(schema.fieldIndex(name)).dataType)))
//    saveMode match {
//      case "string" => schemaArr.map {
//        case (name, _) =>
//          if (name.equalsIgnoreCase("ums_ts_"))
//            s""""$name":{"cf":"$columnFamily", "col":"$name", "type":"long"}"""
//          else if (name.equalsIgnoreCase("ums_id_"))
//            s""""$name":{"cf":"$columnFamily", "col":"$name", "type":"long"}"""
//          else if (name.equalsIgnoreCase("ums_active_"))
//            s""""$name":{"cf":"$columnFamily", "col":"$name", "type":"int"}"""
//          else
//            s""""$name":{"cf":"$columnFamily", "col":"$name", "type":"$str"}"""
//
//      }.mkString(",")
//      case "normal" => schemaArr.map {
//        case (name, dataType) =>
//            s""""$name":{"cf":"$columnFamily", "col":"$name", "type":"$dataType"}"""
//      }.mkString(",")
//    }
//  }
//
//  private def generateCatalog(schema: StructType, tableName: String, columnFamily: String, saveMode: String) = {
//    val columns = generateColumn(schema, columnFamily, saveMode)
//    s"""{"table":{"namespace":"default", "name":"$tableName","tableCoder":"PrimitiveType"},"rowkey":"key","columns":{"col0":{"cf":"rowkey", "col":"key", "type":"string"},$columns}}"""
//  }
//
//  private def withCatalog(cat: String): DataFrame = {
//    sparkSession.sqlContext
//      .read
//      .options(Map(HBaseTableCatalog.tableCatalog-> cat))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//  }
//
//  private def getDfFromHbase(dataCheckConfig: DataCheckConfig, schema: StructType, sparkSession: SparkSession): DataFrame = {
//   val sinkSpecificConfig = dataCheckConfig.`sink.specific_config`.split("_")
//    val columnFamily = sinkSpecificConfig(0)
//    val saveMode = sinkSpecificConfig(1)
//    val sinkTable = getTableNameFromNamespace(dataCheckConfig.`sink.namespace`,dataCheckConfig.`sink.type`)
//    implicit var schema = processSchema(sourceSchema, dataCheckConfig.`table.columns`)
//    schema = schema.add(StructField("ums_active_", IntegerType, nullable = true))
//    val catalog = generateCatalog(schema, sinkTable, columnFamily, saveMode)
//    println("======================catalog")
//    println(catalog)
//    println("======================catalog")
//    withCatalog(catalog)
//  }
//
//
//  private def getDfFromHdfs(dataConfig: DataCheckConfig,sourceOrSinkType: String): DataFrame = {
//    val fromTsStr = dataConfig.`compare.start_ts`
//    val toTsStr = dataConfig.`compare.end_ts`
//    val incrementPathList = getParquetList(sourceOrSinkType)
//
//    //if parquet file not exist, or parquet count is 0, will be wrong here
//
//    val incrementDf = sparkSession.read.parquet(incrementPathList: _*)
//
//    val incrementFinalDf: DataFrame = WormholeUtils.getIncrementByTs(incrementDf, WormholeUtils.keys2keyList(dataConfig.`table.keys_cols`), fromTsStr, toTsStr)
//    println("######################")
//    println("incrementFinalDf count" + incrementFinalDf.count())
//    println("######################")
//    incrementFinalDf.drop("ums_op_") //todo do not drop ums_ts_ only drop op
//  }
//
//  private def getValidDataDf(dataConfig: DataCheckConfig, inputDf: DataFrame, sourceOrSinkName: String,sourceOrSinkType:String) = {
//    inputDf.createOrReplaceTempView("filterValidData")
//    val today = dataCheckConfig.`compare.end_ts`
//    val yest = dataCheckConfig.`compare.start_ts`
//    val current = DateUtils.dt2timestamp(today)
//    val yesterday = DateUtils.dt2timestamp(yest)
//
//    val dataActual = sourceOrSinkType match {
//      case  SOURCEDATA =>
//        if (update_column.length > 0) {
//          val sqlComment = "select * from filterValidData where ifnull(" + update_column + "," + create_column + ")" + ">=" + "'" + yesterday + "' and  ifnull(" + update_column + "," + create_column + ")" + "<" + "'" + current + "'"
//          sparkSession.sql(sqlComment)
//        }
//        else if (create_column.length > 0) {
//          val sqlComment = "select * from filterValidData where " + create_column + ">=" + "'" + yesterday + "' and  " + create_column + "<" + "'" + current + "'"
//          sparkSession.sql(sqlComment)
//        }
//        else {
//          val sqlComment = "select * from filterValidData "
//          println("SOURCE: " + sqlComment)
//          sparkSession.sql(sqlComment)
//        }
//      case SINKDATA =>
//        val activeCol = sourceOrSinkName match {
//          case "hdfs" => " 1=1 "
//          case _ => " ums_active_ <>0"
//        }
//        if (update_column.length > 0) {
//          val sqlComment = "select * from filterValidData where ifnull(" + update_column + "," + create_column + ")" + ">=" + "'" + yesterday + "' and  ifnull(" + update_column + "," + create_column + ")" + "<" + "'" + current + "'" + " and " + activeCol
//          sparkSession.sql(sqlComment)
//        }
//        else if (create_column.length > 0) {
//          val sqlComment = "select * from filterValidData where " + create_column + ">=" + "'" + yesterday + "' and  " + create_column + "<" + "'" + current + "'" + " and " + activeCol
//          sparkSession.sql(sqlComment)
//        }
//        else {
//          val sqlComment = "select * from filterValidData where " + activeCol
//          println("SINK: " + sqlComment)
//          sparkSession.sql(sqlComment)
//        }
//    }
//
//    dataActual
//  }
//
//  private def getCoalesceColumnsTable(columns: Array[(String, String)], table: String) = columns.map(column => {
//    val result = new StringBuilder
//    result.append("coalesce(").append(table).append(".").append(column._1).append(",cast('null' as String))")
//    result
//  })
//
//  //insert into table "unmatched_data"
//  private def insertTable(columns: Array[(String, String)], dfTable: String, sinkTable: String, errorType: String, isInformed: Int, activeFlag: Int, namespace: String, sourceSink: String, dataCheckConfig: DataCheckConfig): Unit = {
//
//    val compare = dataCheckConfig.`source.type` + "_" + dataCheckConfig.`sink.type`
//    val info = List("'" + errorType + "' as ERROR_TYPE", "'" + isInformed + "' as IS_INFORMED", "'" + activeFlag + "' as ACTIVE_FLAG", "'" + namespace + "' as NAMESPACE", "'" + sourceSink + "' as SOURCE_SINK", "'" + compare + "' as compare_relation")
//
//    val (outputUser, outputPassword) = getUrlConfig(dataCheckConfig.`result.mysql_connection_url`)
//    val outputProp = new Properties()
//    outputProp.setProperty("user", outputUser)
//    outputProp.setProperty("password", outputPassword)
//    val infoStatement = info.mkString(",")
//    val columnStatement = "concat(" + getCoalesceColumnsTable(columns, dfTable).mkString(",';',") + ")"
//    val concatKeys = keysSql(dataCheckConfig.`table.keys_cols`, dfTable, ",'-edp-',")
//    val sqlStatement = if (update_column.length > 0) {
//      "select " + concatKeys + " as PRIMARY_KEY,ifnull(" + update_column + "," + create_column + ") as UPDATE_TS," + infoStatement + "," + columnStatement + " as  REAL_DATA from " + dfTable
//    }
//    else if (create_column.length > 0) {
//      "select " + concatKeys + " as PRIMARY_KEY," + create_column + " as UPDATE_TS," + infoStatement + "," + columnStatement + " as  REAL_DATA from " + dfTable
//    }
//    else {
//      "select " + concatKeys + " as PRIMARY_KEY,'" + now_ts + "' as UPDATE_TS," + infoStatement + "," + columnStatement + " as  REAL_DATA from " + dfTable
//    }
//    val sqlDf = sparkSession.sql(sqlStatement)
//
//    sqlDf.write.mode("append").jdbc(dataCheckConfig.`result.mysql_connection_url`, sinkTable, outputProp)
//  }
//
//  private def getUrlConfig(url: String) = {
//    val splitUrl = url.split(":")
//    val dbName = splitUrl(1)
//    if (dbName == "mysql") {
//      val urlArray = url.split("user")
//      val urlConfig = urlArray(1).split("&password")
//      val user = urlConfig(0).substring(1)
//      val password = urlConfig(1).substring(1)
//      (user, password)
//    }
//    else {
//      val userConf = splitUrl(3)
//      val splitUser = userConf.split("@")
//      val userPwd = splitUser(0)
//      (userPwd.split("/")(0), userPwd.split("/")(1))
//    }
//  }
//
//  private def getConn(url: String, jdbcType: String): () => Connection = {
//    if (jdbcType == "mysql") {
//      () => {
//        Class.forName("com.mysql.jdbc.Driver").newInstance()
//        DriverManager.getConnection(url)
//      }
//    }
//    else {
//      () => {
//        Class.forName("oracle.jdbc.OracleDriver").newInstance()
//        DriverManager.getConnection(url)
//      }
//    }
//  }
//
//  private def columnArray(columnType: Array[(String, String)]): Array[StringBuilder] = columnType.map(column => {
//    val result = new StringBuilder
//    val (columnName, columnTp) = column
//    if (columnTp.startsWith("Timestamp")) {
//      // result.append("date_format(").append(columnName).append(",'yyyy-MM-dd HH:mm:ss.SSS') as " + columnName)
//      result.append("date_format(").append(columnName).append(",'yyyy-MM-dd HH:mm:ss') as " + columnName)
//
//    } else if (columnTp.startsWith("Date")) {
//      result.append("date_format(").append(columnName).append(",'yyyy-MM-dd') as " + columnName)
//    } else if (columnTp.startsWith("Double") || columnTp.startsWith("Float") || (columnTp.startsWith("Decimal") && (!columnTp.split(",").last.startsWith("0")))) {
//      result.append("format_number(").append(columnName).append(",3) as " + columnName)
//    } else if (columnTp.startsWith("String")) {
//      result.append("trim(").append(columnName).append(") as " + columnName)
//    }
//    else {
//      result.append(columnName)
//    }
//    result
//  })
//
//  private def processSchema(schema: StructType, selectedColumns: String): StructType = {
//    // val columnNames = schema.map(column => column.name.toLowerCase())
//    //decimal（20,0）change to longtype
//    val tmpSchema = StructType(for (columnType <- schema if !columnType.name.equalsIgnoreCase("ums_active_")) yield {
//      val typeName = columnType.dataType.typeName
//      val typeNull = columnType.nullable
//      val typeCol: String = columnType.name.toLowerCase()
//      if (typeName.startsWith("dec") && typeName.split(",")(1).startsWith("0")) {
//        StructField(typeCol, LongType, typeNull)
//      }
//      else {
//        StructField(typeCol, columnType.dataType, typeNull)
//      }
//    })
//    var actualSchema = new StructType
//    if (selectedColumns.length > 0) {
//      val selectedColumnsList: Array[String] = selectedColumns.toLowerCase.split(",")
//      for (field <- tmpSchema.fields) {
//        if (selectedColumnsList.contains(field.name)) {
//          actualSchema = actualSchema.add(field)
//        }
//      }
//    } else actualSchema = tmpSchema
//    actualSchema
//  }
//
//  private def getParquetList(sourceOrSinkType: String): List[String] = {
//    val namespaceArray = sourceOrSinkType match {
//      case SOURCEDATA => dataCheckConfig.`source.namespace`.split("\\.")
//      case SINKDATA => dataCheckConfig.`sink.namespace`.split("\\.")
//    }
//    val fromTsStr = dataCheckConfig.`compare.start_ts`
//    val toTsStr = dataCheckConfig.`compare.end_ts`
//    val sourceNamespace = UmsNamespace(
//      UmsDataSystem.dataSystem(namespaceArray(0)),
//      namespaceArray(1),
//      namespaceArray(2),
//      namespaceArray(3),
//      namespaceArray(4),
//      "", "")
//    val path : String =  sourceOrSinkType match {
//      case SOURCEDATA =>  FileUtils.pf(dataCheckConfig.`source.connection_url`)
//      case SINKDATA => FileUtils.pf(dataCheckConfig.`sink.connection_url`)
//    }
//
//    WhHdfsLogPathUtils.getHdfsLogPathList(
//      List(path, WhHdfsLogPathBase.STG.toString, UmsNamespace.namespacePath(sourceNamespace)).mkString("/"),
//      fromTsStr, toTsStr)
//  }
//
//  //take table structure and column info, decimal need change to long
//   def getSchema(dataCheckConfig: DataCheckConfig) = {
//    dataCheckConfig.`source.type` match {
//      case "hdfs" =>
//        val incrementPathList = getParquetList(SOURCEDATA)
//        sparkSession.read.parquet(incrementPathList.head).drop("ums_op_").first().schema //TODO delete ums_ts_ search ums_ts_
//      case _ =>
//        val connectionUrl = dataCheckConfig.`source.connection_url`
//        val tableName = getTableNameFromNamespace(dataCheckConfig.`source.namespace`,dataCheckConfig.`source.type`).split("\\.")(1)
//        val createCol = dataCheckConfig.`table.create_ts_col`.split(",")(0)
//        val (inputUser, inputPassword) = getUrlConfig(connectionUrl)
//        val selectedColumns: String = dataCheckConfig.`table.columns`
//        val inputProp = new Properties()
//        inputProp.setProperty("user", inputUser)
//        inputProp.setProperty("password", inputPassword)
//        val example: DataFrame = sparkSession.read.jdbc(connectionUrl, tableName, createCol, 0, 0, 1, inputProp)
//        processSchema(example.schema, selectedColumns)
//    }
//  }
//
//  private def getColumnsTable(columns: Array[(String, String)], table: String) = columns.map(column => {
//    val result = new StringBuilder
//    result.append(table).append(".").append(column._1)
//    result
//  })
//
//  private def keysSql(keys: String, sqlTable: String, seq: String) = {
//    val keyList = keys.split(",")
//    val adjustList = keyList.map(key => {
//      sqlTable + "." + key
//    }
//    )
//    val tableKeys = adjustList.mkString(seq)
//    if (keyList.length == 1) {
//      sqlTable + "." + keys
//    } else "concat(" + tableKeys + ")"
//  }
//
//  //hbase Table does not Include database name, others change to database name.table name
//  private def getTableNameFromNamespace(namespace: String, sourceOrSinkType:String) = {
//    val namespaceArray = namespace.split("\\.")
//    val dbName = namespaceArray(2)
//    val tableName = namespaceArray(3).toLowerCase
//    val splitTable = namespaceArray(6).toLowerCase
//    if (sourceOrSinkType == "hbase") {
//      if (splitTable != "0" && splitTable != "*") {
//        splitTable
//      } else {
//        tableName
//      }
//    }
//    else {
//      if (splitTable != "0" && splitTable != "*") {
//        dbName + "." + splitTable
//      } else {
//        dbName + "." + tableName
//      }
//    }
//  }
// private def getRealTypeHbaseDf(firstHbaseDf:DataFrame): DataFrame = {
//   val schema = sourceSchema.add(StructField("ums_active_", IntegerType, nullable = true))
//   val schemaArr: Array[(String, DataType)] = schema.fieldNames.map(name => (name, schema.apply(schema.fieldIndex(name)).dataType))
//
//   val saveMode = dataCheckConfig.`sink.specific_config`.split("_")(1)
//   if (saveMode == "string") {
//     val transformSchema: Array[Column] = schemaArr.map { case (name, dataType) =>
//       name match {
//         case "ums_ts_" => (firstHbaseDf(name)/1000).as(name).cast(dataType)
//         case _=>  firstHbaseDf(name).cast(dataType)
//       }
//     }
//     firstHbaseDf.select(transformSchema: _*)
//   } else {
//     val transformSchema: Array[Column] = schemaArr.map { case (name, dataType) =>
//       dataType match {
//         case TimestampType | DateType => (firstHbaseDf(name)/1000).as(name).cast(dataType)
//         case _=> firstHbaseDf(name).cast(dataType)
//       }
//     }
//     firstHbaseDf.select(transformSchema: _*)
//   }
// }
//  private def getDfFromEs(dataCheckConfig: DataCheckConfig, schema: StructType, sparkSession: SparkSession) = {
//    val indexAndType = dataCheckConfig.`sink.namespace`.split("\\.").slice(2,4).mkString("/")
//    sparkSession.sqlContext.read.format("org.elasticsearch.spark.sql").options(Map("es.nodes"->dataCheckConfig.`sink.connection_url`)).load(indexAndType).cache()
//
//  }
//  private def getRealTypeEsDf(firstEsDf:DataFrame) = {
//    val schema = sourceSchema.add(StructField("ums_active_", IntegerType, nullable = true))
//    val schemaArr: Array[(String, DataType)] = schema.fieldNames.map(name => (name, schema.apply(schema.fieldIndex(name)).dataType))
//
//    val transformSchema: Array[Column] = schemaArr.map { case (name, dataType) => firstEsDf(name).cast(dataType)}
//    firstEsDf.select(transformSchema: _*)
//  }
//  private def getSourceOrSinkDataFrame(sourceOrSinkName: String, sourceOrSinkUrl: String, sparkSession: SparkSession, sourceOrSinkType: String,sourceOrSinkTableName:String) = {
//    sourceOrSinkName match {
//      case "hdfs" =>
//        val firstHdfsDf = getDfFromHdfs(dataCheckConfig,sourceOrSinkType) //drop ums_op_
//         getValidDataDf(dataCheckConfig, firstHdfsDf, sourceOrSinkName,sourceOrSinkType)
//      case "hbase" =>
//        val firstHbaseDf = getDfFromHbase(dataCheckConfig, sourceSchema, sparkSession)
//        val realTypeHbaseDf = getRealTypeHbaseDf(firstHbaseDf)
//        getValidDataDf(dataCheckConfig, realTypeHbaseDf, sourceOrSinkName,sourceOrSinkType)
//      case "es" =>
//        val firstEsDf = getDfFromEs(dataCheckConfig, sourceSchema, sparkSession)
//        val realTypeEsDf = getRealTypeEsDf(firstEsDf)
//        getValidDataDf(dataCheckConfig, realTypeEsDf, sourceOrSinkName,sourceOrSinkType)
//      case _ =>
//        val firstDbDf = getDfFromDb(dataCheckConfig, sourceOrSinkTableName, sourceOrSinkName, sourceOrSinkUrl, sourceSchema, sourceOrSinkType)
//         getValidDataDf(dataCheckConfig, firstDbDf, sourceOrSinkName,sourceOrSinkType)
//    }
//  }
//
//
//  // begin here, source should not has ums fields except hdfs parquet
//  val SOURCEDATA = "SOURCEDATA"
//  val SINKDATA = "SINKDATA"
//
//  val config: Boolean = args(0).endsWith(".json")
//  val dataCheckConfig = if (config) {
//    val file = new File(args(0))
//    JsonUtils.json2caseClass[DataCheckConfig](Source.fromFile(file).getLines().mkString(""))
//  } else JsonUtils.json2caseClass[DataCheckConfig](args(0))
//  assert(dataCheckConfig.`source.type` != "hbase" && dataCheckConfig.`source.type` != "es", "source.type cannot be es")
//
//  val sparkSession: SparkSession = dataCheckConfig.`sink.type` match {
//    case "es" =>new SparkSession.Builder().config("es.mapping.date.rich","false")getOrCreate()
//    case _ => new SparkSession.Builder().getOrCreate()
//  }
//
//  val update_column = dataCheckConfig.`table.update_ts_col`.split(",")(0)
//  val create_column = dataCheckConfig.`table.create_ts_col`.split(",")(0)
//
//  val now_ts = dataCheckConfig.`today.time`
//
//  val sourceTable = getTableNameFromNamespace(dataCheckConfig.`source.namespace`,dataCheckConfig.`source.type`)
//  val sinkTable = getTableNameFromNamespace(dataCheckConfig.`sink.namespace`,dataCheckConfig.`sink.type`)
//
//  val sourceSchema: StructType = getSchema(dataCheckConfig)
//
//
//  val sourceDf = getSourceOrSinkDataFrame(dataCheckConfig.`source.type`, dataCheckConfig.`source.connection_url`, sparkSession, SOURCEDATA,sourceTable).cache
//  //can only be database or hdfs, not support hdfs as source
//  val sinkDf = getSourceOrSinkDataFrame(dataCheckConfig.`sink.type`, dataCheckConfig.`sink.connection_url`, sparkSession, SINKDATA,sinkTable).cache
//  //if hdfs, drop ums_op_
//
//
//
//  sourceDf.createOrReplaceTempView("expected")
//  sinkDf.createOrReplaceTempView("actual")
//
//  val columnTypes = sourceDf.dtypes map { case (colName, colType) => if (colName.equalsIgnoreCase("ums_active_")) null else (colName, colType) } flatMap Option[(String, String)]
//  // source contains ums_active ...
//  val columnString = columnArray(columnTypes)
//  val columnSql = columnString.mkString(",")
//  val keys = dataCheckConfig.`table.keys_cols`
//
//  val sqlExpected = "select " + columnSql + " from expected"
//  val sqlActual = "select " + columnSql + " from actual"
//  println(sqlActual)
//
//
//  val actualData = sparkSession.sql(sqlActual)//
//  val expectedData = sparkSession.sql(sqlExpected)
//
//  val missing: Dataset[Row] = expectedData.except(actualData)
//  missing.createOrReplaceTempView("missing")
//
//  val unexpected: Dataset[Row] = actualData.except(expectedData)
//  unexpected.createOrReplaceTempView("unexpected")
//
//  val unmatched_type = missing.dtypes
//  val columnMissingSql = getColumnsTable(unmatched_type, "missing").mkString(",")
//  val missingSql = keysSql(keys, "missing", ",")
//  val unexpectedSql = keysSql(keys, "unexpected", ",")
//  val mismatchedMissingSql = "select " + columnMissingSql + " from missing,unexpected where " + missingSql + "=" + unexpectedSql
//
//  val columnUnexpectedSql = getColumnsTable(unmatched_type, "unexpected").mkString(",")
//  val mismatchedUnexpectedSql = "select " + columnUnexpectedSql + " from missing,unexpected where " + missingSql + "=" + unexpectedSql
//
//  val mismatchedSourceData: DataFrame = sparkSession.sql(mismatchedMissingSql)
//  val mismatchedSinkData: DataFrame = sparkSession.sql(mismatchedUnexpectedSql)
//
//  val missingData = missing.except(mismatchedSourceData)
//  val unexpectedData = unexpected.except(mismatchedSinkData)
//
//  val mismatchedSourceNum = mismatchedSourceData.count()
//  val mismatchedSinkNum = mismatchedSinkData.count()
//  val missingNum = missingData.count()
//  val unexpectedNum = unexpectedData.count()
//
//  mismatchedSourceData.createOrReplaceTempView("mismatchsource")
//  if (mismatchedSourceNum > 0) {
//    insertTable(unmatched_type, "mismatchsource", "unmatched_data", "mismatch", 0, 1, dataCheckConfig.`source.namespace`, "source", dataCheckConfig)
//  }
//  mismatchedSinkData.createOrReplaceTempView("mismatchsink")
//  if (mismatchedSinkNum > 0) {
//    insertTable(unmatched_type, "mismatchsink", "unmatched_data", "mismatch", 0, 1, dataCheckConfig.`sink.namespace`, "sink", dataCheckConfig)
//  }
//  missingData.createOrReplaceTempView("misdata")
//  if (missingNum > 0) {
//    insertTable(unmatched_type, "misdata", "unmatched_data", "missing", 0, 1, dataCheckConfig.`source.namespace`, "source", dataCheckConfig)
//  }
//  unexpectedData.createOrReplaceTempView("unexpdata")
//  if (unexpectedNum > 0) {
//    insertTable(unmatched_type, "unexpdata", "unmatched_data", "unexpected", 0, 1, dataCheckConfig.`source.namespace`, "sink", dataCheckConfig)
//  }
//  println("%%%%%%%%%%%%%%%")
//  println("source count:" + sourceDf.count())
//  println("second sinkDf count:" + sinkDf.count)
//  println("%%%%%%%%%%%%%%%")
//  sourceDf.unpersist
//  sinkDf.unpersist
//  if (sparkSession != null) sparkSession.stop()
//
//
//}
