package edp.wormhole.kuduconnection

import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsActiveType, UmsFieldType, UmsOpType, UmsSysField}
import edp.wormhole.util.DateUtils
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import org.apache.kudu.Type
import org.apache.kudu.client._
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable

object KuduConnection extends Serializable {

  private lazy val logger = Logger.getLogger(this.getClass)

  private lazy val defaultMutationBufferSpace = 50000

  private lazy val defaultAdminOperationTimeoutMs = 60000

  private lazy val defaultOperationTimeoutMs = 60000

  private lazy val defaultSocketReadTimeoutMs = 60000

  private lazy val defaultTimeoutMillis = 60000

  val kuduConfigurationMap: mutable.HashMap[String, ConnectionConfig] = new mutable.HashMap[String, ConnectionConfig]

  def initKuduConfig(connectionConfig: ConnectionConfig): Unit = {
    kuduConfigurationMap(connectionConfig.connectionUrl) = connectionConfig
  }

  def getKuduConfigParamsMap(kvConfigs: Option[Seq[KVConfig]]): Map[String, String] = {
    if (kvConfigs.isDefined) {
      kvConfigs.get.map(kv => (kv.key, kv.value)).toMap[String, String]
    } else {
      Map.empty[String, String]
    }
  }

  def getKuduClient(kuduUrl: String): KuduClient = {
    val connectionConfig = kuduConfigurationMap(kuduUrl)
    var client = new KuduClient.KuduClientBuilder(kuduUrl.split(",").toList)
    val kvConfigMap = getKuduConfigParamsMap(connectionConfig.parameters)
    client =
      client
        .defaultAdminOperationTimeoutMs(
          kvConfigMap.getOrElse[String]("AdminOperationTimeoutMs", defaultAdminOperationTimeoutMs.toString).toLong)
        .defaultOperationTimeoutMs(
          kvConfigMap.getOrElse[String]("OperationTimeoutMs", defaultOperationTimeoutMs.toString).toLong)
        .defaultSocketReadTimeoutMs(
          kvConfigMap.getOrElse[String]("SocketReadTimeoutMs", defaultSocketReadTimeoutMs.toString).toLong)
    client.build()
  }

  def getSession(url: String, client: KuduClient): KuduSession = {
    val config = kuduConfigurationMap(url)
    val kvConfigMap = getKuduConfigParamsMap(config.parameters)
    val session: KuduSession = client.newSession()

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)
    session.setTimeoutMillis(kvConfigMap.getOrElse[String]("TimeoutMillis", defaultTimeoutMillis.toString).toLong)
    session.setMutationBufferSpace(kvConfigMap.getOrElse[String]("MutationBufferSpace", defaultMutationBufferSpace.toString).toInt)

    session
  }


  def closeSession(session: KuduSession): Unit = {
    if (session != null && !session.isClosed)
      try {
        session.close
      } catch {
        case e: Throwable =>
          logger.error("Close KuduSession Error!", e)
      }
  }

  def closeClient(client: KuduClient): Unit = {
    if (client != null)
      try {
        client.close()
      } catch {
        case e: Throwable =>
          logger.error("Close KuduClient Error!", e)
      }
  }

  def getAllFieldsKuduTypeMap(table: KuduTable): mutable.HashMap[String, Type] = {
    val fieldTypeMap = mutable.HashMap.empty[String, Type]
    val schema = table.getSchema
    val columns = schema.getColumns
    for (i <- 0 until columns.size()) {
      val columnSchema = columns.get(i)
      val columnName = columnSchema.getName
      val columnType = columnSchema.getType
      fieldTypeMap(columnName.toLowerCase) = columnType
    }
    fieldTypeMap
  }

  def getAllFieldsUmsTypeMap(fieldsKuduTypeMap: mutable.HashMap[String, Type]): mutable.HashMap[String, String] = {
    fieldsKuduTypeMap.map { case (fieldName, fieldType) =>
      fieldType match {
        case Type.STRING =>
          (fieldName, UmsFieldType.STRING.toString)
        case Type.INT64 =>
          (fieldName, UmsFieldType.LONG.toString)
        case Type.INT8 | Type.INT16 | Type.INT32 =>
          (fieldName, UmsFieldType.INT.toString)
        case Type.FLOAT =>
          (fieldName, UmsFieldType.FLOAT.toString)
        case Type.DOUBLE =>
          (fieldName, UmsFieldType.DOUBLE.toString)
        case Type.BOOL =>
          (fieldName, UmsFieldType.BOOLEAN.toString)
        case Type.DECIMAL =>
          (fieldName, UmsFieldType.DECIMAL.toString)
        case Type.BINARY =>
          (fieldName, UmsFieldType.BINARY.toString)
        case Type.UNIXTIME_MICROS =>
          (fieldName, UmsFieldType.LONG.toString)
      }
    }
  }

  def getTableSchema(tableName: String, database: String, url: String): mutable.Map[String, Type] = {
    val client = getKuduClient(url)

    val newTableName = getTableName(tableName, database)
    val table: KuduTable = client.openTable(newTableName)
    val fieldTypeMap: mutable.Map[String, Type] = getAllFieldsKuduTypeMap(table)
    client.close()
    fieldTypeMap
  }

  def getTableName(tableName: String, database: String): String = {
    if (database == "default") tableName else database + tableName
  }

  def doQueryByKeyList(tableName: String, database: String, url: String, keysName: Seq[String], tupleList: Seq[Seq[String]], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       queryFieldsName: Seq[String]): mutable.HashMap[String, Map[String, (Any, String)]] = {
    logger.info("doQueryByKeyList:" + kuduConfigurationMap(url) + ":::" + tableName)
    val queryResultMap = mutable.HashMap.empty[String, Map[String, (Any, String)]]
    val client: KuduClient = getKuduClient(url)
    try {
      val newTableName = getTableName(tableName, database)
      val table: KuduTable = client.openTable(newTableName)
      val fieldTypeMap: mutable.Map[String, Type] = getAllFieldsKuduTypeMap(table)

      tupleList.foreach((row: Seq[String]) => {
        val keysContent = keysName.map(keyName => {
          row(schemaMap(keyName)._1)
        })
        val (keysStr, valueMap) = doQueryByKey(keysName, keysContent, fieldTypeMap, client, table, queryFieldsName)
        if (valueMap.nonEmpty) queryResultMap(keysStr) = valueMap

      })
    } catch {
      case e: Throwable =>
        logger.error("doQueryByKeyList", e)
        throw e
    } finally {
      closeClient(client)
    }
    queryResultMap
  }

  def doQueryByKeyListInBatch(tableName: String, database: String, url: String, keyName: String, tupleList: Seq[Seq[String]], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                              queryFieldsName: Seq[String]): mutable.HashMap[String, Map[String, (Any, String)]] = {
    logger.info("doQueryByKeyListInBatch:" + kuduConfigurationMap(url) + ":::" + tableName)
    val queryResultMap = mutable.HashMap.empty[String, Map[String, (Any, String)]]
    val client: KuduClient = getKuduClient(url)
    try {
      val newTableName = getTableName(tableName, database)
      val table: KuduTable = client.openTable(newTableName)
      val fieldTypeMap: mutable.Map[String, Type] = getAllFieldsKuduTypeMap(table)
      val keyType: Type = fieldTypeMap(keyName)

      val dataList: Seq[Any] = tupleList.map(row => {
        val keyData = row(schemaMap(keyName)._1)
        keyType match {
          case Type.STRING =>
            keyData
          case Type.INT64 =>
            keyData.toLong
          case Type.INT8 | Type.INT16 | Type.INT32 =>
            keyData.toInt
          case _ =>
            keyData
        }
      })


      val kuduPredicate = KuduPredicate.newInListPredicate(table.getSchema.getColumn(keyName), dataList)
      val scannerBuilder: KuduScanner.KuduScannerBuilder = client.newScannerBuilder(table)
        .setProjectedColumnNames(queryFieldsName) //指定输出列
      scannerBuilder.addPredicate(kuduPredicate)
      val scanner = scannerBuilder.build()

      while (scanner.hasMoreRows) {
        val results = scanner.nextRows
        while (results.hasNext) {
          val result = results.next()
          val schema = result.getSchema
          val queryResult: Map[String, (Any, String)] = queryFieldsName.map(f => {
            val value: (Any, String) = schema.getColumn(f).getType match {
              case Type.STRING => (if (result.isNull(f)) null else result.getString(f), UmsFieldType.STRING.toString)
              case Type.BOOL => (if (result.isNull(f)) null else result.getBoolean(f), UmsFieldType.BOOLEAN.toString)
              case Type.BINARY => (if (result.isNull(f)) null else result.getBinary(f), UmsFieldType.BINARY.toString)
              case Type.DECIMAL => (if (result.isNull(f)) null.asInstanceOf[String] else result.getDecimal(f), UmsFieldType.DECIMAL.toString)
              case Type.DOUBLE => (if (result.isNull(f)) null else result.getDouble(f), UmsFieldType.DOUBLE.toString)
              case Type.INT8 | Type.INT16 | Type.INT32 => (if (result.isNull(f)) null else result.getInt(f), UmsFieldType.INT.toString)
              case Type.FLOAT => (if (result.isNull(f)) null else result.getFloat(f), UmsFieldType.FLOAT.toString)
              case Type.INT64 => (if (result.isNull(f)) null else result.getLong(f), UmsFieldType.LONG.toString)
              case Type.UNIXTIME_MICROS => (if (result.isNull(f)) null else DateUtils.dt2dateTime(result.getLong(f)), UmsFieldType.DATETIME.toString)
              case _ => (if (result.isNull(f)) null else result.getString(f), UmsFieldType.STRING.toString)
            }
            (f, value)
          }).toMap
          queryResultMap(queryResult(keyName)._1.toString) = queryResult
        }
      }
    } catch {
      case e: Throwable =>
        logger.error("doQueryByKeyListInBatch", e)
        throw e
    } finally {
      closeClient(client)
    }

    queryResultMap
  }

  def doQueryByKey(keysName: Seq[String], keysContent: Seq[String], keysTypeMap: mutable.Map[String, Type],
                   client: KuduClient, table: KuduTable, queryFieldsName: Seq[String]): (String, Map[String, (Any, String)]) = {
    val scannerBuilder: KuduScanner.KuduScannerBuilder = client.newScannerBuilder(table)
      .setProjectedColumnNames(queryFieldsName) //指定输出列

    for (i <- keysName.indices) {
      val keyContent = keysContent(i)
      val keyName = keysName(i)
      val kuduPredicate = keysTypeMap(keyName) match {
        case Type.STRING =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent)
        case Type.INT64 =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.toLong)
        case Type.INT8 | Type.INT16 | Type.INT32 =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.toInt)
        case Type.FLOAT =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.toFloat)
        case Type.DOUBLE =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.toDouble)
        case Type.BOOL =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.toBoolean)
        case Type.DECIMAL =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, new java.math.BigDecimal(keyContent))
        case Type.BINARY =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, keyContent.getBytes())
        case Type.UNIXTIME_MICROS =>
          KuduPredicate.newComparisonPredicate(table.getSchema.getColumn(keyName), KuduPredicate.ComparisonOp.EQUAL, DateUtils.dt2long(keyContent))
      }
      scannerBuilder.addPredicate(kuduPredicate)
    }
    val scanner = scannerBuilder.build()

    val keysStr = keysContent.mkString("_")

    if (scanner.hasMoreRows) {
      val results = scanner.nextRows
      if (results.hasNext) {
        val result = results.next()
        val schema = result.getSchema
        val queryResult: Map[String, (Any, String)] = queryFieldsName.map(f => {
          val value: (Any, String) = schema.getColumn(f).getType match {
            case Type.STRING => (if (result.isNull(f)) null else result.getString(f), UmsFieldType.STRING.toString)
            case Type.BOOL => (if (result.isNull(f)) null else result.getBoolean(f), UmsFieldType.BOOLEAN.toString)
            case Type.BINARY => (if (result.isNull(f)) null else result.getBinary(f), UmsFieldType.BINARY.toString)
            case Type.DECIMAL => (if (result.isNull(f)) null.asInstanceOf[String] else result.getDecimal(f), UmsFieldType.DECIMAL.toString)
            case Type.DOUBLE => (if (result.isNull(f)) null else result.getDouble(f), UmsFieldType.DOUBLE.toString)
            case Type.INT8 | Type.INT16 | Type.INT32 => (if (result.isNull(f)) null else result.getInt(f), UmsFieldType.INT.toString)
            case Type.FLOAT => (if (result.isNull(f)) null else result.getFloat(f), UmsFieldType.FLOAT.toString)
            case Type.INT64 => (if (result.isNull(f)) null else result.getLong(f), UmsFieldType.LONG.toString)
            case Type.UNIXTIME_MICROS => (if (result.isNull(f)) null else DateUtils.dt2dateTime(result.getLong(f)), UmsFieldType.DATETIME.toString)
            case _ => (if (result.isNull(f)) null else result.getString(f), UmsFieldType.STRING.toString)
          }
          (f, value)
        }).toMap
        (keysStr, queryResult)
      } else (keysStr, Map.empty[String, (Any, String)])
    } else (keysStr, Map.empty[String, (Any, String)])
  }

  def fillRow(row: PartialRow, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], data: Seq[String]) {
    schemaMap.foreach(fieldSchema => {
      val fieldName = fieldSchema._1
      val fieldContent: String = data(fieldSchema._2._1)
      fieldSchema._2._2 match {
        case UmsFieldType.STRING =>
          if (fieldName == UmsSysField.OP.toString) {
            row.addInt(UmsSysField.ACTIVE.toString, if (UmsOpType.DELETE.toString == fieldContent.toLowerCase) UmsActiveType.INACTIVE else UmsActiveType.ACTIVE)
          } else {
            if (fieldContent == null) row.setNull(fieldName) else row.addString(fieldName, fieldContent)
          }
        case UmsFieldType.BOOLEAN => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addBoolean(fieldName, fieldContent.toBoolean)
        case UmsFieldType.BINARY => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addBinary(fieldName, fieldContent.getBytes())
        case UmsFieldType.DECIMAL => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addDecimal(fieldName, new java.math.BigDecimal(fieldContent))
        case UmsFieldType.DOUBLE => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addDouble(fieldName, fieldContent.toDouble)
        case UmsFieldType.INT => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addInt(fieldName, fieldContent.toInt)
        case UmsFieldType.FLOAT => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addFloat(fieldName, fieldContent.toFloat)
        case UmsFieldType.LONG => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addLong(fieldName, fieldContent.toLong)
        case UmsFieldType.DATETIME => if (fieldContent == null || fieldContent.trim.isEmpty) row.isNull(fieldName) else row.addLong(fieldName, DateUtils.dt2long(fieldContent))
        case _ => row.addString(fieldName, fieldContent)
      }
    })
  }


  def doInset(tableName: String, database: String, url: String, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], fieldsContent: Seq[Seq[String]]): Int = {
    doWrite(tableName, database, url, schemaMap, fieldsContent, "insert")
  }

  def doUpdate(tableName: String, database: String, url: String, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], fieldsContent: Seq[Seq[String]]): Int = {
    doWrite(tableName, database, url, schemaMap, fieldsContent, "update")
  }

  def doWrite(tableName: String, database: String, url: String, schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], fieldsContent: Seq[Seq[String]], optType: String): Int = {
    logger.info("doWrite:" + kuduConfigurationMap + ":::" + tableName)
    var errorsCount = 0
    val client: KuduClient = getKuduClient(url)
    try {
      val newTableName = getTableName(tableName, database)
      val table: KuduTable = client.openTable(newTableName)
      val session = getSession(url, client)

      fieldsContent.foreach(content => {
        val opt = if (optType == "insert") table.newInsert else table.newUpdate()
        val row = opt.getRow

        fillRow(row, schemaMap, content)
        session.apply(opt)
      })

      session.flush()

      closeSession(session)

      errorsCount = session.countPendingErrors()

      if (errorsCount != 0) {
        logger.error("do " + optType + " has error,error count=" + errorsCount)
      }

    } catch {
      case e: Throwable =>
        logger.error("doQueryByKeyListInBatch", e)
        throw e
    } finally {
      closeClient(client)
    }
    errorsCount
  }

}
