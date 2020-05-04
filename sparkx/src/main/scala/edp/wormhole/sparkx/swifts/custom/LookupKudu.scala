package edp.wormhole.sparkx.swifts.custom

import edp.wormhole.kuduconnection.KuduConnection
import edp.wormhole.sparkx.common.SparkSchemaUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.ums.UmsFieldType.{UmsFieldType, umsFieldType}
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.kudu.client.KuduTable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LookupKudu extends EdpLogging {

  def transform(session: SparkSession, df: DataFrame, sqlConfig: SwiftsSql, sourceNamespace: String, sinkNamespace: String,
                connectionConfig: ConnectionConfig, batchSize: Option[Int] = None): DataFrame = {
    val database = sqlConfig.lookupNamespace.get.split("\\.")(2)
    val fromIndex = sqlConfig.sql.indexOf(" from ")
    val afterFromSql = sqlConfig.sql.substring(fromIndex + 6).trim
    val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
    val tableName = KuduConnection.getTableName(tmpTableName, database)
    logInfo("tableName:" + tableName)
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    val table: KuduTable = client.openTable(tableName)
    val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
    val tableSchema: mutable.Map[String, String] = KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)
    logInfo(s"query data from table $tableName success")
    KuduConnection.closeClient(client)

    val resultSchema: StructType = {
      var resultSchema: StructType = df.schema

      val selectFieldArray: Array[(String, String)] = getFieldsArrayLookup(sqlConfig.fields.get) //select fields,key=sourcename,value=newname

      selectFieldArray.foreach(field => {
        val sourceName = field._1
        val asName = field._2

        if(tableSchema.contains(sourceName)) {
          resultSchema = resultSchema.add(StructField(asName, SparkSchemaUtils.ums2sparkType(umsFieldType(tableSchema(sourceName)))))
        } else {
          log.error(s"""kudu table $database.$tmpTableName not contain field $sourceName, all fields is $tableSchema""")
          throw new Exception(s"""kudu table $database.$tmpTableName not contain field $sourceName""")
        }
      })
      resultSchema
    }

    val joinedRDD = df.rdd.mapPartitions(partition => {
      KuduConnection.initKuduConfig(connectionConfig)

      val selectFieldNewNameArray: Seq[String] = getFieldsArrayLookup(sqlConfig.fields.get).map(_._1).toList //select fields,newname

      val originalData: ListBuffer[Row] = partition.to[ListBuffer]
      val originalDataSize = originalData.size
      val resultData = ListBuffer.empty[Row]

      val lookupFieldNameArray = sqlConfig.lookupTableFields.get
      if (lookupFieldNameArray.length == 1) {
        //sink table field names
        val sourceFieldName = sqlConfig.sourceTableFields.get.head
        val keyType = UmsFieldType.umsFieldType(KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)(lookupFieldNameArray.head))
        val keySchemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
        keySchemaMap(lookupFieldNameArray.head) = (0, keyType, true)

        //        originalData.grouped(batchSize.get).foreach((subList: mutable.Seq[Row]) => {
        val tupleList: mutable.Seq[List[String]] = originalData.map(row => {
          sqlConfig.sourceTableFields.get.toList.map(field => {
            val tmpKey = row.get(row.fieldIndex(field))
            if (tmpKey == null) null.asInstanceOf[String]
            else tmpKey.toString
          })

        }).filter((keys: Seq[String]) => {
          !keys.contains(null)
        })

        val queryDataMap: mutable.Map[String, ListBuffer[Map[String, (Any, String)]]] =
          KuduConnection.doQueryMultiByKeyListInBatch(tmpTableName, database, connectionConfig.connectionUrl,
            lookupFieldNameArray.head, tupleList, keySchemaMap.toMap, selectFieldNewNameArray, batchSize.getOrElse(1),
            tableSchemaInKudu, sqlConfig.lookupTableConstantCondition)
        val queryDataMapSize = queryDataMap.size
        logInfo(s"doQueryMultiByKeyListInBatch,originalDataSize:$originalDataSize,queryDataMapSize:$queryDataMapSize.")
        originalData.foreach((row: Row) => {
          val originalArray: Array[Any] = row.schema.fieldNames.map(name => row.get(row.fieldIndex(name)))
          val joinData = row.get(row.fieldIndex(sourceFieldName))
          if (joinData == null || queryDataMap == null || queryDataMap.isEmpty || !queryDataMap.contains(joinData.toString))
            resultData.append(getJoinRow(selectFieldNewNameArray, null.asInstanceOf[Map[String, (Any, String)]], originalArray, resultSchema))
          else queryDataMap(joinData.toString).foreach(data => {
            resultData.append(getJoinRow(selectFieldNewNameArray, data, originalArray, resultSchema))
          })

        })
      } else {
        val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
        try {
          val table: KuduTable = client.openTable(tableName)
          val sourceFieldNameArray = sqlConfig.sourceTableFields.get
          originalData.map(row => {
            val tuple: Array[String] = sourceFieldNameArray.map(field => {
              val tmpKey = row.get(row.fieldIndex(field))
              if (tmpKey == null) null.asInstanceOf[String]
              else tmpKey.toString
            }).filter(key => {
              key != null
            })


            val originalArray: Array[Any] = row.schema.fieldNames.map(name => row.get(row.fieldIndex(name)))
            if (tuple == null || tuple.isEmpty || tuple.length != sourceFieldNameArray.length) {
              resultData.append(getJoinRow(selectFieldNewNameArray, null.asInstanceOf[Map[String, (Any, String)]], originalArray, resultSchema))
            } else {
              val queryResult: mutable.HashMap[String, ListBuffer[Map[String, (Any, String)]]] =
                KuduConnection.doQueryMultiByKey(lookupFieldNameArray, tuple.toList, tableSchemaInKudu, client, table, selectFieldNewNameArray, sqlConfig.lookupTableConstantCondition)

              if (queryResult == null || queryResult.isEmpty) {
                resultData.append(getJoinRow(selectFieldNewNameArray, null.asInstanceOf[Map[String, (Any, String)]], originalArray, resultSchema))
              } else {
                queryResult.head._2.foreach(data => {
                  val newRow: GenericRowWithSchema = getJoinRow(selectFieldNewNameArray, data, originalArray, resultSchema)
                  resultData.append(newRow)
                })
              }
            }

            resultData
          })
          logInfo(s"query data from table $tableName success")
        } catch {
          case e: Throwable =>
            logError("LookupKudu", e)
            throw e
        } finally {
          KuduConnection.closeClient(client)
        }
      }

      val resultDataSize = resultData.size
      logInfo(s"lookup finish,originalDataSize:$originalDataSize,resultData:$resultDataSize")
      resultData.toIterator
    })
    session.createDataFrame(joinedRDD, resultSchema)
  }


  def getFieldsArrayLookup(fields: String): Array[(String, String)] = {
    fields.split(",").map(f => {
      val fields = f.split(":")
      val sourceName = fields(0).trim
      val fields1trim = fields(1).trim
      if (fields1trim.toLowerCase.contains(" as ")) {
        val asIndex = fields1trim.toLowerCase.indexOf(" as ")
        val newName = fields1trim.substring(asIndex + 4).trim
        (sourceName, newName)
      } else {
        (sourceName, sourceName)
      }
    })
  }

  def getFieldsArrayUnion(fields: String): Array[(String, String, String)] = {
    fields.split(",").map(f => {
      val fields = f.split(":")
      val sourceName = fields(0).trim
      val fields1trim = fields(1).trim
      if (fields1trim.toLowerCase.contains(" as ")) {
        val asIndex = fields1trim.toLowerCase.indexOf(" as ")
        val fieldType = fields1trim.substring(0, asIndex).trim
        val newName = fields1trim.substring(asIndex + 4).trim
        (sourceName, newName, fieldType)
      } else {
        (sourceName, sourceName, fields1trim)
      }
    })
  }


  def getJoinRow(fieldArray: Seq[String], queryFieldsResultMap: Map[String, (Any, String)], originalArray: Array[Any], resultSchema: StructType): GenericRowWithSchema = {
    val outputArray = ListBuffer.empty[Any]
    for (i <- fieldArray.indices) {
      val queryFieldName = fieldArray(i)
      val toSpark = if (queryFieldsResultMap == null || queryFieldsResultMap.isEmpty) SparkSchemaUtils.s2sparkValue(null, umsFieldType(UmsFieldType.STRING.toString))
      else {
        val (fieldContent, fieldType) = queryFieldsResultMap(queryFieldName)
        if (fieldType == null || fieldType.isEmpty) SparkSchemaUtils.s2sparkValue(null, umsFieldType(UmsFieldType.STRING.toString))
        else SparkSchemaUtils.s2sparkValue(if (fieldContent == null) null else fieldContent.toString, umsFieldType(fieldType))
      }
      outputArray += toSpark
    }

    new GenericRowWithSchema(originalArray ++ outputArray, resultSchema)

  }

}
