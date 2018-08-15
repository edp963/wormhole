package edp.wormhole.swifts.custom

import edp.wormhole.common.ConnectionConfig
import edp.wormhole.spark.common.SparkSchemaUtils
import edp.wormhole.spark.common.SparkSchemaUtils.ums2sparkType
import edp.wormhole.sinks.kudu.KuduConnection
import edp.wormhole.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.SwiftsSql
import edp.wormhole.ums.UmsFieldType.{UmsFieldType, umsFieldType}
import edp.wormhole.ums.UmsFieldType
import org.apache.kudu.client.KuduTable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LookupKudu extends EdpLogging {

  def transform(session: SparkSession, df: DataFrame, sqlConfig: SwiftsSql, sourceNamespace: String, sinkNamespace: String, connectionConfig: ConnectionConfig): DataFrame = {
    val database = sqlConfig.lookupNamespace.get.split("\\.")(2)
    val fromIndex = sqlConfig.sql.indexOf(" from ")
    val afterFromSql = sqlConfig.sql.substring(fromIndex + 6).trim
    val tmpTableName = afterFromSql.substring(0, afterFromSql.indexOf(" ")).trim
    val tableName = KuduConnection.getTableName(tmpTableName,database)
    logInfo("tableName:" + tableName)
    KuduConnection.initKuduConfig(connectionConfig)
    val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
    val table: KuduTable = client.openTable(tableName)
    val tableSchemaInKudu = KuduConnection.getAllFieldsKuduTypeMap(table)
    val tableSchema: mutable.Map[String, String] = KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)
    KuduConnection.closeClient(client)

    val resultSchema: StructType = {
      var resultSchema: StructType = df.schema

      val fieldArray: Array[(String, String)] = getFieldsArray(sqlConfig.fields.get)

      fieldArray.foreach(field => {
        val sourceName = field._1
        val asName = field._2

        resultSchema = resultSchema.add(StructField(asName, ums2sparkType(umsFieldType(tableSchema(sourceName)))))
      })
      resultSchema
    }

    val joinedRDD = df.rdd.mapPartitions(partition => {
      KuduConnection.initKuduConfig(connectionConfig)

      val fieldArray: Seq[String] = getFieldsArray(sqlConfig.fields.get).map(_._1).toList

      val originalData: ListBuffer[Row] = partition.to[ListBuffer]
      val resultData = ListBuffer.empty[Row]

      if (sqlConfig.lookupTableFields.get.length == 1) {
        val keysName = sqlConfig.lookupTableFields.get.toList
        val keyName  = keysName.head
        val keyType = UmsFieldType.umsFieldType(KuduConnection.getAllFieldsUmsTypeMap(tableSchemaInKudu)(keyName))
        val keySchemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
        keySchemaMap(keyName)=(0,keyType,true)

        originalData.sliding(1000,1000).foreach((subList: mutable.Seq[Row]) =>{
          val tupleList: mutable.Seq[List[String]] = subList.map(row=>{
            keysName.map(field => {
              row.get(row.fieldIndex(field)).toString

            })
          })
          val queryMap: mutable.Map[String, Map[String, (Any, String)]] =
            KuduConnection.doQueryByKeyListInBatch(tmpTableName,database, connectionConfig.connectionUrl, keyName, tupleList, keySchemaMap.toMap,  fieldArray)

          subList.foreach((row: Row) =>{
            val originalArray: Array[Any] = row.schema.fieldNames.map(name => row.get(row.fieldIndex(name)))
            val keyContent = row.get(row.fieldIndex(keyName)).toString
            val queryFieldsResultMap: Map[String, (Any, String)] = if(queryMap==null||queryMap.isEmpty || !queryMap.contains(keyContent))  null.asInstanceOf[Map[String, (Any, String)]]
            else queryMap(keyContent)
            resultData.append(getJoinRow(fieldArray,queryFieldsResultMap,originalArray,resultSchema))
          })
        })
      } else {
        val client = KuduConnection.getKuduClient(connectionConfig.connectionUrl)
        val table: KuduTable = client.openTable(tableName)
        originalData.map(row => {
          val keysName: Array[String] = sqlConfig.lookupTableFields.get
          val tuple: Array[String] = keysName.map(field => {
            row.get(row.fieldIndex(field)).toString
          })

          val originalArray: Array[Any] = row.schema.fieldNames.map(name => row.get(row.fieldIndex(name)))

          val queryResult: (String, Map[String, (Any, String)]) = KuduConnection.doQueryByKey(keysName, tuple.toList, tableSchemaInKudu, client, table, fieldArray)

          val queryFieldsResultMap: Map[String, (Any, String)] = queryResult._2
          val newRow: GenericRowWithSchema = getJoinRow(fieldArray,queryFieldsResultMap,originalArray,resultSchema)
          resultData.append(newRow)
        })
        KuduConnection.closeClient(client)
      }

      resultData.toIterator
    })
    session.createDataFrame(joinedRDD, resultSchema)
  }

  def getFieldsArray(fields: String): Array[(String, String)] = {
    fields.split(",").map(f => {
      val trimF = f.trim
      val lowerF = trimF.toLowerCase
      val asPosition = lowerF.indexOf(" as ")
      if (asPosition > 0) (trimF.substring(0, asPosition).trim, trimF.substring(asPosition + 4).trim)
      else (trimF, trimF)
    })
  }

  def getJoinRow(fieldArray: Seq[String],queryFieldsResultMap: Map[String, (Any, String)],originalArray: Array[Any],resultSchema: StructType):GenericRowWithSchema = {
    val outputArray = ListBuffer.empty[Any]
    for (i <- fieldArray.indices) {
      val queryFieldName = fieldArray(i)
      val toSpark = if (queryFieldsResultMap==null||queryFieldsResultMap.isEmpty) SparkSchemaUtils.s2sparkValue(null, umsFieldType(UmsFieldType.STRING.toString))
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
