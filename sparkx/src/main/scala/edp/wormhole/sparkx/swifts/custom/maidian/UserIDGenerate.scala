package edp.wormhole.sparkx.swifts.custom.maidian

import java.sql.{Connection, PreparedStatement, ResultSet, SQLTransientConnectionException}
import java.util.UUID

import com.alibaba.fastjson.JSON
import edp.wormhole.dbdriver.dbpool.DbConnection
import edp.wormhole.sinks.utils.SinkDbSchemaUtils
import edp.wormhole.sparkx.common.SparkSchemaUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.ums.{UmsFieldType, UmsSysField}
import edp.wormhole.util.config.ConnectionConfig
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

class UserIDGenerate extends EdpLogging {
  //{"jdbcUrl":"jdbc:mysql://ip:3306/test","username":"un","password":"pw","tableName":"tn","selectFieldName":"customer_uuid","conditions":[{"user_name":"customer_name","cred_type":"main_id_type","cred_num":"main_id_num"},{"mobilenum":"main_mobile"},{"user_name":"customer_name","user_birthday":"customer_birthday","user_gender":"customer_gender"}]}
  def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, param: String, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    val qcp = parseParam(param)
    logInfo("parse jdbc & condition success")

    val schemaMap: Map[String, StructField] = df.schema.map(ss => (ss.name.toLowerCase(), ss)).toMap

    var resultSchema: StructType = df.schema
    val umsIdIndex = df.schema.fieldIndex(UmsSysField.ID.toString)
    resultSchema = resultSchema.add(StructField(qcp.selectFieldName, SparkSchemaUtils.ums2sparkType(UmsFieldType.STRING)))

    val userIDRDD = df.rdd.mapPartitions(p => {
      val resultData = ListBuffer.empty[Row]
      val cc = ConnectionConfig(qcp.jdbcUrl, Some(qcp.username), Some(qcp.password), None)
      val selectPartSql = getSelectPartSql(qcp)
      var errorCount = 0
      p.foreach((row: Row) => {

        val (queryResult, selectFieldValue, umsId) = generateOneRowID(qcp, row, selectPartSql, cc, schemaMap)
        if (queryResult == -1) {
          errorCount += 1
        }
        val selectFieldNewNameArray: Array[String] = Array {
          selectFieldValue
        }

        val rowReplace = ListBuffer.empty[Any]
        for (i <- 0 until row.size) {
          if (i == umsIdIndex) {
            if (queryResult == 1) {
              rowReplace += umsId + 1
            } else rowReplace += row.get(i)
          } else {
            rowReplace += row.get(i)
          }
        }

        resultData.append(new GenericRowWithSchema(rowReplace.toArray ++ selectFieldNewNameArray, resultSchema))

      })

      resultData.toIterator
    })

    val selectFieldName = qcp.selectFieldName
    var tmpDf: DataFrame = session.createDataFrame(userIDRDD, resultSchema).where(s"$selectFieldName is not null").cache()

    var finalDf = session.createDataFrame(session.sparkContext.emptyRDD[Row], resultSchema)

    qcp.conditions.foreach(condition => {
      val columnNames: Array[String] = condition.map(_._1)
      val notNullCondition = columnNames.map(c => {
        s"$c is not null"
      }).mkString(" and ")
      val dropDuplicatesDf = tmpDf.where(notNullCondition).dropDuplicates(columnNames)
      finalDf = finalDf.union(dropDuplicatesDf)

      val isNullCondition = columnNames.map(c => {
        s"$c is null"
      }).mkString(" or ")
      val hasNullDf = tmpDf.where(isNullCondition).cache()
      tmpDf.unpersist()
      tmpDf = hasNullDf

    })

    finalDf = finalDf.union(tmpDf)
    tmpDf.unpersist()

    finalDf

  }

  def generateOneRowID(qcp: QueryConditionParam, row: Row, selectPartSql: String, cc: ConnectionConfig, schemaMap: Map[String, StructField]): (Int, String, Long) = {
    var flag = true
    var i = 0
    var selectFieldValue = null.asInstanceOf[String]
    var umsId = -1l
    var queryResult = 0
    while (flag) {
      val conditionArray = qcp.conditions(i)
      i += 1
      val (tmpQueryResult, tmpSelectFieldValue, tmpUmsId) = queryByCondition(selectPartSql, cc, qcp.selectFieldName, conditionArray, row, schemaMap)

      if (qcp.conditions.size == i || tmpQueryResult == 1 || tmpQueryResult == -1) {
        flag = false
      }

      queryResult = tmpQueryResult
      if (tmpQueryResult == 1) {
        selectFieldValue = tmpSelectFieldValue
        umsId = tmpUmsId
//        logInfo("查到id，无需生成")
      }
//      else if (tmpQueryResult == 0) {
//        logInfo("未查到id，生成，" + row)
//      }
    }

    if (selectFieldValue == null) selectFieldValue = UUID.randomUUID().toString

    (queryResult, selectFieldValue, umsId)
  }

  def queryByCondition(selectPartSql: String, cc: ConnectionConfig, selectFieldName: String, conditionArray: Array[(String, String)],
                       row: Row, schemaMap: Map[String, StructField]): (Int, String, Long) = {
    var hasNull = false
    for (i <- conditionArray.indices) {
      val fieldValue = getFieldValue(schemaMap, conditionArray(i)._1, row)
      if (fieldValue == null) {
        hasNull = true
      }
    }
    if (hasNull) {
      (0, null.asInstanceOf[String], null.asInstanceOf[Long])
    } else {
      val wherePartSql = getWherePartSql(cc.connectionUrl, conditionArray)

      var ps: PreparedStatement = null
      var conn: Connection = null
      try {
        conn = DbConnection.getConnection(cc)
        val sql = selectPartSql + wherePartSql
        logInfo(sql)
        ps = conn.prepareStatement(sql)
        for (i <- conditionArray.indices) {
          val fieldValue = getFieldValue(schemaMap, conditionArray(i)._1, row)
          ps.setObject(i + 1, fieldValue, SinkDbSchemaUtils.ums2dbType(SparkSchemaUtils.spark2umsType(schemaMap(conditionArray(i)._1).dataType)))
        }

        val rs: ResultSet = ps.executeQuery()
        val queryResult = if (rs.next()) {
          (1, rs.getString(1), rs.getLong(2))
        } else {
          (0, null.asInstanceOf[String], null.asInstanceOf[Long])
        }

        rs.close()
        queryResult
      } catch {
        case e: SQLTransientConnectionException =>
          DbConnection.resetConnection(cc)
          logError("SQLTransientConnectionException", e)
          throw e
        case e: Throwable =>
          logError("queryByCondition error ", e)
          throw e
      } finally {
        if (ps != null)
          try {
            ps.close()
          } catch {
            case e: Throwable => logError("ps.close", e)
          }
        if (null != conn)
          try {
            conn.close()
            conn == null
          } catch {
            case e: Throwable => logError("conn.close", e)
          }
      }
    }


  }

  def getFieldValue(schemaMap: Map[String, StructField], fieldName: String, row: Row): Any = {
    val fieldIndex = row.fieldIndex(fieldName)
    schemaMap(fieldName).dataType match {
      case StringType =>
        val strValue = row.getString(fieldIndex)
        if (strValue == null) {
          strValue
        } else strValue.trim
      case IntegerType => row.getInt(fieldIndex)
      case LongType => row.getLong(fieldIndex)
      case BooleanType => row.getBoolean(fieldIndex)
      case DateType => row.getDate(fieldIndex)
      case TimestampType => row.getTimestamp(fieldIndex)
      case _ => throw new UnsupportedOperationException(s"Unknown DataType: $schemaMap(fieldName).dataType")
    }
  }

  def getWherePartSql(jdbcUrl: String, condtionArray: Array[(String, String)]): String = {
    if (jdbcUrl.indexOf("mysql") > -1) {
      condtionArray.map { case (_, dbField) =>
        s"""`$dbField`=?"""
      }.mkString(" and ")
    } else {
      condtionArray.map { case (_, dbField) =>
        s""""$dbField"=?"""
      }.mkString(" and ")
    }
  }

  def getSelectPartSql(qcp: QueryConditionParam): String = {
    val selectField = qcp.selectFieldName
    val tableName = qcp.tableName
    val umsId = UmsSysField.ID.toString
    if (qcp.jdbcUrl.indexOf("mysql") > -1) {
      s"""select `$selectField`,$umsId from `$tableName` where """
    } else {
      s"""select "$selectField" ,$umsId from "$tableName" where """
    }
  }


  def parseParam(param: String): QueryConditionParam = {

    val replaceParam = param.replaceAll("\\\\", "")
    logInfo(s"replaceParam:$replaceParam")
    val configJson = JSON.parseObject(replaceParam)
    val jdbcUrl = configJson.getString("jdbcUrl")
    val username = configJson.getString("username")
    val password = configJson.getString("password")
    val tableName = configJson.getString("tableName")
    val conditionArray = configJson.getJSONArray("conditions")
    val conditionList = ListBuffer.empty[Array[(String, String)]]
    for (i <- 0 until conditionArray.size()) {
      val paramJson = conditionArray.getJSONObject(i)
      val params: Array[(String, String)] = paramJson.keySet().toArray.map(k => {
        val ks = k.toString
        val v = paramJson.getString(k.toString)
        (ks.toLowerCase(), v.toLowerCase())
      })
      conditionList += params
    }

    val selectFieldName = configJson.getString("selectFieldName")

    val qcp = QueryConditionParam(jdbcUrl, username, password, tableName, conditionList, selectFieldName)
    qcp
  }
}


case class QueryConditionParam(jdbcUrl: String, username: String, password: String, tableName: String, conditions: ListBuffer[Array[(String, String)]], selectFieldName: String)
