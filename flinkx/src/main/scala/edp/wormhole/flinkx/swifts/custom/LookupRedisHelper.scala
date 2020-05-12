package edp.wormhole.flinkx.swifts.custom


import com.alibaba.fastjson.JSON
import edp.wormhole.dbdriver.redis.JedisConnection
import edp.wormhole.flinkx.swifts.LookupHelper
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.swifts.ConnectionMemoryStorage
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.log4j.Logger
import scala.collection.{mutable,Map}
import scala.collection.mutable.ListBuffer

object LookupRedisHelper {
  private val logger = Logger.getLogger(this.getClass)

  def covertResultSet2Map(swiftsSql: SwiftsSql,
                          row: Row,
                          preSchemaMap: Map[String, (TypeInformation[_], Int)],
                          dataStoreConnectionsMap: Map[String, ConnectionConfig]): mutable.HashMap[String, ListBuffer[Array[Any]]] = {

    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)
    val selectFields: Map[String, (String, String, Int)] = LookupHelper.getDbOutPutSchemaMap(swiftsSql)
    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[Any]]]
    try {
      val params: Seq[KVConfig] = connectionConfig.parameters.get
      val mode = params.filter(_.key == "mode").map(_.value).head
      val key: String = joinFieldsInRow(row, swiftsSql, preSchemaMap)
      //string
      val lookupValue = JedisConnection.get(connectionConfig.connectionUrl, connectionConfig.password, mode, key)

      val arrayBuf: Array[Any] = Array.fill(selectFields.size) {
        ""
      }
      selectFields.foreach { case (name, (rename, dataType, index)) =>
        val value = parseModel(swiftsSql, lookupValue, name)
        arrayBuf(index) = if (value != null) {
          if (dataType == UmsFieldType.BINARY.toString) CommonUtils.base64byte2s(value.asInstanceOf[Array[Byte]])
          else FlinkSchemaUtils.s2TrueValue(FlinkSchemaUtils.s2FlinkType(dataType), value)
        } else null
      }

      if (!dataTupleMap.contains(key)) {
        dataTupleMap(key) = ListBuffer.empty[Array[Any]]
      }
      dataTupleMap(key) += arrayBuf
    }
    catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
    dataTupleMap
  }



  def joinFieldsInRow(row: Row,
                      swiftsSql: SwiftsSql,
                      preSchemaMap: Map[String, (TypeInformation[_], Int)]): String = {

    val joinbyFiledsArray = swiftsSql.sourceTableFields.get
    val joinbyFileds = if (joinbyFiledsArray(0).contains("(")) {
      val left = joinbyFiledsArray(0).toLowerCase.indexOf("(")
      val right = joinbyFiledsArray(0).toLowerCase.lastIndexOf(")")
      joinbyFiledsArray(0).substring(left + 1, right)
    } else {
      joinbyFiledsArray(0)
    }

    val keyFieldContentDesc: Array[(Boolean, Int, String)] = joinbyFileds.split("\\+").map(fieldName => {
      if (!fieldName.startsWith("'")) {
        (true, preSchemaMap(fieldName)._2, preSchemaMap(fieldName)._1.toString())
      } else {
        (false, 0, fieldName.substring(1, fieldName.length - 1))
      }
    })

    val key: String = keyFieldContentDesc.map(fieldDesc => {
      if (fieldDesc._1) {
        if (row.getField(fieldDesc._2) == null) "N/A" else row.getField(fieldDesc._2)
      } else {
        fieldDesc._3
      }
    }).mkString("")
    key
  }

  def parseModel(swiftsSql: SwiftsSql, lookupValue: String, name: String): String ={
    if (swiftsSql.sql.indexOf("(json)") < 0) {
      lookupValue
    } else {
      if (lookupValue == null) {
        null
      } else {
        val json = JSON.parseObject(lookupValue)
        json.getString(name)
      }
    }
  }
}

