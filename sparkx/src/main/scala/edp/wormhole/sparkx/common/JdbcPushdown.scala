package edp.wormhole.sparkx.common

import java.sql.ResultSet

import edp.wormhole.ums.UmsField
import edp.wormhole.ums.UmsFieldType._
import org.apache.spark.sql._
object JdbcPushdown{


//  def jdbcDf(session: SparkSession, sql: String, namespace: String, jsonSchema: String, jdbcConfig:ConnectionConfig): DataFrame = {
//    try {
//      val fields = UmsSchemaUtils.toUmsSchema(jsonSchema).fields_get
//      val schema = ums2sparkSchema(fields)
//      val jdbcRdd: EdpJdbcRDD[Row] = new EdpJdbcRDD[Row](session.sparkContext,
//        jdbcConfig,
//        sql,
//        rs => getRow(rs, fields))
//      val df = session.createDataFrame(jdbcRdd, schema)
//      jdbcRdd.unpersist()
//      df
//    } catch {
//      case e: Throwable => logError("getDataFrame", e); throw e
//    }
//  }


  private def getRow(rs: ResultSet, fields: Seq[UmsField]): Row = {
    val results: Seq[Any] = fields.map(field => {

      val fieldContent: Any = field.`type` match {
        case STRING => rs.getString(field.name)
        case INT => rs.getInt(field.name)
        case LONG => rs.getLong(field.name)
        case FLOAT => rs.getFloat(field.name)
        case DOUBLE => rs.getDouble(field.name)
        case BOOLEAN => rs.getBoolean(field.name)
        case DATE => rs.getDate(field.name)
        case DATETIME => rs.getTimestamp(field.name)
        case DECIMAL => rs.getBigDecimal(field.name)
        case BINARY => rs.getBytes(field.name)
      }
      fieldContent
    })

    Row(results: _*)
  }

}
