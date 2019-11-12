package edp.wormhole.sparkx.swifts.custom.maidian

import java.util

import edp.wormhole.ums.{UmsFieldType, UmsSysField}
import org.apache.spark.sql.DataFrame

object MaidianCommon {

  def getFieldType(df: DataFrame): util.HashMap[String, DataType] = {
    val tableFieldMap: util.HashMap[String, DataType] = new util.HashMap[String, DataType]
    df.schema.foreach(field => {
      println(field.name.toLowerCase)
      val ft =
        if (field.dataType.typeName.toLowerCase == "timestamp") DataType.TIME
        else
          UmsFieldType.umsFieldType(field.dataType.typeName) match {
            case UmsFieldType.DECIMAL => DataType.NUMBER
            case UmsFieldType.LONG => DataType.NUMBER
            case UmsFieldType.DOUBLE => DataType.NUMBER
            case UmsFieldType.DECIMAL => DataType.NUMBER
            case UmsFieldType.FLOAT => DataType.NUMBER
            case UmsFieldType.INT => DataType.NUMBER
            case UmsFieldType.BOOLEAN => DataType.LOGIC
            case UmsFieldType.DATETIME => DataType.TIME
            case UmsFieldType.DATE => DataType.TIME
            case UmsFieldType.STRING => DataType.TEXT
            case _ => DataType.TEXT
          }
      tableFieldMap.put(field.name.toLowerCase, ft)
    })
    tableFieldMap
  }

}
