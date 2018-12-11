package edp.wormhole.flinkx.udf

import java.lang.reflect.Method

import edp.wormhole.flinkx.udf.UdafRegister.logger
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, TypeInformation}
import org.apache.flink.table.api.Types

import scala.collection.mutable

object UdfUtils {
  lazy val jarPathMap = mutable.HashMap.empty[String, (Any, Method)]

  def getObjectAndMethod(udfName: String, className: String): (Any, Method) = {
    if (!jarPathMap.contains(udfName)) {
      val clazz = Class.forName(className)
      val o: Any = clazz.newInstance()
      val methods = clazz.getMethods
      var callMethod: Method = null
      for (i <- methods.indices) {
        val m: Method = methods(i)
        if (m.getName.equals(udfName) && !m.getReturnType.getName.equals("java.lang.Object")) {
          callMethod = m
        }
      }
      if(callMethod != null) {
        jarPathMap(udfName) = (o, callMethod)
      } else {
        logger.info(className + "not contain the function of" + udfName)
        throw new Exception(className + "not contain the function of" + udfName)
      }
    }
    jarPathMap(udfName)
  }

  def convertFlinkType(returnClassName: String): TypeInformation[_] = {
    returnClassName match {
      case "int" => Types.INT
      case "java.lang.Integer" => Types.INT
      case "long" => Types.LONG
      case "java.lang.Long" => Types.LONG
      case "float" => Types.FLOAT
      case "java.lang.Float" => Types.FLOAT
      case "double" => Types.DOUBLE
      case "java.lang.Double" => Types.DOUBLE
      case "boolean" => Types.BOOLEAN
      case "java.lang.Boolean" => Types.BOOLEAN
      case "java.lang.String" => Types.STRING
      case "java.math.BigDecimal" => Types.DECIMAL
      case "java.util.Date" => Types.SQL_DATE
      case "java.sql.Date" => Types.SQL_DATE
      case "java.sql.Timestamp" => Types.SQL_TIMESTAMP
      case "java.security.Timestamp" => Types.SQL_TIMESTAMP
      case "com.sun.jmx.snmp.Timestamp" => Types.SQL_TIMESTAMP
      case "binary" => BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO
      //case "java.lang.Object" => Types.LONG
      case unknown =>
        throw new Exception("unknown type:" + unknown)
    }
  }
}
