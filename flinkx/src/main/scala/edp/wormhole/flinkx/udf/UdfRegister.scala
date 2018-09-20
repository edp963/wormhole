package edp.wormhole.flinkx.udf

import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala.StreamTableEnvironment
import scala.collection.mutable


object UdfRegister {
  def register(udfName: String, udfClassFullname: String, tableEnv: StreamTableEnvironment) {
    //loadJar("file:D:/work/project/jar/udf.test.1.0.0-jar-with-dependencies.jar")
    val clazz = Class.forName(udfClassFullname)
    val method = {
      val methods = clazz.getMethods
      var callMethod: Method = null
      for (i <- methods.indices) {
        val m: Method = methods(i)
        if (m.getName.equals(udfName)) {
          callMethod = m
        }
      }
      callMethod
    }
    val returnDataType = UdfUtils.convertFlinkType(method.getReturnType.getName)
    //if (!FlinkSchemaUtils.udfSchemaMap.contains(udfName))
    FlinkSchemaUtils.udfSchemaMap += udfName -> returnDataType
    val paramCount = method.getParameterCount
    //registerUdf(paramCount, udfName, udfClassFullname, returnDataType, tableEnv)
    tableEnv.registerFunction(udfName, new UdfProxy(paramCount, udfName, udfClassFullname, returnDataType))
  }

  /*private def loadJar(path: String): Unit = {
    val url = new URL(path)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)
  }*/

}
