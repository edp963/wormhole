package edp.wormhole.flinkx.udf

import edp.wormhole.flinkx.util.FlinkSchemaUtils
import org.apache.flink.table.api.scala.StreamTableEnvironment


object UdfRegister {
  def register(udfName: String, udfClassFullname: String, tableEnv: StreamTableEnvironment) {
    //loadJar("file:D:/work/project/udftest/target/udf.test.1.0.0-jar-with-dependencies.jar")
    /*val clazz = Class.forName(udfClassFullname)
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
    }*/
    val (obj, method) = UdfUtils.getObjectAndMethod(udfName, udfClassFullname)
    val returnDataType = UdfUtils.convertFlinkType(method.getReturnType.getName)
    //if (!FlinkSchemaUtils.udfSchemaMap.contains(udfName))
    //val udfNameLower = udfName.toLowerCase()
    FlinkSchemaUtils.udfSchemaMap += udfName -> returnDataType
    val paramCount = method.getParameterCount
    paramCount match {
      case 0 => tableEnv.registerFunction(udfName, new UdfProxy0(udfName, udfClassFullname))
      case 1 => tableEnv.registerFunction(udfName, new UdfProxy1(udfName, udfClassFullname))
      case 2 => tableEnv.registerFunction(udfName, new UdfProxy2(udfName, udfClassFullname))
      case 3 => tableEnv.registerFunction(udfName, new UdfProxy3(udfName, udfClassFullname))
      case 4 => tableEnv.registerFunction(udfName, new UdfProxy4(udfName, udfClassFullname))
      case 5 => tableEnv.registerFunction(udfName, new UdfProxy5(udfName, udfClassFullname))
      case 6 => tableEnv.registerFunction(udfName, new UdfProxy6(udfName, udfClassFullname))
      case 7 => tableEnv.registerFunction(udfName, new UdfProxy7(udfName, udfClassFullname))
      case 8 => tableEnv.registerFunction(udfName, new UdfProxy8(udfName, udfClassFullname))
      case 9 => tableEnv.registerFunction(udfName, new UdfProxy9(udfName, udfClassFullname))
      case 10 => tableEnv.registerFunction(udfName, new UdfProxy10(udfName, udfClassFullname))
      case 11 => tableEnv.registerFunction(udfName, new UdfProxy11(udfName, udfClassFullname))
      case 12 => tableEnv.registerFunction(udfName, new UdfProxy12(udfName, udfClassFullname))
      case 13 => tableEnv.registerFunction(udfName, new UdfProxy13(udfName, udfClassFullname))
      case 14 => tableEnv.registerFunction(udfName, new UdfProxy14(udfName, udfClassFullname))
      case 15 => tableEnv.registerFunction(udfName, new UdfProxy15(udfName, udfClassFullname))
      case 16 => tableEnv.registerFunction(udfName, new UdfProxy16(udfName, udfClassFullname))
      case 17 => tableEnv.registerFunction(udfName, new UdfProxy17(udfName, udfClassFullname))
      case 18 => tableEnv.registerFunction(udfName, new UdfProxy18(udfName, udfClassFullname))
      case 19 => tableEnv.registerFunction(udfName, new UdfProxy19(udfName, udfClassFullname))
      case 20 => tableEnv.registerFunction(udfName, new UdfProxy20(udfName, udfClassFullname))
      case 21 => tableEnv.registerFunction(udfName, new UdfProxy21(udfName, udfClassFullname))
      case 22 => tableEnv.registerFunction(udfName, new UdfProxy22(udfName, udfClassFullname))
    }
  }

  /*private def loadJar(path: String): Unit = {
    val url = new URL(path)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)
  }*/
}
