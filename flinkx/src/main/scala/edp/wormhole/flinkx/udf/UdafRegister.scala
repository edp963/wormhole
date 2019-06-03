package edp.wormhole.flinkx.udf

import edp.wormhole.flinkx.util.FlinkSchemaUtils
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction
import org.apache.log4j.Logger
import org.apache.flink.streaming.api.scala._

object UdafRegister {
  val logger: Logger = Logger.getLogger(this.getClass)
  def register(udfName: String, udfClassFullName: String, tableEnv: StreamTableEnvironment): Unit = {

    //loadJar("file:D:/work/project/udftest/target/udf.test.1.0.0-jar-with-dependencies.jar")
    val (obj, method) = UdfUtils.getObjectAndMethod("getValue", udfClassFullName)

    val functionType = method.getReturnType.getName
    logger.info("functionType:" + functionType)
    val returnDataType = UdfUtils.convertFlinkType(method.getReturnType.getName)
    FlinkSchemaUtils.udfSchemaMap += udfName -> returnDataType

    tableEnv.registerFunction(udfName, obj.asInstanceOf[AggregateFunction[Object, Object]])
  }

  /*private def loadJar(path: String): Unit = {
    val url = new URL(path)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)
  }*/
}