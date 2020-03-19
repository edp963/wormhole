package edp.wormhole.sparkextension.udf

import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._

import scala.collection.mutable

object UdfRegister {

  lazy val jarPathMap = mutable.HashMap.empty[String, (Any, Method)]

  def getObjectAndMethod(udfName: String, className: String): (Any, Method) = {
    if (!jarPathMap.contains(udfName)) {
      val clazz = Class.forName(className)
      val o: Any = clazz.newInstance()
      val methods = clazz.getMethods
      var callMethod: Method = null
      for (i <- methods.indices) {
        val m: Method = methods(i)
        if (m.getName.equals(udfName)) {
          callMethod = m
        }
      }
      jarPathMap(udfName) = (o, callMethod)
    }
    jarPathMap(udfName)
  }

  def convertSparkType(returnClassName: String) = {
    returnClassName match {
      case "int" => IntegerType
      case "java.lang.Integer" => IntegerType
      case "long" => LongType
      case "java.lang.Long" => LongType
      case "float" => FloatType
      case "java.lang.Float" => FloatType
      case "double" => DoubleType
      case "java.lang.Double" => DoubleType
      case "boolean" => BooleanType
      case "java.lang.Boolean" => BooleanType
      case "java.lang.String" => StringType
      case "java.math.BigDecimal" => DecimalType.SYSTEM_DEFAULT
      case "java.util.Date" => DateType
      case "java.sql.Date" => DateType
      case "java.sql.Timestamp" => TimestampType
      case "java.security.Timestamp" => TimestampType
      case "com.sun.jmx.snmp.Timestamp" => TimestampType
      case _ => BinaryType
    }
  }

  def removeUdf(udfName: String): Unit = {
    jarPathMap.remove(udfName)
  }


  def register(udfName: String, udfClassFullname: String, udfJarPath: String, session: SparkSession, ifLoadJar: Boolean) {
    //    if (!jarPathSet.contains(udfJarPath)) {
    synchronized {
      if (ifLoadJar) {
        session.sparkContext.addJar(udfJarPath)
        loadJar(udfJarPath)
      }
      //      jarPathSet += udfJarPath
      //    }
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
      val returnDataType = convertSparkType(method.getReturnType.getName)
      val paramCount = method.getParameterCount
      if (session.sessionState.functionRegistry.functionExists(udfName)) session.sessionState.functionRegistry.dropFunction(udfName)

      registerUdf(paramCount, session, udfName, udfClassFullname, returnDataType)
    }
  }

  private def loadJar(path: String): Unit = {
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    } catch {
      case e: Throwable => println(e.getMessage)
    }
    val url = new URL(path)
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)

  }

  private def registerUdf(paramCount: Int, session: SparkSession, udfName: String, udfClassName: String, returnDataType: DataType): Unit = {

    paramCount match {
      case 0 =>
        val f = new Function0[Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(): Any = method.invoke(o)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 1 =>
        val f = new Function1[Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s: Object): Any = method.invoke(o, s)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 2 =>
        val f = new Function2[Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object): Any = method.invoke(o, s1, s2)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 3 =>
        val f = new Function3[Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object): Any = method.invoke(o, s1, s2, s3)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 4 =>
        val f = new Function4[Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object): Any = method.invoke(o, s1, s2, s3, s4)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 5 =>
        val f = new Function5[Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object): Any = method.invoke(o, s1, s2, s3, s4, s5)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 6 =>
        val f = new Function6[Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 7 =>
        val f = new Function7[Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 8 =>
        val f = new Function8[Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 9 =>
        val f = new Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 10 =>
        val f = new Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 11 =>
        val f = new Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 12 =>
        val f = new Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 13 =>
        val f = new Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 14 =>
        val f = new Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 15 =>
        val f = new Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 16 =>
        val f = new Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 17 =>
        val f = new Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 18 =>
        val f = new Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 19 =>
        val f = new Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 20 =>
        val f = new Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 21 =>
        val f = new Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 22 =>
        val f = new Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object, s22: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
    }
  }

}
