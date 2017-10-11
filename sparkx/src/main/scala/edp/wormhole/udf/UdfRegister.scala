/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package edp.wormhole.udf

import java.lang.reflect.Method
import java.net.{URI, URL, URLClassLoader}

import edp.wormhole.spark.log.EdpLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._

object UdfRegister extends EdpLogging {

  def register(udfName: String, udfClassFullname: String, udfJarPath: String, session: SparkSession) {
    //    if (!jarPathSet.contains(udfJarPath)) {
    synchronized {
      session.sparkContext.addJar(udfJarPath)
      loadJar(udfJarPath)
      //      jarPathSet += udfJarPath
      //    }
      val clazz = Class.forName(udfClassFullname)
      val method = {
        val methods = clazz.getDeclaredMethods
        var callMethod: Method = null
        for (i <- methods.indices) {
          val m: Method = methods(i)
          if (m.getName.equals("udf")) {
            callMethod = m
          }
        }
        callMethod
      }
      val returnDataType = UdfUtils.convertSparkType(method.getReturnType.getName)
      val paramCount = method.getParameterCount
      if (session.sessionState.functionRegistry.functionExists(udfName)) session.sessionState.functionRegistry.dropFunction(udfName)

      registerUdf(paramCount, session, udfName, udfClassFullname, returnDataType)
    }
  }

  private def loadJar(path: String): Unit = {
    val url = new URI(path).toURL
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val loaderMethod = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    loaderMethod.setAccessible(true)
    loaderMethod.invoke(classLoader, url)
  }

  private def registerUdf(paramCount: Int, session: SparkSession, udfName: String, udfClassName: String, returnDataType: DataType): Unit = {

    paramCount match {
      case 1 =>
        val f = new Function1[Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s: Object): Any = method.invoke(o, s)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 2 =>
        val f = new Function2[Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object): Any = method.invoke(o, s1, s2)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 3 =>
        val f = new Function3[Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object): Any = method.invoke(o, s1, s2, s3)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 4 =>
        val f = new Function4[Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object): Any = method.invoke(o, s1, s2, s3, s4)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 5 =>
        val f = new Function5[Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object): Any = method.invoke(o, s1, s2, s3, s4, s5)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 6 =>
        val f = new Function6[Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 7 =>
        val f = new Function7[Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 8 =>
        val f = new Function8[Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 9 =>
        val f = new Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 10 =>
        val f = new Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 11 =>
        val f = new Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 12 =>
        val f = new Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 13 =>
        val f = new Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 14 =>
        val f = new Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 15 =>
        val f = new Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 16 =>
        val f = new Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 17 =>
        val f = new Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 18 =>
        val f = new Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 19 =>
        val f = new Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 20 =>
        val f = new Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 21 =>
        val f = new Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
      case 22 =>
        val f = new Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
          lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)

          def apply(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object, s22: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22)
        }
        session.sessionState.functionRegistry.registerFunction(
          udfName,
          (e: Seq[Expression]) => ScalaUDF(f, returnDataType, e))
    }
  }

}
