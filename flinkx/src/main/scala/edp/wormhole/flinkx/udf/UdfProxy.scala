package edp.wormhole.flinkx.udf

import org.apache.flink.table.functions.ScalarFunction

class UdfProxy0(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(): Any = method.invoke(o)
}
class UdfProxy1(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s: Object): Any = method.invoke(o, s)
}

class UdfProxy2(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object):Any= method.invoke(o, s1, s2)
  //override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = returnDataType
}

class UdfProxy3(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object): Any = method.invoke(o, s1, s2, s3)
}

class UdfProxy4(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object): Any = method.invoke(o, s1, s2, s3, s4)
}

class UdfProxy5(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object): Any = method.invoke(o, s1, s2, s3, s4, s5)
}

class UdfProxy6(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6)
}

class UdfProxy7(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7)
}

class UdfProxy8(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8)
}

class UdfProxy9(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9)
}

class UdfProxy10(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
}

class UdfProxy11(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11)
}

class UdfProxy12(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12)
}

class UdfProxy13(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13)
}

class UdfProxy14(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
}

class UdfProxy15(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15)
}

class UdfProxy16(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
}

class UdfProxy17(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17)
}

class UdfProxy18(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18)
}

class UdfProxy19(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19)
}

class UdfProxy20(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20)
}

class UdfProxy21(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)
}

class UdfProxy22(udfName: String, udfClassName: String) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object, s22: Object): Any = method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22)
}