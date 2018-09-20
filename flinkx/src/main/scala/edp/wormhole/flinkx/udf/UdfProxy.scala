package edp.wormhole.flinkx.udf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.ScalarFunction

class UdfProxy(paramCount: Int, udfName: String, udfClassName: String, returnDataType: TypeInformation[_]) extends ScalarFunction {
  lazy val (o, method) = UdfUtils.getObjectAndMethod(udfName, udfClassName)
  //def eval(arg: Object *): Any = method.invoke(o, arg)
  def eval(): Any = if(paramCount == 0) method.invoke(o) else None
  def eval(s: Object): Any = if(paramCount == 1) method.invoke(o, s) else None
  def eval(s1: Object, s2: Object): Any = if(paramCount == 2) method.invoke(o, s1, s2) else None
  def eval(s1: Object, s2: Object, s3: Object): Any = if(paramCount == 3) method.invoke(o, s1, s2, s3) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object): Any = if(paramCount == 4) method.invoke(o, s1, s2, s3, s4) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object): Any = if(paramCount == 5) method.invoke(o, s1, s2, s3, s4, s5) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object): Any = if(paramCount == 6) method.invoke(o, s1, s2, s3, s4, s5, s6) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object): Any = if(paramCount == 7) method.invoke(o, s1, s2, s3, s4, s5, s6, s7) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object): Any = if(paramCount == 8) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object): Any = if(paramCount == 9) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object): Any = if(paramCount == 10) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object): Any = if(paramCount == 11) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object): Any = if(paramCount == 12) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object): Any = if(paramCount == 13) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object): Any = if(paramCount == 14) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object): Any = if(paramCount == 15) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object): Any = if(paramCount == 16) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object): Any = if(paramCount == 17) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object): Any = if(paramCount == 18) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17, s18) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object): Any = if(paramCount == 19) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17, s18, s19) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object): Any = if(paramCount == 20) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17, s18, s19, s20) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object): Any = if(paramCount == 21) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21) else None
  def eval(s1: Object, s2: Object, s3: Object, s4: Object, s5: Object, s6: Object, s7: Object, s8: Object, s9: Object, s10: Object, s11: Object, s12: Object, s13: Object, s14: Object, s15: Object, s16: Object, s17: Object, s18: Object, s19: Object, s20: Object, s21: Object, s22: Object): Any = if(paramCount == 22) method.invoke(o, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22) else None
}
