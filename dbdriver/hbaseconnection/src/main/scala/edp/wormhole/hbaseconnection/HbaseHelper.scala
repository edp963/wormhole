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


package edp.wormhole.hbaseconnection

import edp.wormhole.ums.{UmsActiveType, UmsSysField}
import org.apache.hadoop.hbase.util.Bytes



object HbaseConstants {
  lazy val activeColBytes = Bytes.toBytes(UmsSysField.ACTIVE.toString)
  lazy val activeString = Bytes.toBytes(UmsActiveType.ACTIVE.toString)
  lazy val inactiveString = Bytes.toBytes(UmsActiveType.INACTIVE.toString)
  lazy val activeBytes = Bytes.toBytes(UmsActiveType.ACTIVE)
  lazy val inactiveBytes = Bytes.toBytes(UmsActiveType.INACTIVE)
  lazy val activeBytesLength = activeBytes.length
  lazy val inactiveBytesLength = inactiveBytes.length
  lazy val nullBytes = null
  lazy val nullBytesLength = 0
  lazy val versionOffset = 10000000000L
}


//object RowkeyPattern extends Enumeration {
//  type RowkeyPattern = Value
//
//  val HASH = Value("hash")
//  val REVERSE = Value("reverse")
//  val VALUE = Value("value")
//  //  val MD5=Value("mod")
//
//  val MD5 = Value("md5")
//  val MOD = Value("mod")
//  val SUB = Value("sub")
//  val ABS = Value("abs")
//
//  val MD5TMP = Value("md5tmp")
//
//  def rkMD5(s: String):String = if (s == null) s else MD5Utils.getMD5String(s)
//
//  def rkMod(s: Long, p: Long):Long = s % p
//
//  def rkSub(s:String,len:Int):String = if (s == null) s else {
//    val lenAbs = abs(len)
//    val sLen = s.length
//    if(sLen>=lenAbs){
//      if(len>0) s.substring(0,lenAbs)
//      else s.substring(sLen-lenAbs,sLen)
//    }else s
//  }
//
//  def rkReverse(s: String):String = if (s == null) s else s.reverse
//
//  def rkAbs(s: Long):Long = abs(s)
//
//  def rkHash(s: String):Long = if (s == null) 0 else MurmurHash3.stringHash(s)
//
//
//
//  def rkModTmp(s: String, p: String):String = {
//    //p=
//    val patternList: Array[String] = p.split("_")
//    val modNum = abs(MurmurHash3.stringHash(s) % patternList(1).toInt).toString
//    val resLen = patternList(2).toInt
//    val modNumLength = modNum.split("").length
//    val subLength = if (resLen <= modNumLength) patternList(2).toInt else modNumLength
//    val subStr = modNum.substring(0, subLength)
//    if (subStr.split("").length < resLen) {
//      val minusVal = resLen - subStr.split("").length
//      var zeroStr = ""
//      for (i <- 0 until minusVal) zeroStr += "0"
//      zeroStr + subStr
//    }
//    else subStr
//  }
//}
