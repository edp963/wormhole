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


package edp.wormhole.sinks.hbasesink

import edp.wormhole.ums.{UmsActiveType, UmsSysField}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.hashing.MurmurHash3
import scala.math._


object HbaseConstants {
  lazy val activeColBytes = Bytes.toBytes(UmsSysField.ACTIVE.toString)
  lazy val activeBytes = Bytes.toBytes(UmsActiveType.ACTIVE)
  lazy val inactiveBytes = Bytes.toBytes(UmsActiveType.INACTIVE)
  lazy val activeBytesLength = activeBytes.length
  lazy val inactiveBytesLength = inactiveBytes.length
  lazy val nullBytes = null
  lazy val nullBytesLength = 0
  lazy val versionOffset = 10000000000L
}




object RowkeyPattern extends Enumeration {
  type RowkeyPattern = Value

  val HASH = Value("hash")
  val REVERSE = Value("reverse")
  val VALUE = Value("value")
  val MD5=Value("mod")

  def rkReverse(s: String) = if (s == null) s else s.reverse

  def rkHash(s: String) = if (s == null) s else MurmurHash3.stringHash(s)
  def rkMod(s:String, p:String)={
    val patternList: Array[String] =p.split("_")
    val modNum=abs(MurmurHash3.stringHash(s)%patternList(1).toInt).toString
    val resLen=patternList(2).toInt
    val modNumLength=modNum.split("").length
    val subLength=if(resLen<=modNumLength) patternList(2).toInt else modNumLength
    val subStr=modNum.substring(0,subLength)
    if (subStr.split("").length<resLen){
      val minusVal=resLen-subStr.split("").length
      var zeroStr=""
      for(i<-0 until minusVal) zeroStr+="0"
      zeroStr+subStr
    }
    else subStr
  }
}
