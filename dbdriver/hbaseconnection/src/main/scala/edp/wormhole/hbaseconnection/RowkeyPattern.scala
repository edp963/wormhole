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

import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.MD5Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.abs
import scala.util.hashing.MurmurHash3

object RowkeyPattern extends Enumeration {
  type RowkeyPattern = Value

  val HASH = Value("hash")
  val REVERSE = Value("reverse")
  val VALUE = Value("value")

  val MD5 = Value("md5")
  val SUFFIX = Value("suffix")
  val PREFIX = Value("prefix")
  val MOD = Value("mod")
  val SUB = Value("sub")
  val ABS = Value("abs")

  val MD5TMP = Value("md5tmp")

  def rkMD5(s: String): String = if (s == null) s else MD5Utils.getMD5String(s)

  def rkMod(s: Long, p: Long): Long = s % p

  def rkSub(s: String, len: Int): String = if (s == null) s else {
    val lenAbs = abs(len)
    val sLen = s.length
    if (sLen >= lenAbs) {
      if (len > 0) s.substring(0, lenAbs)
      else s.substring(sLen - lenAbs, sLen)
    } else s
  }

  def rkReverse(s: String): String = if (s == null) s else s.reverse

  def rkAbs(s: Long): Long = abs(s)

  def rkConcat(s1: String, s2: String): String = if (s1 == null || s2 == null) null else s1 + s2

  def rkHash(s: String): Long = if (s == null) 0 else MurmurHash3.stringHash(s)

  def rkModTmp(s: String, p: String): String = {
    //p=
    val patternList: Array[String] = p.split("_")
    val modNum = abs(MurmurHash3.stringHash(s) % patternList(1).toInt).toString
    val resLen = patternList(2).toInt
    val modNumLength = modNum.split("").length
    val subLength = if (resLen <= modNumLength) patternList(2).toInt else modNumLength
    val subStr = modNum.substring(0, subLength)
    if (subStr.split("").length < resLen) {
      val minusVal = resLen - subStr.split("").length
      var zeroStr = ""
      for (_ <- 0 until minusVal) zeroStr += "0"
      zeroStr + subStr
    }
    else subStr
  }
}

case class RowkeyPatternContent(patternType: String, fieldContent: String, patternList: ListBuffer[(String, String)])

object RowkeyPatternType extends Enumeration {
  type RowkeyPatternType = Value

  val EXPERSSION = Value("expression")
  val DELIMIER = Value("delimiter")

}

object RowkeyTool {

  def parse(joinContent: String): ListBuffer[RowkeyPatternContent] = {
    val joinGrp = joinContent.split("\\+").map(_.trim)
    val rowkeyPatternContentList = ListBuffer.empty[RowkeyPatternContent]
    joinGrp.foreach(oneFieldPattern => {
      var subPatternContent = oneFieldPattern
      val keyOpts = ListBuffer.empty[(String, String)]
      if (subPatternContent.contains("(")) {
        while (subPatternContent.contains("(")) {
          val firstIndex = subPatternContent.indexOf("(")
          val keyOpt = subPatternContent.substring(0, firstIndex).trim
          val lastIndex = subPatternContent.lastIndexOf(")")
          subPatternContent = subPatternContent.substring(firstIndex + 1, lastIndex)
          val param = if (subPatternContent.trim.endsWith(")")) null.asInstanceOf[String]
          else {
            if (subPatternContent.contains("(")) {
              val subLastIndex = subPatternContent.lastIndexOf(")", lastIndex)
              val part = subPatternContent.substring(subLastIndex + 1)
              subPatternContent = subPatternContent.substring(0, subLastIndex + 1)
              if (part.contains(",")) part.trim.substring(1)
              else null.asInstanceOf[String]
            } else if (subPatternContent.contains(",")) {
              val tmpIndex = subPatternContent.indexOf(",")
              val tmp = subPatternContent.substring(tmpIndex + 1)
              subPatternContent = subPatternContent.substring(0, tmpIndex)
              tmp
            } else null.asInstanceOf[String]
          }
          keyOpts += ((keyOpt.toLowerCase, param))
        }
        rowkeyPatternContentList += RowkeyPatternContent(RowkeyPatternType.EXPERSSION.toString, subPatternContent, keyOpts)
      } else {
        rowkeyPatternContentList += RowkeyPatternContent(RowkeyPatternType.DELIMIER.toString, subPatternContent.replace("'", "").trim, keyOpts)
      }
    })
    rowkeyPatternContentList

  }


  def generateOnerowKeyFieldsSchema(schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)], patternContentList: Seq[RowkeyPatternContent]): Seq[(Boolean, Int, String)] = {
    val keyFieldContentDesc = patternContentList.map(patternContent => {
      if (patternContent.patternType == RowkeyPatternType.EXPERSSION.toString) {
        (true, schemaMap(patternContent.fieldContent)._1, "")
      } else {
        (false, 0, patternContent.fieldContent)
      }
    })
    keyFieldContentDesc
  }

  def generateTupleKeyDatas(keyFieldsSchema: Seq[(Boolean, Int, String)], tuple: Seq[Any]): Seq[String] = {
    keyFieldsSchema.map(fieldDesc => {
      if (fieldDesc._1) {
        val value = tuple(fieldDesc._2)
        if (value != null) value.toString else "N/A"
      } else {
        fieldDesc._3
      }
    })
  }

  def generatePatternKey(keyDatas: Seq[String], patternContentList: Seq[RowkeyPatternContent]): String = {
    val keys = ListBuffer.empty[String]
    for (i <- patternContentList.indices) {
      if (patternContentList(i).patternType == RowkeyPatternType.EXPERSSION.toString) {
        var fieldContent = keyDatas(i).toString
        patternContentList(i).patternList.reverse.foreach(pattern => {
          val keyOpt = pattern._1
          val param = pattern._2
          fieldContent = if (keyOpt == RowkeyPattern.HASH.toString) RowkeyPattern.rkHash(fieldContent).toString
          else if (keyOpt == RowkeyPattern.MD5.toString) RowkeyPattern.rkMD5(fieldContent)
          else if (keyOpt == RowkeyPattern.ABS.toString) RowkeyPattern.rkAbs(fieldContent.toLong).toString
          else if (keyOpt == RowkeyPattern.SUFFIX.toString) RowkeyPattern.rkConcat(fieldContent,param.toString)
          else if (keyOpt == RowkeyPattern.PREFIX.toString) RowkeyPattern.rkConcat(param.toString,fieldContent)
          else if (keyOpt == RowkeyPattern.MOD.toString) {
            RowkeyPattern.rkMod(fieldContent.toLong, param.toLong).toString
          } else if (keyOpt == RowkeyPattern.SUB.toString) {
            RowkeyPattern.rkSub(fieldContent, param.toInt)
          } else if (keyOpt == RowkeyPattern.REVERSE.toString) RowkeyPattern.rkReverse(fieldContent)
          else fieldContent //RowkeyPattern.VALUE.toString
        })
        keys += fieldContent
      } else keys += keyDatas(i).toString
    }
    keys.mkString("")
  }

  def generateKeyFieldsSchema(originalData: Map[String,Integer], patternContentList: mutable.Seq[RowkeyPatternContent]): mutable.Seq[(Boolean, Int, String)] = {
    val keyFieldContentDesc: mutable.Seq[(Boolean, Int, String)] = patternContentList.map(patternContent => {
      if (patternContent.patternType == RowkeyPatternType.EXPERSSION.toString) {
        (true, originalData(patternContent.fieldContent).intValue, "")
      } else {
        (false, 0, patternContent.fieldContent)
      }
    })
    keyFieldContentDesc
  }
}
