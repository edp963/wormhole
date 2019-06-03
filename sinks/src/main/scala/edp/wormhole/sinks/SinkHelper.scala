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


package edp.wormhole.sinks

import java.util.UUID

import edp.wormhole.ums.UmsFieldType.UmsFieldType
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

object SourceMutationType extends Enumeration {
  private lazy val logger = Logger.getLogger(this.getClass)
  type SourceMutationType = Value

  val INSERT_ONLY = Value("i")
  val I_U_D = Value("iud")
  val SPLIT_TABLE_IDU = Value("split_table_idu")//分表幂等
  val INSERT_INSERT = Value("i_i")
  val INSERT_UPDATE = Value("i_u")
  val UPDATE_INSERT = Value("u_i")
  val UPDATE_UPDATE = Value("u_u")
  val DELETE_INSERT = Value("d_i")
  val DELETE_UPDATE = Value("d_u")
  val NONE_INSERT = Value("none_i")
  val NONE_UPDATE = Value("none_u")

  def sourceMutationType(s: String): SourceMutationType = try {
    SourceMutationType.withName(s.toLowerCase)
  } catch {
    case e: Throwable =>
      logger.warn(s"SourceMutationType invalid string: $s")
      I_U_D
  }
}

//object RowKeyType extends Enumeration with EdpLogging {
//  type RowKeyType = Value
//
//  val USER = Value("user")
//  val SYSTEM = Value("system")
//}

object DbHelper {
  def removeFieldNames(allFieldNames: List[String], removeFn: String => Boolean): List[String] = allFieldNames.filterNot(removeFn)

  def removeOtherFieldNames(allFieldNames: List[String], retainFn: String => Boolean): List[String] = allFieldNames.filter(retainFn)
}

object _IDHelper{
  def get_Ids(tuple: Seq[String], _ids: Array[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): String = {

    val tmp_ids = ListBuffer.empty[String]
    if (_ids.nonEmpty ) {
      _ids.foreach(keyname => {
        val (index, _, _) = schemaMap(keyname)
        tmp_ids += tuple(index)
      })
      tmp_ids.mkString("_")
    } else UUID.randomUUID().toString
  }
}
