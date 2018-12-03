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


package edp.rider.monitor

import java.io.File

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.monitor.JsonFileType.JsonFileType


object JsonFileType extends Enumeration {
  type JsonFileType = Value

  val ESMAXFLOW = Value("/conf/EsSearchMaxValueFlow.json")
  val ESMAXSTREAM = Value("/conf/EsSearchMaxValueStream.json")
  val ESDELETED = Value("/conf/EsDeleteByQueryHistory.json")
  val GRAFANACREATE = Value("/conf/GrafanaDashboard.json")
  val ESCREATEINDEX = Value("/conf/EsCreateIndex.json")
  val ESSTREAM=Value("/conf/EsSearchStream.json")
  val ESFLOW=Value("/conf/EsSearchFlow.json")
  def getValue(JFType: JsonFileType) = JFType.toString
}


object ReadJsonFile extends RiderLogger {

  def getMessageFromJson(JFType: JsonFileType): String = {
    var msg: String = ""
    val dir: String = RiderConfig.riderRootPath + JsonFileType.getValue(JFType)
    try {
      val lines: Iterator[String] = scala.io.Source.fromFile(new File(dir)).getLines
      if (lines.nonEmpty) {
        msg = lines.mkString
      }
    } catch {
      case e: Exception =>
        riderLogger.error(s"Failed to get the message of Grafana dashboard from $dir", e)
    }
    msg
  }

}
