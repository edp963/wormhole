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


package edp.mad.util

import java.io.File
import org.apache.log4j.Logger

object ReadJsonFile {
  private val logger = Logger.getLogger(this.getClass)

  def getMessageFromJson(rootPath: String, filePath: String): String = {
    var msg: String = ""
    val dir: String = s"${rootPath}${filePath}"
    try {
      val lines: Iterator[String] = scala.io.Source.fromFile(new File(dir)).getLines
      if (lines.nonEmpty) {
        msg = lines.mkString
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get the message of Grafana dashboard from $dir", e)
    }
    msg
  }
}
