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


package edp.wormhole.util

import java.io.{File, PrintWriter}

object FileUtils extends FileUtils

trait FileUtils {
  def pfRight(path: String): String = {
    val right = if (path.endsWith("/")) path.dropRight(1) else path
    right
  }

  def pf(path: String): String = {
    val right = if (path.endsWith("/")) path.dropRight(1) else path
    val left = if (right.startsWith("/")) right.tail else right
    left
  }

  def projectRootPath(clazz: Class[_]): String = {
    val currentPath = clazz.getResource("").getPath
    val rootPath = pf(currentPath.substring(0, currentPath.indexOf("target")))
    s"/$rootPath"
  }

  def testResourcesPath(clazz: Class[_]): String = s"/${projectRootPath(clazz)}/src/test/resources"

  //  def testResourcesPath(clazz: Class[_]): String = {
  //    val currentPath = clazz.getResource("").getPath
  //    val rootPath = pf(currentPath.substring(0, currentPath.indexOf("target")))
  //    s"/$rootPath/src/test/resources"
  //  }

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}
