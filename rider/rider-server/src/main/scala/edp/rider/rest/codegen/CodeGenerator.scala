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


package edp.rider.rest.codegen

import java.io.File
import edp.rider.RiderStarter.modules
import scala.language.postfixOps

object CodeGenerator extends App {

  val slickDriver = "slick.jdbc.MySQLProfile"
  val jdbcDriver = modules.config.getString("mysql.db.driver")
  val url = modules.config.getString("mysql.db.url")
  val outputFolder = new File(getClass().getClassLoader().getResource("").toURI()).getPath
  val pkg = "codegen"
  val user = modules.config.getString("mysql.db.user")
  val password = modules.config.getString("mysql.db.password")

  slick.codegen.SourceCodeGenerator.main(
    Array(slickDriver, jdbcDriver, url, outputFolder, pkg, user, password)
  )

  println("success")
}
