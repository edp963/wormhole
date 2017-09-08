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


package edp.wormhole.ums.toolkit

import java.io.{File, FileWriter}
import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums.{Rdbms, UmsSchemaUtils}
import edp.wormhole.ums.UmsSysField._

import scala.io.Source

object UmsSchema2Ddl extends App {

  val jsonSchema = args(0)
  val rdbms = Rdbms.rdbms(args(1))
  val tableName = args(2)
  val keys = args(3)
  val outputPath = args(4)

  val schema = UmsSchemaUtils.toUmsSchema(jsonSchema)
  val schemaFields = schema.fields_get
  val baseFields = schemaFields.filterNot(field => Set(OP.toString).contains(field.name))
  val fields = baseFields.map(umsField => {
    val notNUllStr = if (!umsField.nullable_get) " NOT NULL" else ""
    umsField.`type` match {
      case STRING => if (rdbms == Rdbms.ORACLE) s"${umsField.name} VARCHAR2(255)$notNUllStr" else s"${umsField.name} VARCHAR(255)$notNUllStr"
      case INT => s"${umsField.name} INT$notNUllStr"
      case LONG => if (rdbms == Rdbms.ORACLE) s"${umsField.name} NUMBER$notNUllStr" else s"${umsField.name} BIGINT$notNUllStr"
      case FLOAT => s"${umsField.name} FLOAT $notNUllStr"
      case DOUBLE => if (rdbms == Rdbms.ORACLE) s"${umsField.name} NUMBER$notNUllStr" else s"${umsField.name} DOUBLE$notNUllStr"
      case BOOLEAN => if (rdbms == Rdbms.ORACLE) s"${umsField.name} NUMBER(5,0)$notNUllStr" else s"${umsField.name} TINYINT$notNUllStr"
      case DATE => s"${umsField.name} DATE$notNUllStr"
      case DATETIME => if (rdbms == Rdbms.ORACLE) s"${umsField.name} DATE$notNUllStr" else s"${umsField.name} DATETIME$notNUllStr"
      case DECIMAL =>if (rdbms == Rdbms.ORACLE) s"${umsField.name} NUMBER$notNUllStr" else s"${umsField.name} DECIMAL(30,2)$notNUllStr"
      case _ => ""
    }
  }).mkString(",")
  val umsActive = s"${ACTIVE.toString} INT"
  val primaryKey = s"PRIMARY KEY ($keys)"
  val createDdl = s"CREATE TABLE $tableName ($fields,$umsActive,$primaryKey)ENGINE=InnoDB DEFAULT CHARSET=utf8;"
  val out = new FileWriter(outputPath, true)
  out.write(createDdl)
  out.flush()
  out.close()
}


object UserExample extends App {

  val file = new File(args(0))
  val jsonSchema = Source.fromFile(file).getLines().mkString("")
  val outputPath = args(1)   //  D:\a.txt
  val tableName = "table_name"
  val pk = ""
  val db = Rdbms.MYSQL.toString
  UmsSchema2Ddl.main(Array(jsonSchema, db, tableName, pk, outputPath))

}
