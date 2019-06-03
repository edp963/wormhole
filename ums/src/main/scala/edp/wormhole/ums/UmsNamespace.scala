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


package edp.wormhole.ums

import edp.wormhole.ums.UmsDataSystem.UmsDataSystem

import scala.collection.mutable.ArrayBuffer

case class UmsNamespace(dataSys: UmsDataSystem,
                        instance: String = "default",
                        database: String = "default",
                        table: String,
                        version: String,
                        databasePar: String = "0",
                        tablePar: String = "0")

object UmsNamespace {
  def apply(ns: String): UmsNamespace = {
    assert(ns != null && ns.contains('.'), "Invalid namespace: " + ns)

    val array = ns.split("\\.")
    assert(array.length == 7, "Invalid namespace: " + ns)

    UmsNamespace(UmsDataSystem.dataSystem(array(0)), array(1), array(2), array(3), array(4), array(5), array(6))
  }

  def applyFuzzyMatch(ns: String): UmsNamespace = {
    assert(ns != null, "Invalid namespace: " + ns)

    val array = ns.split("\\.")
    val buffer = ArrayBuffer[String]()

    array.copyToBuffer(buffer)
    for (_ <- array.length until 7)
      buffer.append("")

    UmsNamespace(UmsDataSystem.dataSystem(buffer(0)), buffer(1), buffer(2), buffer(3), buffer(4), buffer(5), buffer(6))

  }

  private def toStringWithDelimiter(delimiter: String, ns: UmsNamespace) = {
    List(ns.dataSys.toString, ns.instance, ns.database, ns.table, ns.version, ns.databasePar, ns.tablePar).mkString(delimiter)
  }

  def namespaceString(ns: UmsNamespace) = toStringWithDelimiter(".", ns)

  def namespacePath(ns: UmsNamespace) = toStringWithDelimiter("/", ns)
}

object UmsDataSystem extends Enumeration {
  type UmsDataSystem = Value
  val PARQUET = Value("parquet")
  val CASSANDRA = Value("cassandra")
  val MYSQL = Value("mysql")
  val ORACLE = Value("oracle")
  val POSTGRESQL = Value("postgresql")
  val H2 = Value("h2")
  val HBASE = Value("hbase")
  val ES = Value("es")
  val HDFS = Value("hdfs")
  val LOG = Value("log")
  val VERTICA = Value("vertica")
  //val SWIFTS = Value("swifts")
  val MONGODB = Value("mongodb")
  val PHOENIX = Value("phoenix")
  val KAFKA = Value("kafka")
  val REDIS = Value("redis")
  val KUDU = Value("kudu")
  val GREENPLUM = Value("greenplum")
  val UNKONWN = Value("")

  def dataSystem(s: String) = UmsDataSystem.withName(s.toLowerCase)
}

object Rdbms extends Enumeration {
  type Rdbms = Value

  val MYSQL = Value("mysql")
  val ORACLE = Value("oracle")
  val POSTGRESQL = Value("postgresql")
  val H2 = Value("h2")
  val PHOENIX = Value("phoenix")

  def rdbms(s: String) = Rdbms.withName(s.toLowerCase)
}

object RdbmsDriver extends Enumeration {
  type RdbmsDriver = Value

  val MYSQL_DRIVER = Value("com.mysql.jdbc.Driver")
  val ORACLE_DRIVER = Value("oracle.jdbc.driver.OracleDriver")
  val POSTGRESQL_DRIVER = Value("org.postgresql.Driver")
  val H2_DRIVER = Value("org.h2.Driver")
  val PHOENIX_DRIVER = Value("org.apache.phoenix.jdbc.PhoenixDriver")

  def driverString(url: String) =
    if (url.contains(Rdbms.MYSQL.toString)) MYSQL_DRIVER.toString
    else if (url.contains(Rdbms.ORACLE.toString)) ORACLE_DRIVER.toString
    else if (url.contains(Rdbms.POSTGRESQL.toString)) POSTGRESQL_DRIVER.toString
    else if (url.contains(Rdbms.H2.toString)) H2_DRIVER.toString
    else if (url.contains(Rdbms.PHOENIX.toString)) PHOENIX_DRIVER.toString
    else null

  def isUrlValid(url: String) =
    url.contains(Rdbms.MYSQL.toString) ||
      url.contains(Rdbms.ORACLE.toString) ||
      url.contains(Rdbms.POSTGRESQL.toString) ||
      url.contains(Rdbms.H2.toString) ||
      url.contains(Rdbms.PHOENIX.toString)
}

object NoSql extends Enumeration {
  type NoSql = Value

  val HBASE = Value("hbase")
  val ES = Value("es")
  val CASSANDRA = Value("cassandra")
  val MONGODB = Value("mongodb")
  val REDIS = Value("redis")
}

object NewSql extends Enumeration {
  type NewSql = Value

}
