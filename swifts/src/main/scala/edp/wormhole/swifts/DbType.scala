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


package edp.wormhole.swifts

object DbType {
  def convert(dataTytpe: String): String = dataTytpe.toUpperCase match {
      //mysql type
    case "TIMESTAMP" => "datetime"
    case "DATE" => "date"
    case "DATETIME" => "datetime"
    case "DECIMAL" => "decimal"
    case "DECIMAL UNSIGNED" => "decimal"
    case "SMALLINT" => "int"
    case "SMALLINT UNSIGNED" => "int"
    case "TINYINT" => "int"
    case "TINYINT UNSIGNED" => "int"
    case "INT" => "int"
    case "INT UNSIGNED" => "int"
    case "BIGINT" => "long"
    case "BIGINT UNSIGNED" => "long"
    case "FLOAT" => "float"
    case "DOUBLE" => "double"
    case "VARCHAR" => "string"
    case "TEXT" => "string"
    case "LONGTEXT" => "string"
    case "BLOB" => "binary"
    case "LONGBLOB" => "binary"
    case "BINARY" => "binary"
    case "CHAR" => "string"
    case "JSON" => "string"
    //kudu
    case "STRING" => "string"

    //oracle
    case "VARCHAR2" => "string"
    case "NVARCHAR2" => "string"
    case "INTEGER" => "int"
    case "CLOB" => "string"
    case "RAW" => "binary"
    case "NUMBER" => "decimal"
    case "LONG RAW" => "binary"

      //cassandra
    case "JAVA.MATH.BIGDECIMAL" => "decimal"
    case "JAVA.SQL.TIMESTAMP" => "datetime"
    case "JAVA.SQL.DATE" => "date"
    case "JAVA.LANG.LONG" => "long"
    case "JAVA.LANG.FLOAT" => "float"
    case "JAVA.LANG.DOUBLE" => "double"
    case "JAVA.LANG.INTEGER" => "int"
    case "JAVA.LANG.STRING" => "string"

    //postgresql
    case "INT4" => "int"

    //ClickHouse
    case "INT8"  => "int"
    case "INT16" => "int"
    case "INT32" => "long"
    case "INT64" => "long"
    case "UINT8" => "int"
    case "UINT16"=> "long"
    case "UINT32"=> "long"
    //case "UINT64"=> "long"

    case unknown =>
      throw new Exception("unknown type:"+unknown)
  }
}
