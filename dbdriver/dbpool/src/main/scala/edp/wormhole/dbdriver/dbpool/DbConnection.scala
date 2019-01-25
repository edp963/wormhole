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


package edp.wormhole.dbdriver.dbpool

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import edp.wormhole.util.config.ConnectionConfig

import scala.collection.mutable


object DbConnection extends Serializable {
  lazy val datasourceMap: mutable.HashMap[(String,String), HikariDataSource] = new mutable.HashMap[(String,String), HikariDataSource]

  def getConnection(jdbcConfig: ConnectionConfig): Connection = {
    val tmpJdbcUrl = jdbcConfig.connectionUrl.toLowerCase
    val tmpUsername = jdbcConfig.username.getOrElse("").toLowerCase
    if (!datasourceMap.contains((tmpJdbcUrl,tmpUsername)) || datasourceMap((tmpJdbcUrl,tmpUsername)) == null) {
      synchronized {
        if (!datasourceMap.contains((tmpJdbcUrl,tmpUsername)) || datasourceMap((tmpJdbcUrl,tmpUsername)) == null) {
          initJdbc(jdbcConfig)
        }
      }
    }
    datasourceMap((tmpJdbcUrl,tmpUsername)).getConnection
  }

  private def initJdbc(jdbcConfig: ConnectionConfig): Unit = {
    val jdbcUrl = jdbcConfig.connectionUrl
    val username = jdbcConfig.username
    val password = jdbcConfig.password
    val kvConfig = jdbcConfig.parameters
    println(jdbcUrl)
    val config = new HikariConfig()
    val tmpJdbcUrl = jdbcUrl.toLowerCase
    if (tmpJdbcUrl.indexOf("mysql") > -1) {
      println("mysql")
      config.setConnectionTestQuery("SELECT 1")
      config.setDriverClassName("com.mysql.jdbc.Driver")
    } else if (tmpJdbcUrl.indexOf("oracle") > -1) {
      println("oracle")
      config.setConnectionTestQuery("SELECT 1 from dual")
      config.setDriverClassName("oracle.jdbc.driver.OracleDriver")
    } else if (tmpJdbcUrl.indexOf("postgresql") > -1) {
      println("postgresql")
      config.setDriverClassName("org.postgresql.Driver")
    } else if (tmpJdbcUrl.indexOf("sqlserver") > -1) {
      println("sqlserver")
      config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    } else if (tmpJdbcUrl.indexOf("h2") > -1) {
      println("h2")
      config.setDriverClassName("org.h2.Driver")
    } else if (tmpJdbcUrl.indexOf("phoenix") > -1) {
      println("hbase phoenix")
      config.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver")
    } else if (tmpJdbcUrl.indexOf("cassandra") > -1) {
      println("cassandra")
      config.setDriverClassName("com.github.adejanovski.cassandra.jdbc.CassandraDriver")
    } else if (tmpJdbcUrl.indexOf("mongodb") > -1) {
      println("mongodb")
      config.setDriverClassName("mongodb.jdbc.MongoDriver")
    } else if (tmpJdbcUrl.indexOf("sql4es") > -1) {
      println("elasticSearch")
      config.setDriverClassName("nl.anchormen.sql4es.jdbc.ESDriver")
    } else if (tmpJdbcUrl.indexOf("vertica") > -1) {
      println("vertical")
      config.setDriverClassName("com.vertica.jdbc.Driver")
//    } else if (tmpJdbcUrl.indexOf("greenplum") > -1) {
//      println("greenplum")
//      config.setDriverClassName("com.pivotal.jdbc.GreenplumDriver")
    }


    config.setUsername(username.getOrElse(""))
    config.setPassword(password.getOrElse(""))
    config.setJdbcUrl(jdbcUrl)
    //    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(1)

    if(tmpJdbcUrl.indexOf("sql4es") < 0){
      config.addDataSourceProperty("cachePrepStmts", "true")
      config.addDataSourceProperty("maximumPoolSize", "1")
      config.addDataSourceProperty("validationTimeout", "3000")
      config.addDataSourceProperty("prepStmtCacheSize", "250")
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    }


    if(kvConfig.nonEmpty)  kvConfig.get.foreach(kv => config.addDataSourceProperty(kv.key, kv.value))

    val ds: HikariDataSource = new HikariDataSource(config)
    println(tmpJdbcUrl + "$$$$$$$$$$$$$$$$$" + ds.getUsername + " " + ds.getPassword)
    datasourceMap((tmpJdbcUrl,username.getOrElse("").toLowerCase)) = ds
  }

  def resetConnection(jdbcConfig: ConnectionConfig):Unit = {
    shutdownConnection(jdbcConfig.connectionUrl.toLowerCase,jdbcConfig.username.orNull)
    //    datasourceMap -= jdbcUrl
    getConnection(jdbcConfig).close()
  }

  def shutdownConnection(jdbcUrl: String,username:String):Unit = {
    val tmpJdbcUrl = jdbcUrl.toLowerCase
    val tmpUsername = if(username==null) "" else username.toLowerCase
    datasourceMap((tmpJdbcUrl,tmpUsername)).close()
    datasourceMap -= ((tmpJdbcUrl,tmpUsername))
  }

}
