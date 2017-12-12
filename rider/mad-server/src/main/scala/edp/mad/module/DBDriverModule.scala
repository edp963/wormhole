package edp.mad.module

/*
  slick 3.2.0 supported databases
DB2
Derby / JavaDB
H2
HSQLDB (HyperSQL)
Microsoft SQL Server
MySQL
Oracle
PostgreSQL
SQLite
*
*
* */

import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

trait DBDriverModule {
  val profile :JdbcProfile
  val dbConfig: DatabaseConfig[JdbcProfile]
  val db: JdbcProfile#Backend#Database

}

trait DBDriverModuleImpl extends DBDriverModule with ConfigOptionImpl {
  this: ConfigModule =>

  override lazy val dbConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig("tsql", config)
  override lazy val profile: JdbcProfile = dbConfig.profile
  override lazy val db: JdbcProfile#Backend#Database = dbConfig.db
}
