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


package edp.rider.rest.util

import com.alibaba.fastjson.JSON
import edp.rider.RiderStarter.modules
import edp.rider.rest.persistence.entities.{NsDatabase, PushDownConnection}
import edp.rider.rest.util.CommonUtils.{isKeyEqualValue, _}
import edp.rider.rest.util.NamespaceUtils._
import edp.wormhole.util.config.KVConfig
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

object NsDatabaseUtils {

  def getDbConfig(nsSys: String, config: String): Option[Seq[KVConfig]] = {
    nsSys match {
      case "mysql" | "postgresql" | "oracle" | "vertica" | "greenplum" | "clickhouse" =>
        if (config == null || config == "") None
        else if (isKeyEqualValue(config)) {
          val seq = new ListBuffer[KVConfig]
          val keyValueSeq = config.split(",").mkString("&").split("&")
          keyValueSeq.foreach(
            keyValue => {
              val data = keyValue.split("=")
              if (data(0) == "maxPoolSize")
                seq += KVConfig(data(0), data(1))
            }
          )
          Some(seq)
        } else None
      case "es" =>
        if (config == null || config == "") None
        else if (isKeyEqualValue(config)) {
          val seq = new ListBuffer[KVConfig]
          val keyValueSeq = config.split(",").mkString("&").split("&")
          keyValueSeq.foreach(
            keyValue => {
              val data = keyValue.split("=")
              if (data(0) != "cluster.name")
                seq += KVConfig(data(0), data(1))
            }
          )
          Some(seq)
        } else None
      case _ =>
        if (config == null || config == "") None
        else if (isKeyEqualValue(config)) {
          val seq = new ListBuffer[KVConfig]
          val keyValueSeq = config.split(",").mkString("&").split("&")
          keyValueSeq.foreach(
            keyValue => {
              val data = keyValue.split("=")
              seq += KVConfig(data(0), data(1))
            }
          )
          Some(seq)
        } else None
    }
  }

  def getPushDownConfig(tranConfig: String): Seq[PushDownConnection] = {
    val seq = new ListBuffer[PushDownConnection]
    if (tranConfig == null || tranConfig == "")
      seq
    else {
      val dbSeq = getDbFromTrans(Some(tranConfig))
      if (dbSeq.nonEmpty) {
        dbSeq.foreach(db => {
          val dbSplit = db.split("\\.")
          val dbInfo = Await.result(modules.relProjectNsDal.getTranDbConfig(dbSplit(0), dbSplit(1), dbSplit(2)), minTimeOut)
          if (dbInfo.nonEmpty) {
            val head = dbInfo.head
            val connInfo =
              if (head.instance.nsSys == "cassandra" || head.instance.nsSys == "es" || head.instance.nsSys == "mongodb")
                getConnUrl(head.instance, head.db, "lookup")
              else getConnUrl(head.instance, head.db)
            seq += PushDownConnection(db, connInfo, head.db.user, head.db.pwd, getDbConfig(head.nsSys, head.db.config.getOrElse("")))
          }
        })
      }
      seq
    }
  }

  def getDbFromTrans(tranConfig: Option[String]): Seq[String] = {
    val dbSeq = new ListBuffer[String]
    if (tranConfig.nonEmpty && tranConfig.get != "") {
      val json = JSON.parseObject(tranConfig.get)
      if (json.containsKey("action")) {
        val seq = json.getString("action").split(";").filter(_.contains("pushdown_sql"))
        if (seq.nonEmpty) {
          seq.foreach(sql => {
            dbSeq += sql.split("with")(1).split("=")(0).trim
          })
        }
      }
    }
    dbSeq.distinct
  }

  def getDb(nsSys: String, nsInstance: String, nsDatabase: String): Option[NsDatabase] = {
    val instance = Await.result(modules.instanceDal.findByFilter(instance => instance.nsSys === nsSys && instance.nsInstance === nsInstance), minTimeOut).headOption
    if (instance.nonEmpty)
      Await.result(modules.databaseDal.findByFilter(db => db.nsInstanceId === instance.get.id && db.nsDatabase === nsDatabase), minTimeOut).headOption
    else None
  }
}


