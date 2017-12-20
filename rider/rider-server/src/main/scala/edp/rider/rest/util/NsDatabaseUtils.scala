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
import edp.rider.rest.persistence.entities.PushDownConnection
import edp.rider.rest.util.CommonUtils.{isJson, isKeyEqualValue, _}
import edp.rider.rest.util.NamespaceUtils._
import edp.wormhole.common.KVConfig

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

object NsDatabaseUtils {

  def getDbConfig(nsSys: String, config: String): Option[Seq[KVConfig]] = {
    nsSys match {
      case "mysql" | "postgresql" | "oracle" =>
        if (config == null || config == "") None
        //        else if (isJson(config)) {
        //          val seq = new ListBuffer[KVConfig]
        //          val json = JSON.parseObject(config)
        //          val keySet = json.keySet().toArray
        //          keySet.foreach(key => {
        //            if (key == "maxPoolSize")
        //              seq += KVConfig(key.toString, json.get(key).toString)
        //          })
        //          Some(seq)
        //        }
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
        //        else if (isJson(config)) {
        //          val seq = new ListBuffer[KVConfig]
        //          val json = JSON.parseObject(config)
        //          val keySet = json.keySet().toArray
        //          keySet.foreach(key => {
        //            if (key == "maxPoolSize")
        //              seq += KVConfig(key.toString, json.get(key).toString)
        //          })
        //          Some(seq)
        //        }
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
        //        else if (isJson(config)) {
        //          val seq = new ListBuffer[KVConfig]
        //          val json = JSON.parseObject(config)
        //          val keySet = json.keySet().toArray
        //          keySet.foreach(key => seq += KVConfig(key.toString, json.get(key).toString))
        //          Some(seq)
        //        }
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
      val json = JSON.parseObject(tranConfig)
      if (json.containsKey("action")) {
        val sql = json.getString("action")
        sql.split(";").foreach(
          sql => {
            if (sql.contains("pushdown_sql")) {
              val regrex = "with[\\s\\S]+\\=".r.findFirstIn(sql)
              if (regrex.nonEmpty) {
                val db = regrex.get.split("=")(0).stripPrefix("with").trim
                if (db != "") {
                  val dbSeq = db.split("\\.")
                  val dbInfo = Await.result(modules.relProjectNsDal.getTranDbConfig(dbSeq(0), dbSeq(1), dbSeq(2)), minTimeOut)
                  if (dbInfo.nonEmpty) {
                    val head = dbInfo.head
                    val connInfo =
                      if (head.instance.nsSys == "cassandra" || head.instance.nsSys == "es")
                        getConnUrl(head.instance, head.db, "lookup")
                      else getConnUrl(head.instance, head.db)
                    seq += PushDownConnection(db, connInfo, head.db.user, head.db.pwd, getDbConfig(head.nsSys, head.db.config.getOrElse("")))
                  }
                }
              }
            }
          }
        )
      }
      seq
    }
  }
}


