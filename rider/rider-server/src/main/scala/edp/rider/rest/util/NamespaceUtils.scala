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


import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, _}
import akka.util.ByteString
import edp.rider.RiderStarter.{materializer, modules, system}
import edp.rider.common.{DBusConfig, RiderLogger}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.wormhole.ums.UmsDataSystem
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.JsonUtils.json2jValue
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

object NamespaceUtils extends RiderLogger {

  def generateStandardNs(ns: NamespaceInfo) = Seq(ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar).mkString(".")

  def generateStandardNs(ns: Namespace) = Seq(ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar).mkString(".")

  def generateStandardNs(ns: NamespaceAdmin) = Seq(ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar).mkString(".")

  def getConnUrl(instance: Instance, db: NsDatabase, connType: String = "sink") = {
    instance.nsSys match {
      case "mysql" | "postgresql" | "phoenix" | "vertica" =>
        db.config match {
          case Some(conf) =>
            if (conf != "") {
              val confStr =
                (keyEqualValuePattern.toString.r findAllIn conf.split(",").mkString("&")).toList.mkString("&")
              s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}?$confStr"
            } else s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}"
          case None => s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}"
        }
      case "clickhouse" =>
        instance.connUrl.split(",").map(url => {
          db.config match {
            case Some(conf) =>
              if (conf != "") {
                val confStr =
                  (keyEqualValuePattern.toString.r findAllIn conf.split(",").mkString("&")).toList.mkString("&")
                s"jdbc:${instance.nsSys}://$url/${db.nsDatabase}?$confStr"
              } else s"jdbc:${instance.nsSys}://$url/${db.nsDatabase}"
            case None => s"jdbc:${instance.nsSys}://$url/${db.nsDatabase}"
          }
        }).mkString(",")
      case "oracle" =>
        val hostPort = instance.connUrl.split(":")
        val serviceName = db.config match {
          case Some(conf) =>
            if (conf != "") {
              if (conf.indexOf("service_name") >= 0) {
                val index = conf.indexOf("service_name")
                val length = "service_name".length
                val lastPart = conf.indexOf(",", index + length)
                val endIndex = if (lastPart < 0) conf.length else lastPart
                conf.substring(conf.indexOf("=", index + length) + 1, endIndex)
              } else if (conf.indexOf("SERVICE_NAME") >= 0) {
                val index = conf.indexOf("SERVICE_NAME")
                val length = "SERVICE_NAME".length
                val lastPart = conf.indexOf(",", index + length)
                val endIndex = if (lastPart < 0) conf.length else lastPart
                conf.substring(conf.indexOf("=", index + length) + 1, endIndex)
              } else {
                riderLogger.info("NO ORACLE SERVICE NAME:")
                ""
              }
            } else ""
          case None => ""
        }
        s"jdbc:oracle:thin:@(DESCRIPTION=(FAILOVER = yes)(ADDRESS = (PROTOCOL = TCP)(HOST =${hostPort(0)})(PORT = ${hostPort(1)}))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = $serviceName)))"

      case "cassandra" =>
        if (connType == "lookup")
          db.config match {
            case Some(conf) =>
              if (conf != "") {
                val confStr =
                  (keyEqualValuePattern.toString.r findAllIn conf.split(",").mkString("&")).toList.mkString("&")
                s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}?$confStr"
              } else s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}"
            case None => s"jdbc:${instance.nsSys}://${instance.connUrl}/${db.nsDatabase}"
          }
        else instance.connUrl
      case "es" =>
        if (connType == "lookup") {
          if (db.config.nonEmpty && db.config.get != "") {
            val confArray = (keyEqualValuePattern.toString.r findAllIn db.config.get.split(",").mkString("&")).toList
            val connConf = confArray.find(_.contains("cluster.name"))
            if (connConf.nonEmpty)
              s"jdbc:sql4es://${instance.connUrl}/${db.nsDatabase}?${connConf.get}"
            else s"jdbc:sql4es://${instance.connUrl}/${db.nsDatabase}"
          } else s"jdbc:sql4es://${instance.connUrl}/${db.nsDatabase}"
        } else instance.connUrl
      case "mongodb" =>
        if (connType == "lookup") {
          if (db.user.nonEmpty && db.user.get != "" && db.config.nonEmpty && db.config.get != "")
            s"mongodb://${db.user.get}:${db.pwd.get}@${instance.connUrl}/${db.nsDatabase}?${db.config.get.split(",").mkString("&")}"
          else if (db.user.nonEmpty && db.user.get != "" && db.config.isEmpty)
            s"mongodb://${db.user.get}:${db.pwd.get}@${instance.connUrl}/${db.nsDatabase}"
          else if (db.user.isEmpty && db.config.nonEmpty && db.config.get != "")
            s"mongodb://${instance.connUrl}/${db.nsDatabase}?${db.config.get.split(",").mkString("&")}"
          else s"mongodb://${instance.connUrl}/${db.nsDatabase}"
        } else instance.connUrl
      case "greenplum" =>
        if (db.config.nonEmpty && db.config.get != "") {
          val confStr: String = (keyEqualValuePattern.toString.r findAllIn db.config.get.split(",").mkString("&")).toList.mkString("&")
          if (confStr.nonEmpty)
            s"jdbc:postgresql://${instance.connUrl}/${db.nsDatabase}?$confStr"
          else
            s"jdbc:postgresql://${instance.connUrl}/${db.nsDatabase}"
        } else
          s"jdbc:postgresql://${instance.connUrl}/${db.nsDatabase}"
      case _ => instance.connUrl
    }

  }

  def permCheck(projectId: Long, nsSeq: Seq[String]): Seq[String] = {
    val nonPermList = new ListBuffer[String]
    val existList = new ListBuffer[Namespace]
    nsSeq.foreach(ns => {
      val namespace = modules.namespaceDal.getNamespaceByNs(ns)
      if (namespace.nonEmpty) existList += namespace.get
      else nonPermList += ns.split("\\.")(3)
    })
    val nsIds = Await.result(modules.relProjectNsDal.findByFilter(_.projectId === projectId), minTimeOut).map(_.nsId)
    nonPermList ++ existList.filterNot(ns => nsIds.contains(ns.id)).map(_.nsTable)
  }

  def getTopic(id: Long): String = {
    val nsOpt = Await.result(modules.namespaceDal.findById(id), minTimeOut)
    nsOpt match {
      case Some(ns) =>
        val instanceOpt = Await.result(modules.instanceDal.findById(ns.nsInstanceId), minTimeOut)
        instanceOpt match {
          case Some(instance) =>
            UmsDataSystem.dataSystem(instance.nsSys) match {
              case UmsDataSystem.KAFKA =>
                Await.result(modules.databaseDal.findById(ns.nsDatabaseId), minTimeOut) match {
                  case Some(db) => db.nsDatabase
                  case None => throw new Exception(s"namespace $id not in any kafka")
                }
              case _ => throw new Exception(s"namespace $id not in kafka")
            }
          case None => throw new Exception(s"namespace $id not valid")
        }
      case None => throw new Exception(s"namespace $id not found")
    }
  }

  def getTopic(ns: String): String = {
    val nsOpt = modules.namespaceDal.getNamespaceByNs(ns)
    nsOpt match {
      case Some(ns) =>
        val instanceOpt = Await.result(modules.instanceDal.findById(ns.nsInstanceId), minTimeOut)
        instanceOpt match {
          case Some(instance) =>
            UmsDataSystem.dataSystem(instance.nsSys) match {
              case UmsDataSystem.KAFKA =>
                Await.result(modules.databaseDal.findById(ns.nsDatabaseId), minTimeOut) match {
                  case Some(db) => db.nsDatabase
                  case None => throw new Exception(s"namespace $ns not in any kafka")
                }
              case _ => throw new Exception(s"namespace $ns not in kafka")
            }
          case None => throw new Exception(s"namespace $ns not valid")
        }
      case None => throw new Exception(s"namespace $ns not found")
    }
  }

  def getDBusToken(dbusConfig: DBusConfig): String = {
    try {
      val postData = DBusUser(dbusConfig.user, dbusConfig.password)
      val request = HttpRequest(uri = dbusConfig.loginUrl,
        method = HttpMethods.POST,
        headers = List(headers.Accept.apply(MediaTypes.`application/json`)),
        entity = HttpEntity.apply(ContentTypes.`application/json`, ByteString(JsonUtils.caseClass2json(postData))))
      val response = Await.result(Http().singleRequest(request), minTimeOut)
      response match {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
            body =>
              val data = json2jValue(body.utf8String)
              val status = JsonUtils.getInt(data, "status")
              if (status == 0) {
                val payload = JsonUtils.getJValue(data, "payload")
                JsonUtils.getString(payload, "token")
              } else {
                val msg = JsonUtils.getString(data, "message")
                riderLogger.error(s"get DBus ${dbusConfig.loginUrl} ${dbusConfig.user} user token failed, ${msg}")
                throw new Exception(s"get DBus ${dbusConfig.loginUrl} ${dbusConfig.user} user token failed, ${msg}")
              }
          }, minTimeOut)
        case resp@HttpResponse(code, _, _, _) =>
          riderLogger.error(s"get DBus ${dbusConfig.loginUrl} ${dbusConfig.user} user token failed, ${code.reason}.")
          throw new Exception(s"get DBus ${dbusConfig.loginUrl} ${dbusConfig.user} user token failed, ${code.reason}")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"request DBus ${dbusConfig.loginUrl} ${dbusConfig.user} user token failed", ex)
        throw new Exception(ex)
    }
  }

/*  def namespaceMatch(flowNs: String, matchNsSeq: Seq[String]): Boolean = {
    val sourceNsSeq = flowNs.split("\\.")
    var matchResult = false
    matchNsSeq.foreach(matchNs => {
      val matchNsSeq = matchNs.split("\\.")
      if(sourceNsSeq(1) == "*" && sourceNsSeq(0) == matchNsSeq(0)) {
        matchResult = true
      } else if(sourceNsSeq(2) == "*" && sourceNsSeq(0) == matchNsSeq(0) && sourceNsSeq(1) == matchNsSeq(1)) {
        matchResult = true
      } else if(sourceNsSeq(3) == "*" && sourceNsSeq(0) == matchNsSeq(0) && sourceNsSeq(1) == matchNsSeq(1) && sourceNsSeq(2) == matchNsSeq(2)) {
        matchResult = true
      } else if(sourceNsSeq(0) == matchNsSeq(0) && sourceNsSeq(1) == matchNsSeq(1) && sourceNsSeq(2) == matchNsSeq(2) && sourceNsSeq(3) == matchNsSeq(3)){
        matchResult = true
      }
    })
    matchResult
  }*/
}
