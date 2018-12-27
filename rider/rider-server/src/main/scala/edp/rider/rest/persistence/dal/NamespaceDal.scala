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


package edp.rider.rest.persistence.dal

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.GenericHttpCredentials
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.util.ByteString
import edp.rider.RiderStarter.modules._
import edp.rider.RiderStarter.{materializer, system}
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.module.DbModule._
import edp.rider.rest.persistence.base.{BaseDal, BaseDalImpl}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.router.SessionClass
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.NamespaceUtils
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.JsonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class NamespaceDal(namespaceTable: TableQuery[NamespaceTable],
                   databaseDal: NsDatabaseDal,
                   instanceDal: BaseDal[InstanceTable, Instance],
                   dbusDal: DbusDal) extends BaseDalImpl[NamespaceTable, Namespace](namespaceTable) with RiderLogger {
  def getDbus4FromRest: Seq[SimpleDbus] = {
    try {
      val dbusServices =
        if (RiderConfig.dbusUrl != null) RiderConfig.dbusUrl.toList
        else List()
      val simpleDbusSeq = new ArrayBuffer[SimpleDbus]
      dbusServices.map {
        service => {
          if (service != null && service != "") {
            val response = Await.result(Http().singleRequest(HttpRequest(uri = service)), minTimeOut)
            response match {
              case HttpResponse(StatusCodes.OK, headers, entity, _) =>
                Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
                  riderLogger.info(s"synchronize dbus namespaces $service success.")
                  body => simpleDbusSeq ++= json2caseClass[Seq[SimpleDbus]](body.utf8String)
                }, minTimeOut)
              case resp@HttpResponse(code, _, _, _) =>
                riderLogger.error(s"synchronize dbus namespaces $service failed, ${code.reason}.")
                "parse failed"
            }
          } else riderLogger.debug(s"dbus namespace service is not config")
        }
      }
      simpleDbusSeq
    } catch {
      case ex: Exception =>
        riderLogger.error(s"synchronize dbus namespace failed", ex)
        Seq()
    }

  }

  def getDbusFromRest: Seq[SimpleDbus] = {
    try {
      val simpleDbusSeq = new ArrayBuffer[SimpleDbus]
      RiderConfig.dbusConfigList.map {
        service => {
          val token = NamespaceUtils.getDBusToken(service)
          val response = Await.result(Http().singleRequest(HttpRequest(uri = service.namespaceUrl)
            .addCredentials(GenericHttpCredentials("", token))), minTimeOut)
          response match {
            case HttpResponse(StatusCodes.OK, headers, entity, _) =>
              Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _).map {
                riderLogger.info(s"synchronize DBus namespaces ${service.namespaceUrl} success.")
                body =>
                  val data = json2jValue(body.utf8String)
                  val status = JsonUtils.getInt(data, "status")
                  if (status == 0) {
                    simpleDbusSeq ++= json2caseClass[DBusNamespaceResponse](body.utf8String).payload
                  } else {
                    val msg = JsonUtils.getString(data, "message")
                    riderLogger.error(s"synchronize DBus namespace failed with token $token, ${msg}")
                    throw new Exception(s"synchronize DBus namespace failed with token $token, ${msg}")
                  }
              }, minTimeOut)
            case resp@HttpResponse(code, _, _, _) =>
              riderLogger.error(s"synchronize dbus namespaces ${service.namespaceUrl} failed, ${code.reason}.")
              "parse failed"
          }
        }
      }
      simpleDbusSeq
    } catch {
      case ex: Exception =>
        riderLogger.error(s"synchronize dbus namespace failed", ex)
        Seq()
    }

  }

  def updateUmsInfo(id: Long, umsInfo: SourceSchema, user: Long): Future[Int] = {
    val schema = Option(caseClass2json[SourceSchema](umsInfo))
    db.run(namespaceTable.filter(_.id === id).map(ns => (ns.umsInfo, ns.updateTime, ns.updateBy)).update(schema, currentSec, user)).mapTo[Int]
  }

  def updateSinkInfo(id: Long, sinkInfo: SinkSchema, user: Long): Future[Int] = {
    val schema = Option(caseClass2json[SinkSchema](sinkInfo))
    db.run(namespaceTable.filter(_.id === id).map(ns => (ns.sinkInfo, ns.updateTime, ns.updateBy)).update(schema, currentSec, user)).mapTo[Int]
  }

  def dbusInsert(session: SessionClass): Future[Seq[Dbus]] = {
    try {
      val simpleDbusSeq = getDbusFromRest ++: getDbus4FromRest
      riderLogger.info(s"sync dbus namespace size: ${simpleDbusSeq.size}")
      val maxDbusId = Await.result(dbusDal.getMaxDbusId, minTimeOut)
      val syncDbusSeq =
        if (maxDbusId.nonEmpty) {
          simpleDbusSeq.filter(_.id > maxDbusId.get)
        } else simpleDbusSeq
      riderLogger.info(s"actual need sync dbus namespace size: ${syncDbusSeq.size}")

      val kafkaSeq = new mutable.HashSet[String]
      val kafkaTopicMap = mutable.HashMap.empty[String, ArrayBuffer[String]]
      val kafkaIdMap = mutable.HashMap.empty[String, Long]
      val topicIdMap = mutable.HashMap.empty[String, Long]
      val instanceSeq = new ArrayBuffer[Instance]
      val databaseSeq = new ArrayBuffer[NsDatabase]
      val dbusSeq = new ArrayBuffer[Dbus]
      val dbusUpdateSeq = new ArrayBuffer[Dbus]

      syncDbusSeq.foreach(simple => {
        kafkaSeq.add(simple.kafka)
        if (kafkaTopicMap.contains(simple.kafka) && !kafkaTopicMap(simple.kafka).contains(simple.topic))
          kafkaTopicMap.update(simple.kafka, kafkaTopicMap(simple.kafka) += simple.topic)
        else if (!kafkaTopicMap.contains(simple.kafka))
          kafkaTopicMap.put(simple.kafka, ArrayBuffer(simple.topic))
      })
      val dbusKafka = Await.result(instanceDal.findByFilter(_.nsInstance.startsWith("dbusKafka")), minTimeOut).size
      var i = dbusKafka + 1
      kafkaSeq.foreach {
        kafka => {
          val instanceSearch = Await.result(instanceDal.findByFilter(_.connUrl === kafka), minTimeOut)
          if (instanceSearch.isEmpty) {
            instanceSeq += Instance(0, s"dbusKafka$i", Some("dbus kafka !!!"), "kafka", kafka, active = true, currentSec, session.userId, currentSec, session.userId)
            i = i + 1
          }
          else kafkaIdMap.put(kafka, instanceSearch.head.id)
        }
      }

      val instances = Await.result(instanceDal.insert(instanceSeq), minTimeOut)
      instances.foreach(instance => kafkaIdMap.put(instance.connUrl, instance.id))

      val topicSearch = Await.result(databaseDal.findByFilter(_.nsInstanceId inSet kafkaIdMap.values), minTimeOut)
      kafkaTopicMap.foreach(map => {
        map._2.foreach(topic => {
          val topicExist = topicSearch.filter(_.nsDatabase == topic)
          if (topicExist.nonEmpty) topicIdMap.put(topic, topicExist.head.id)
          else
            databaseSeq += NsDatabase(0, topic, Some("dbus topic !!!"), kafkaIdMap(map._1), Some(""), Some(""), Some(1), Some(""), active = true, currentSec, session.userId, currentSec, session.userId)
        })
      })

      val dbSeq = Await.result(databaseDal.insert(databaseSeq), maxTimeOut)
      dbSeq.foreach(db => topicIdMap.put(db.nsDatabase, db.id))

      val dbusSearch = Await.result(dbusDal.findAll, minTimeOut)
      syncDbusSeq.foreach(simple => {
        val dbusExist = dbusSearch.filter(dbus => dbus.namespace.split("\\.").take(4).mkString(".") == simple.namespace.split("\\.").take(4).mkString("."))
        if (dbusExist.isEmpty) {
          riderLogger.info("simple: " + simple)
          dbusSeq += Dbus(0, simple.id, simple.namespace, simple.kafka, simple.topic, kafkaIdMap(simple.kafka), topicIdMap(simple.topic), simple.createTime, currentSec)
        }
        else {
          val dbusUpdate = dbusExist.filter(dbus => dbus.kafka == simple.kafka && dbus.topic == simple.topic && dbus.namespace == simple.namespace)
          if (dbusUpdate.isEmpty)
            dbusUpdateSeq += Dbus(dbusExist.head.id, simple.id, simple.namespace, simple.kafka, simple.topic, kafkaIdMap(simple.kafka), topicIdMap(simple.topic), simple.createTime, currentSec)
        }
      })

      riderLogger.info("dbus insert size: " + dbusSeq.size)
      val dbusInsertSeq = Await.result(dbusDal.insert(dbusSeq), maxTimeOut)
      riderLogger.info("dbus update size: " + dbusUpdateSeq.size)
      Await.result(dbusDal.update(dbusUpdateSeq), minTimeOut)
      Future(dbusInsertSeq ++ dbusUpdateSeq)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"insert or update dbus namespaces failed", ex)
        Future(Seq())
    }
  }


  def generateNamespaceSeqByDbus(dbusSeq: Seq[Dbus], session: SessionClass): (Seq[Namespace], Seq[Namespace]) = {
    val namespace = Await.result(super.findAll, minTimeOut)
    val insertSeq = new ArrayBuffer[Namespace]()
    val updateSeq = new ArrayBuffer[Namespace]()
    dbusSeq.map(dbus => {
      val nsSplit: Array[String] = dbus.namespace.split("\\.")
      val nsSearch = namespace.filter(ns => ns.nsSys == nsSplit(0) && ns.nsInstance == nsSplit(1) && ns.nsDatabase == nsSplit(2) && ns.nsTable == nsSplit(3))
      if (nsSearch.isEmpty)
        insertSeq += Namespace(0, nsSplit(0), nsSplit(1), nsSplit(2), nsSplit(3), "*", "*", "*",
          None, None, None, dbus.databaseId, dbus.instanceId, active = true, dbus.synchronizedTime, session.userId, currentSec, session.userId)
      else
        updateSeq += Namespace(nsSearch.head.id, nsSplit(0), nsSplit(1), nsSplit(2), nsSplit(3), "*", "*", "*",
          None, None, None, dbus.databaseId, dbus.instanceId, active = true, dbus.synchronizedTime, session.userId, currentSec, session.userId)
    })
    (insertSeq, updateSeq)
  }

  def getNamespaceByNs(ns: String): Option[Namespace] = {
    try {
      val nsSplit = ns.split("\\.")
      Await.result(super.findByFilter(ns => ns.nsSys === nsSplit(0) && ns.nsInstance === nsSplit(1) && ns.nsDatabase === nsSplit(2) && ns.nsTable === nsSplit(3)), minTimeOut).headOption
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get namespace object by $ns failed", ex)
        throw ex
    }

  }

  def getNamespaceByNs(sys: String, database: String, table: String): Option[Namespace] =
    try {
      val namespaces = Await.result(super.findByFilter(ns => ns.nsSys === sys.toLowerCase && ns.nsDatabase === database && ns.nsTable === table), minTimeOut)
      if (namespaces.isEmpty) None
      else if (namespaces.size == 1) namespaces.headOption
      else {
        riderLogger.warn(s"search source namespace for request sys $sys, database $database, table $table has ${namespaces.size} match, $namespaces")
        namespaces.sortBy(_.updateTime).foreach(
          ns => {
            val instance = Await.result(instanceDal.findById(ns.nsInstanceId), minTimeOut).head
            if (instance.nsSys == "kafka") {
              riderLogger.warn(s"search source namespace for request sys $sys, database $database, table $table has ${namespaces.size} match, select $ns")
              return Some(ns)
            }
          }
        )
        riderLogger.warn(s"search source namespace for request sys $sys, database $database, table $table has ${namespaces.size} match, finally selected no one")
        None
      }

    } catch {
      case ex: Exception =>
        riderLogger.error(s"get namespace by dataSys $sys, database $database, table $table failed", ex)
        throw ex
    }

  def getSinkNamespaceByNs(sys: String, instance: String, database: String, table: String): Option[Namespace] =
    try {
      Await.result(super.findByFilter(ns => ns.nsSys === sys.toLowerCase && ns.nsInstance === instance && ns.nsDatabase === database && ns.nsTable === table), minTimeOut).headOption
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get namespace by dataSys $sys, database $database, table $table failed", ex)
        throw ex
    }


  def getNsDetail(ns: String): (Instance, NsDatabase, Namespace) = {
    val namespace = getNamespaceByNs(ns).get
    try {
      val instance = Await.result(instanceDal.findByFilter(_.id === namespace.nsInstanceId), minTimeOut).head
      val database = Await.result(databaseDal.findByFilter(_.id === namespace.nsDatabaseId), minTimeOut).head
      (instance, database, namespace)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"get instance/database by namespace $ns failed", ex)
        throw ex
    }
  }

  def updateKeys(id: Long, keys: String): Int =
    try {
      Await.result(db.run(namespaceTable.filter(_.id === id).map(_.keys).update(Some(keys))), minTimeOut)
    } catch {
      case ex: Exception =>
        riderLogger.error(s"update namespace $id keys $keys failed", ex)
        throw ex
    }

  def getUmsInfo(id: Long): Future[Option[SourceSchema]] = {
    super.findById(id).map[Option[SourceSchema]](
      namespaceOpt => namespaceOpt match {
        case Some(namespace) =>
          namespace.sourceSchema match {
            case Some(umsInfo) => Some(json2caseClass[SourceSchema](umsInfo))
            case None => None
          }
        case None => None
      }
    )
  }

  def getSinkInfo(id: Long): Future[Option[SinkSchema]] = {
    super.findById(id).map[Option[SinkSchema]](
      namespaceOpt => namespaceOpt match {
        case Some(namespace) =>
          namespace.sinkSchema match {
            case Some(sinkInfo) => Some(json2caseClass[SinkSchema](sinkInfo))
            case None => None
          }
        case None => None
      }
    )
  }

  def delete(id: Long): (Boolean, String) = {
    try {
      //      val ns = NamespaceUtils.generateStandardNs(Await.result(super.findById(id), minTimeOut).get)
      val relProject = Await.result(relProjectNsDal.findByFilter(_.nsId === id), minTimeOut)
      //      val flows = Await.result(flowDal.findByFilter
      //      (flow => flow.sourceNs === ns || flow.sinkNs === ns || flow.tranConfig.getOrElse("").like(s"%${ns.split("\\.").take(3).mkString(".")}%")), minTimeOut).map(_.id)
      val projects = Await.result(projectDal.findByFilter(_.id inSet relProject.map(_.projectId)), minTimeOut).map(_.name)
      if (projects.nonEmpty) {
        riderLogger.info(s"project ${projects.mkString(",")} still use namespace $id, can't delete it")
        (false, s"please revoke project ${projects.mkString(",")} and namespace binding relation first")
      } else {
        Await.result(super.deleteById(id), minTimeOut)
        (true, "success")
      }
      // if (projects.nonEmpty && flows.nonEmpty) {
      //        riderLogger.info(s"project ${projects.mkString(",")} and flow ${flows.mkString(",")} still use namespace $id, can't delete it")
      //        (false, s"please revoke project ${projects.mkString(",")} and namespace binding relations, flow ${flows.mkString(",")} first")
      //      } else if (projects.nonEmpty && flows.isEmpty) {
      //        riderLogger.info(s"project ${projects.mkString(",")} still use namespace $id, can't delete it")
      //        (false, s"please revoke project ${projects.mkString(",")} and namespace binding relations first")
      //      } else if (projects.isEmpty && flows.nonEmpty) {
      //        riderLogger.info(s"flow ${flows.mkString(",")} still use namespace $id, can't delete it")
      //        (false, s"please revoke flow ${flows.mkString(",")} and namespace binding relations first")
      //      } else {
      //        Await.result(super.deleteById(id), minTimeOut)
      //        (true, "success")
      //      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete namespace $id failed", ex)
        throw new Exception(s"delete namespace $id failed", ex)
    }
  }

  def updateNs(nsSeq: Seq[Namespace]): Int = {
    nsSeq.map(ns => updateNs(ns)).sum
  }

  def updateNs(ns: Namespace): Int = {
    Await.result(super.update(ns), minTimeOut)
  }

}
