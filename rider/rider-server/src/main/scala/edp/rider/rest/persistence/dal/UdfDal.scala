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

import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import edp.rider.rest.util.UdfUtils
import slick.lifted.TableQuery
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UdfDal(udfTable: TableQuery[UdfTable], relProjectUdfDal: RelProjectUdfDal, relStreamUdfDal: RelStreamUdfDal,
             projectDal: ProjectDal, streamDal: StreamDal) extends BaseDalImpl[UdfTable, Udf](udfTable) with RiderLogger {

  def getUdfProjectById(id: Long): Option[UdfProject] = {
    val udfOpt = Await.result(super.findById(id), minTimeOut)
    udfOpt match {
      case Some(udf) =>
        val map = Await.result(relProjectUdfDal.getUdfProjectName(Some(udf.id)), minTimeOut)
        val projectNames =
          if (map.contains(udf.id))
            map(udf.id).sorted.mkString(",")
          else ""
        Some(UdfProject(udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.pubic, udf.streamType, udf.mapOrAgg,
          udf.createTime, udf.createBy, udf.updateTime, udf.updateBy, projectNames))
      case None => None

    }
  }

  def getUdfProject: Future[Seq[UdfProject]] = {
    try {
      val udfProjectMap = Await.result(relProjectUdfDal.getUdfProjectName(), minTimeOut)
      val udfs = super.findAll
      udfs.map[Seq[UdfProject]] {
        val udfProjectSeq = new ArrayBuffer[UdfProject]
        udfs =>
          udfs.foreach(
            udf =>
              if (udfProjectMap.contains(udf.id))
                udfProjectSeq += UdfProject(udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.pubic, udf.streamType, udf.mapOrAgg,
                  udf.createTime, udf.createBy, udf.updateTime, udf.updateBy, udfProjectMap(udf.id).sorted.mkString(","))
              else
                udfProjectSeq += UdfProject(udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.pubic, udf.streamType, udf.mapOrAgg,
                  udf.createTime, udf.createBy, udf.updateTime, udf.updateBy, "")
          )
          udfProjectSeq
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"admin refresh users failed", ex)
        throw ex
    }
  }

  def delete(id: Long): (Boolean, String) = {
    try {
      val relProject = Await.result(relProjectUdfDal.findByFilter(_.udfId === id), minTimeOut)
      val relStream = Await.result(relStreamUdfDal.findByFilter(_.udfId === id), minTimeOut)
      val projects = Await.result(projectDal.findByFilter(_.id inSet relProject.map(_.projectId)), minTimeOut).map(_.name)
      val streams = Await.result(streamDal.findByFilter(_.id inSet relStream.map(_.streamId)), minTimeOut).map(_.name)
      if (projects.nonEmpty && streams.nonEmpty) {
        riderLogger.info(s"project ${projects.mkString(",")} and stream ${streams.mkString(",")} still use UDF $id, can't delete it")
        (false, s"please revoke project ${projects.mkString(",")}, stream ${streams.mkString(",")} and UDF binding relation first")
      } else if (projects.nonEmpty && streams.isEmpty) {
        riderLogger.info(s"project ${projects.mkString(",")} still use UDF $id, can't delete it")
        (false, s"please revoke project ${projects.mkString(",")} and UDF binding relation first")
      } else if (projects.isEmpty && streams.nonEmpty) {
        riderLogger.info(s"stream ${streams.mkString(",")} still use UDF $id, can't delete it")
        (false, s"please revoke stream ${streams.mkString(",")} and UDF binding relation first")
      } else {
        val jarName = Await.result(super.findById(id), minTimeOut).get.jarName
        val sameJarUdfs = Await.result(super.findByFilter(udf => udf.jarName === jarName && udf.id =!= id), minTimeOut)
        if (sameJarUdfs.isEmpty)
          UdfUtils.deleteHdfsPath(jarName)
        Await.result(super.deleteById(id), minTimeOut)
        (true, "success")
      }
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete UDF $id failed", ex)
        throw new Exception(s"delete UDF $id failed", ex)
    }
  }
}
