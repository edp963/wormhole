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
import slick.lifted.{CanBeQueryCondition, TableQuery}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class UdfDal(udfTable: TableQuery[UdfTable], relProjectUdfDal: RelProjectUdfDal) extends BaseDalImpl[UdfTable, Udf](udfTable) with RiderLogger {

  def getUdfProject: Future[Seq[UdfProject]] = {
    try {
      val udfProjectMap = Await.result(relProjectUdfDal.getUdfProjectName, minTimeOut)
      val udfs = super.findAll
      udfs.map[Seq[UdfProject]] {
        val udfProjectSeq = new ArrayBuffer[UdfProject]
        udfs => udfs.foreach(
          udf =>
            if (udfProjectMap.contains(udf.id))
              udfProjectSeq += UdfProject(udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.pubic,
                udf.createTime, udf.createBy, udf.updateTime, udf.updateBy, udfProjectMap(udf.id).sorted.mkString(","))
            else
              udfProjectSeq += UdfProject(udf.id, udf.functionName, udf.fullClassName, udf.jarName, udf.desc, udf.pubic,
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
}
