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


package edp.mad.persistence.dal

import edp.mad.module.{ConfigModuleImpl, DBDriverModuleImpl}
import edp.mad.persistence.base.BaseDalImpl
import edp.mad.persistence.entities._
import org.apache.log4j.Logger
import slick.lifted.TableQuery
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, SECONDS}

class ApplicationCacheDal(applicationCacheTableQ: TableQuery[ApplicationCacheTable]) extends BaseDalImpl[ApplicationCacheTable, ApplicationCacheEntity](applicationCacheTableQ) with ConfigModuleImpl with DBDriverModuleImpl{

  private val logger = Logger.getLogger(this.getClass)

  def updateOrInsert(rows: Seq[ApplicationCacheEntity]) = {
    rows.foreach { row =>
      logger.info(s"updateOrInsert ${row} ")
      try {
        if( Await.result(super.findByFilter(_.applicationId === row.applicationId), FiniteDuration(180, SECONDS) ).isEmpty){
          db.run(applicationCacheTableQ += row).mapTo[Seq[ApplicationCacheEntity]]
          logger.info(s"insert row \n")
        } else {
          db.run(applicationCacheTableQ.filter(_.applicationId === row.applicationId).update(row)).mapTo[Seq[ApplicationCacheEntity]]
          logger.info(s"update row  \n")
        }
      } catch {
        case e: Exception =>
          logger.error(s" Flow table query error ", e)
      }
    }
  }
}
