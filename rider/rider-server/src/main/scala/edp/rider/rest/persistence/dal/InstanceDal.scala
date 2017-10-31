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

import edp.rider.common.{DatabaseSearchException, InstanceNotExistException, RiderLogger}
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.duration.Duration._

class InstanceDal(instanceTable: TableQuery[InstanceTable]) extends BaseDalImpl[InstanceTable, Instance](instanceTable) with RiderLogger {

  def getStreamKafka(streamInstanceMap: Map[Long, Long]): Map[Long, StreamKafka] = {
    try {
      val instanceMap = Await.result(super.findByFilter(_.id inSet streamInstanceMap.values.toList.distinct), Inf)
        .map(instance => (instance.id, StreamKafka(instance.nsInstance, instance.connUrl))).toMap[Long, StreamKafka]
      streamInstanceMap.map(
        map => {
          if (instanceMap.contains(map._2)) (map._1, instanceMap(map._2))
          else throw InstanceNotExistException(s"instance ${map._2} didn't exist")
        }
      )
    } catch {
      case ex: Exception =>
        throw DatabaseSearchException(ex.getMessage, ex.getCause)
    }
  }
}
