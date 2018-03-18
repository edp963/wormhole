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

import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.TableQuery
import edp.rider.module.DbModule._
import scala.concurrent.Await

class DirectiveDal(directiveTable: TableQuery[DirectiveTable]) extends BaseDalImpl[DirectiveTable, Directive](directiveTable) {

  def getDetail(id: Long): Option[SimpleDirective] = {
    Await.result(db.run(directiveTable.filter(_.id === id)
      .map(directive => (directive.protocolType, directive.flowId) <> (SimpleDirective.tupled, SimpleDirective.unapply))
      .result.headOption), minTimeOut)
  }
}
