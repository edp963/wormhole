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


package edp.rider.rest.router

import akka.http.scaladsl.server._
import edp.rider.module._
import edp.rider.rest.router.admin.routes._
import edp.rider.rest.router.app.routes._
import edp.rider.rest.router.user.routes._
import edp.rider.rest.util.CrossDomainSupport._

class RoutesApi(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val swagger = new SwaggerRoutes
  lazy val login = new LoginRoutes(modules)
  lazy val genToken = new GenTokenRoutes(modules)
  lazy val changePwd = new ChangePwdRoutes(modules)
  lazy val instanceAdmin = new InstanceAdminRoutes(modules)
  lazy val databaseAdmin = new NsDatabaseAdminRoutes(modules)
  lazy val namespaceAdmin = new NamespaceAdminRoutes(modules)
  lazy val streamAdmin = new StreamAdminRoutes(modules)
  lazy val flowAdmin = new FlowAdminRoutes(modules)
  lazy val userAdmin = new UserAdminRoutes(modules)
  lazy val projectAdmin = new ProjectAdminRoutes(modules)
  lazy val riderInfoAdmin = new RiderInfoAdminRoutes(modules)
  lazy val udfAdmin = new UdfAdminRoutes(modules)
  lazy val riderUI = new RiderRoutes
  lazy val jobAdmin = new JobAdminRoutes(modules)

  lazy val projectUser = new ProjectUserRoutes(modules)
  lazy val namespaceUser = new NamespaceUserRoutes(modules)
  lazy val streamUser = new StreamUserRoutes(modules)
  lazy val flowUser = new FlowUserRoutes(modules)
  lazy val actionUser = new ActionUserRoutes(modules)
  lazy val databaseUser = new NsDatabaseUserRoutes(modules)
  lazy val jobUser = new JobUserRoutes(modules)
  lazy val instanceUser = new InstanceUserRoutes(modules)
  lazy val udfUser = new UdfUserRoutes(modules)
  lazy val user = new UserRoutes(modules)

  lazy val flowApp = new FlowAppRoutes(modules)
  lazy val jobApp = new JobAppRoutes(modules)
  lazy val instanceApp = new InstanceAppRoutes(modules)
  lazy val nsDatabaseApp = new NsDatabaseAppRoutes(modules)
  lazy val nsApp = new NamespaceAppRoutes(modules)
  lazy val monitor = new MonitorRoutes(modules)

  lazy val routes: Route =
    crossDomainHandler(swagger.indexRoute) ~
      crossDomainHandler(swagger.routes) ~
      crossDomainHandler(riderUI.indexRoute) ~
      pathPrefix("api" / "v1") {
        crossDomainHandler(login.routes) ~
          crossDomainHandler(changePwd.routes) ~
          crossDomainHandler(genToken.routes) ~
          pathPrefix("admin") {
            crossDomainHandler(instanceAdmin.routes) ~
              crossDomainHandler(databaseAdmin.routes) ~
              crossDomainHandler(namespaceAdmin.routes) ~
              crossDomainHandler(streamAdmin.routes) ~
              crossDomainHandler(flowAdmin.routes) ~
              crossDomainHandler(userAdmin.routes) ~
              crossDomainHandler(projectAdmin.routes) ~
              crossDomainHandler(riderInfoAdmin.routes) ~
              crossDomainHandler(udfAdmin.routes) ~
              crossDomainHandler(jobAdmin.routes)
          } ~
          pathPrefix("user") {
            crossDomainHandler(projectUser.routes) ~
              crossDomainHandler(namespaceUser.routes) ~
              crossDomainHandler(streamUser.routes) ~
              crossDomainHandler(flowUser.routes) ~
              crossDomainHandler(actionUser.routes) ~
              crossDomainHandler(databaseUser.routes) ~
              crossDomainHandler(instanceUser.routes) ~
              crossDomainHandler(udfUser.routes) ~
              crossDomainHandler(jobUser.routes) ~
              crossDomainHandler(user.routes) ~
              crossDomainHandler(monitor.routes)
          } ~
          pathPrefix("app") {
            crossDomainHandler(flowApp.routes) ~
              crossDomainHandler(jobApp.routes) ~
              crossDomainHandler(instanceApp.routes) ~
              crossDomainHandler(nsDatabaseApp.routes) ~
              crossDomainHandler(nsApp.routes)
          }
      }
}
