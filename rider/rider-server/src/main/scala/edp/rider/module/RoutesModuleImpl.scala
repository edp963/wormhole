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


package edp.rider.module

import edp.rider.rest.router.admin.api._
import edp.rider.rest.router.app.api.{FlowAppApi, JobAppApi, MonitorAppApi}
import edp.rider.rest.router.user.api._


trait RoutesModuleImpl {
  this: ConfigurationModule with PersistenceModule =>

  lazy val instanceAdminService = new InstanceAdminApi(instanceDal)
  lazy val databaseAdminService = new NsDatabaseAdminApi(databaseDal)
  lazy val namespaceAdminService = new NamespaceAdminApi(namespaceDal, databaseDal, relProjectNsDal)
  lazy val streamAdminService = new StreamAdminApi(streamDal)
  lazy val flowAdminService = new FlowAdminApi(flowDal, streamDal)
  lazy val userAdminService = new UserAdminApi(userDal, relProjectUserDal)
  lazy val projectAdminService = new ProjectAdminApi(projectDal, relProjectNsDal, relProjectUserDal, relProjectUdfDal)
  lazy val monitorAdminService = new MonitorAdminApi(streamDal)
  lazy val udfAdminService = new UdfAdminApi(udfDal, relProjectUdfDal)


  lazy val userService = new UserApi(userDal, relProjectUserDal)
  lazy val projectUserService = new ProjectUserApi(projectDal, relProjectUserDal)
  lazy val namespaceUserService = new NamespaceUserApi(namespaceDal, relProjectNsDal)
  lazy val streamUserService = new StreamUserApi(streamDal, projectDal, relStreamUdfDal, inTopicDal, flowDal)
  lazy val flowUserService = new FlowUserApi(flowDal, streamDal)
  lazy val actionUserService = new ActionUserApi(streamDal, flowDal)
  lazy val monitorUserService = new MonitorUserApi(streamDal)
  lazy val instanceUserService = new InstanceUserApi(relProjectNsDal)
  lazy val databaseUserService = new NsDatabaseUserApi(databaseDal)
  lazy val jobUserService = new JobUserApi(jobDal, projectDal)
  lazy val udfUserService = new UdfUserApi(udfDal, relProjectUdfDal)


  lazy val jobAppService = new JobAppApi(jobDal, projectDal)
  lazy val flowAppService = new FlowAppApi(flowDal, streamDal, projectDal)
  lazy val monitorAppService = new MonitorAppApi(flowDal, projectDal, streamDal, jobDal, feedbackFlowErrDal, feedbackOffsetDal)


}


