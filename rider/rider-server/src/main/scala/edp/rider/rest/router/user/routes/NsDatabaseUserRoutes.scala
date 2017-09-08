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


package edp.rider.rest.router.user.routes

import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module._
import io.swagger.annotations._

@Api(value = "/databases", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class NsDatabaseUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = getDbByFilterRoute

  lazy val basePath = "projects"

  @Path("/{id}/instances/{instanceId}/databases/")
  @ApiOperation(value = "get databases by filter", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = false, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "instanceId", value = "instance id", required = false, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getDbByFilterRoute: Route = modules.databaseUserService.getByFilterRoute(basePath)


}

