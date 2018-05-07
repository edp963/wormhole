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


package edp.rider.rest.router.app.routes

import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module._
import edp.rider.rest.router.JsonSerializer
import io.swagger.annotations._

@Api(value = "/app", consumes = "application/json", produces = "application/json")
@Path("/app/instances")
class InstanceAppRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives with JsonSerializer {

  lazy val routes: Route = postInstanceRoute ~ getInstanceByFilterRoute

  lazy val basePath = "instances"


  //  @Path("/{id}")
  //  @ApiOperation(value = "get one instance from system by id", notes = "", nickname = "", httpMethod = "GET")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "instance id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def getInstanceByIdRoute: Route = modules.instanceAdminService.getByIdRoute(basePath)


  @ApiOperation(value = "get instance id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "type", value = "instance type", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "nsInstance", value = "nsInstance input", required = true, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not app"),
    new ApiResponse(code = 404, message = "not found"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getInstanceByFilterRoute: Route = modules.instanceAdminService.getByFilterRoute(basePath)


  @ApiOperation(value = "Add new instance to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "instance", value = "Instance object to be inserted", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleInstance", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 400, message = "conn_url format is wrong, please alter it as http://ip:port"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "permission denied"),
    new ApiResponse(code = 409, message = "conn_url or instance already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postInstanceRoute: Route = modules.instanceAdminService.postRoute(basePath)


  //  @ApiOperation(value = "update instance in the system", notes = "", nickname = "", httpMethod = "PUT")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "instance", value = "Instance object to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.Instance", paramType = "body")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "put success"),
  //    new ApiResponse(code = 400, message = "conn_url format is wrong, please alter it as http://ip:port"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin"),
  //    new ApiResponse(code = 409, message = "conn_url or instance already exists"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def putInstanceRoute: Route = modules.instanceAdminService.putRoute(basePath)

  //  @Path("/{id}/databases")
  //  @ApiOperation(value = "get all database from system by instance id", notes = "", nickname = "", httpMethod = "GET")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "instance id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def getDbByIdRoute: Route = modules.databaseAdminService.getDbByInstanceIdRoute(basePath)
  //
  //  @Path("/{id}/")
  //  @ApiOperation(value = "delete one instance from system by id", notes = "", nickname = "", httpMethod = "DELETE")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "instance id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin user"),
  //    new ApiResponse(code = 412, message = "user still has some projects"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def deleteByIdRoute: Route = modules.instanceAdminService.deleteRoute(basePath)

}

