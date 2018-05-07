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


package edp.rider.rest.router.admin.routes


import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module._
import io.swagger.annotations._

@Api(value = "/users", consumes = "application/json", produces = "application/json")
@Path("/admin")
class UserAdminRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = getUserByFilterRoute ~ postUserRoute ~ putUserRoute ~ getUserByIdRoute ~ getUserByProjectIdRoute ~ deleteByIdRoute

  lazy val basePath = "users"

  @Path("/users/{id}")
  @ApiOperation(value = "get one user from system by id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "user id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUserByIdRoute: Route = modules.userAdminService.getById(basePath)

  @Path("/{id}/")
  @ApiOperation(value = "delete one user from system by id", notes = "", nickname = "", httpMethod = "DELETE")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "user id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin user"),
    new ApiResponse(code = 412, message = "user still has some projects"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def deleteByIdRoute: Route = modules.userAdminService.deleteRoute(basePath)

  @Path("/users")
  @ApiOperation(value = "get all users", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "visible", value = "true or false", required = false, dataType = "boolean", paramType = "query"),
    new ApiImplicitParam(name = "email", value = "user email", required = false, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUserByFilterRoute: Route = modules.userAdminService.getByFilterRoute(basePath)

  @Path("/users")
  @ApiOperation(value = "post user to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "user", value = "User object to be added", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleUser", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 409, message = "user already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postUserRoute: Route = modules.userAdminService.postRoute(basePath)

  @Path("/users")
  @ApiOperation(value = "update user in the system", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "user", value = "User object to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.User", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def putUserRoute: Route = modules.userAdminService.putRoute(basePath)

  @Path("/projects/{id}/users")
  @ApiOperation(value = "get one project's users selected information from system by id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUserByProjectIdRoute: Route = modules.userAdminService.getByProjectIdRoute("projects")
}

