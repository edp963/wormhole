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

@Api(value = "/namespaces", consumes = "application/json", produces = "application/json")
@Path("/admin")
class NamespaceAdminRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = postNamespaceRoute ~ putNamespaceRoute ~ getNamespaceByAllRoute ~ getNamespaceByIdRoute ~ getNsByProjectIdRoute

  lazy val basePath = "namespaces"

  @Path("/namespaces/{id}")
  @ApiOperation(value = "get one namespace from system by id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "namespace id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getNamespaceByIdRoute: Route = modules.namespaceAdminService.getByIdRoute(basePath)

  @Path("/namespaces")
  @ApiOperation(value = "get all namespaces", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "visible", value = "true or false", required = false, dataType = "boolean", paramType = "query"),
    new ApiImplicitParam(name = "instanceId", value = "instance id", required = false, dataType = "integer", paramType = "query"),
    new ApiImplicitParam(name = "databaseId", value = "database id", required = false, dataType = "integer", paramType = "query"),
    new ApiImplicitParam(name = "tableNames", value = "table names", required = false, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getNamespaceByAllRoute: Route = modules.namespaceAdminService.getByAllRoute(basePath)

  @Path("/namespaces")
  @ApiOperation(value = "add tables to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "simpleNamespace", value = "Namespace object to be inserted", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleNamespace", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 409, message = "some tables already exist"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postNamespaceRoute: Route = modules.namespaceAdminService.postRoute(basePath)

  @Path("/namespaces")
  @ApiOperation(value = "update namespace in the system", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "namespace", value = "Namespace object to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.Namespace", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def putNamespaceRoute: Route = modules.namespaceAdminService.putRoute(basePath)

  @Path("/projects/{id}/namespaces")
  @ApiOperation(value = "get one project's namespaces selected information from system by id", notes = "", nickname = "", httpMethod = "GET")
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
  def getNsByProjectIdRoute: Route = modules.namespaceAdminService.getByProjectIdRoute("projects")
}

