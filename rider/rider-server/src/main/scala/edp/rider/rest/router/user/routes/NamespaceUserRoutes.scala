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

@Api(value = "/namespaces", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class NamespaceUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = filterFlowNsByProjectIdRoute ~ getNsByProjectIdRoute ~ getUmsInfoRoute ~ getSinkInfoRoute ~ getTopicRoute

  lazy val basePath = "projects"

  @Path("/{projectId}/streams/{streamId}/namespaces")
  @ApiOperation(value = "get namespaces of the project", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "instanceType", value = "instance namespace type", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "sourceType", value = "instance namespace type", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "sinkType", value = "sink namespace type", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "transType", value = "transformation namespace type", required = false, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def filterFlowNsByProjectIdRoute: Route = modules.namespaceUserService.filterFlowNsByProjectId(basePath)


  @Path("/{id}/namespaces")
  @ApiOperation(value = "get namespaces of the project", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "sourceType", value = "source namespace type", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "sinkType", value = "sink namespace type", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "transType", value = "transformation namespace type", required = false, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getNsByProjectIdRoute: Route = modules.namespaceUserService.getNsByProjectId(basePath)

  @Path("/{id}/namespaces/{id}/schema/source")
  @ApiOperation(value = "get namespace config in the system", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "nsId", value = "namespace id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUmsInfoRoute: Route = modules.namespaceUserService.getUmsInfoByIdRoute(basePath)

  @Path("/{id}/namespaces/{nsId}/schema/sink")
  @ApiOperation(value = "get namespace config in the system", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "nsId", value = "namespace id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getSinkInfoRoute: Route = modules.namespaceUserService.getSinkInfoByIdRoute(basePath)

  @Path("/{id}/namespaces/{nsId}/topic")
  @ApiOperation(value = "get namespace topic", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "nsId", value = "namespace id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getTopicRoute: Route = modules.namespaceUserService.getTopicRoute(basePath)
}

