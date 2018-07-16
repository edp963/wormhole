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

@Api(value = "/flows", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class FlowUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = postFlowRoute ~ getFlowByFilterRoute ~ putFlowRoute ~ getFlowByIdRoute ~ lookupSqlPermVerifyRoute ~ postFlowTopicsOffset ~ postUserDefinedTopic ~ getTopics ~ getUdfs ~ startRoute

  lazy val basePath = "projects"

  @Path("/{projectId}/streams/{streamId}/flows/{flowId}/")
  @ApiOperation(value = "get one flow from system", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getFlowByIdRoute: Route = modules.flowUserService.getByIdRoute(basePath)

  @Path("/{projectId}/flows")
  @ApiOperation(value = "get all flows from system", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "visible", value = "true or false", required = false, dataType = "boolean", paramType = "query"),
    new ApiImplicitParam(name = "sourceNs", value = "source namespace", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "sinkNs", value = "sink namespace", required = false, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getFlowByFilterRoute: Route = modules.flowUserService.getByFilterRoute(basePath)


  @Path("/{projectId}/streams/{streamId}/flows")
  @ApiOperation(value = "add new flow of the project", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flow", value = "Flow Object", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleFlow", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 400, message = "config is not json type"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 409, message = "this source namespace to sink namespace already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postFlowRoute: Route = modules.flowUserService.postRoute(basePath)


  @Path("/{projectId}/streams/{streamId}/flows")
  @ApiOperation(value = "update flow of the project", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flow", value = "Flow Object", required = true, dataType = "edp.rider.rest.persistence.entities.Flow", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 400, message = "config is not json type"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 409, message = "this source namespace to sink namespace already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def putFlowRoute: Route = modules.flowUserService.putRoute(basePath)


  @Path("/{projectId}/streams/{streamId}/flows/sqls/lookup/")
  @ApiOperation(value = "check spark sql tables permission", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "sql", value = "sql", required = true, dataType = "edp.rider.rest.persistence.entities.Sql", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 406, message = "not have permission to table"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def lookupSqlPermVerifyRoute: Route = modules.flowUserService.lookupSqlVerifyRoute(basePath)

   //post /user/projects/1/flows/1/topics
  @Path("/{projectId}/flows/{flowId}/topics")
  @ApiOperation(value = "get topic offsets by request topics", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "topics", value = "topics name", required = true, dataType = "edp.rider.rest.persistence.entities.GetTopicsOffsetRequest", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postFlowTopicsOffset: Route = modules.flowUserService.postFlowTopicsOffsetRoute(basePath)

  // post /user/projects/1/streams/1/topics/userdefined
  @Path("/{projectId}/flows/{flowId}/topics/userdefined")
  @ApiOperation(value = "get userdefined topic offsets", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "topicName", value = "topic name", required = true, dataType = "edp.rider.rest.persistence.entities.PostUserDefinedTopic", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postUserDefinedTopic: Route = modules.flowUserService.postFlowUserDefinedTopicRoute(basePath)

  @Path("/{projectId}/flows/{flowId}/topics")
  @ApiOperation(value = "get topics detail", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
    def getTopics: Route = modules.flowUserService.getFlowTopicsRoute(basePath)

  @Path("/{projectId}/flows/{flowId}/udfs")
  @ApiOperation(value = "get flow udfs", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUdfs: Route = modules.flowUserService.getFlowUdfsRoute(basePath)

  @Path("/{projectId}/flinkstreams/flows/{flowId}/start")
  @ApiOperation(value = "start flink flow by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowDirective", value = "topics offset and udfs information", required = false, dataType = "edp.rider.rest.persistence.entities.FlowDirective", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "action is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def startRoute: Route = modules.flowUserService.startFlinkRoute(basePath)
}

