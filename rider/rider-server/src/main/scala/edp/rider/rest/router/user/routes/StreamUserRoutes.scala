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
import edp.rider.rest.router.JsonSerializer
import io.swagger.annotations.{ApiResponses, _}


@Api(value = "/streams", consumes = "application/json", produces = "application/json")
@Path("/user")
class StreamUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives with JsonSerializer {
  lazy val routes: Route = getStreamByAllRoute ~ putStreamRoute ~ postStreamRoute ~ renewRoute ~
    getStreamById ~ getLogByStreamId ~ stop ~ startRoute ~ deleteStream ~ getSparkConf ~ getTopics ~ getJvmConf ~
    postUserDefinedTopic ~ getUdfs ~ postTopicsOffset ~ getDefaultConfig ~ getYarnUi ~ getFlowPrioritiesByStreamId ~
    updateFlowOrder
  //  ~ deleteUserDefinedTopic

  lazy val basePath = "projects"

  @Path("/projects/{id}/streams")
  @ApiOperation(value = "get streams from system by project id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamName", value = "stream name", required = false, dataType = "string", paramType = "query", allowMultiple = false),
    new ApiImplicitParam(name = "streamType", value = "stream type", required = false, dataType = "string", paramType = "query", allowMultiple = false),
    new ApiImplicitParam(name = "functionType", value = "function type", required = false, dataType = "string", paramType = "query", allowMultiple = false)
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error"),
    new ApiResponse(code = 501, message = "the request url is not supported")
  ))
  def getStreamByAllRoute: Route = modules.streamUserService.getByFilterRoute(basePath)


  @Path("/projects/{id}/streams/{streamId}/")
  @ApiOperation(value = "get stream from system by stream id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getStreamById: Route = modules.streamUserService.getByIdRoute(basePath)


  @Path("/projects/{id}/streams")
  @ApiOperation(value = "update stream of the system", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "updateStream", value = "Stream object to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.PutStream", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 400, message = "config is not the right format"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def putStreamRoute: Route = modules.streamUserService.putRoute(basePath)

  @Path("/projects/{id}/streams")
  @ApiOperation(value = "post streams to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "stream", value = "Stream object to be added", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleStream", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 400, message = "config is not the right format"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error"),
    new ApiResponse(code = 409, message = "duplicate key")
  ))
  def postStreamRoute: Route = modules.streamUserService.postRoute(basePath)

  @Path("/projects/{id}/streams/{streamId}/logs/")
  @ApiOperation(value = "get stream log by stream id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getLogByStreamId: Route = modules.streamUserService.getLogByStreamId(basePath)

  @Path("/projects/{id}/streams/{streamId}/start")
  @ApiOperation(value = "start stream by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamDirective", value = "topics offset and udfs information", required = false, dataType = "edp.rider.rest.persistence.entities.StreamDirective", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "action is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def startRoute: Route = modules.streamUserService.startRoute(basePath)

  @Path("/projects/{id}/streams/{streamId}/stop")
  @ApiOperation(value = "stop stream by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "action is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def stop: Route = modules.streamUserService.stopRoute(basePath)

  @Path("/projects/{id}/streams/{streamId}/renew")
  @ApiOperation(value = "update topic and add udf directive to zk by stream id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamDirective", value = "update topics offset and add udf information", required = true, dataType = "edp.rider.rest.persistence.entities.StreamDirective", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "action is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def renewRoute: Route = modules.streamUserService.renewRoute(basePath)

  @Path("/projects/{id}/streams/{streamId}/delete")
  @ApiOperation(value = "delete stream by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 412, message = "can't delete stream now, please delete flow first"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def deleteStream: Route = modules.streamUserService.deleteStream(basePath)


  @Path("/projects/streams/default/config/jvm")
  @ApiOperation(value = "get default stream resource config", notes = "", nickname = "", httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJvmConf: Route = modules.streamUserService.getDefaultJvmConf(basePath)

  @Path("/projects/streams/default/config/spark")
  @ApiOperation(value = "get default spark config", notes = "", nickname = "", httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getSparkConf: Route = modules.streamUserService.getDefaultSparkConf(basePath)

  @Path("/projects/{id}/streams/{streamId}/topics")
  @ApiOperation(value = "get topics detail", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getTopics: Route = modules.streamUserService.getTopicsRoute(basePath)

  // post /user/projects/1/streams/1/topics/userdefined
  @Path("/projects/{id}/streams/{streamId}/topics/userdefined")
  @ApiOperation(value = "get userdefined topic offsets", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "topicName", value = "topic name", required = true, dataType = "edp.rider.rest.persistence.entities.PostUserDefinedTopic", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postUserDefinedTopic: Route = modules.streamUserService.postUserDefinedTopicRoute(basePath)

  // post /user/projects/1/streams/1/topics
  @Path("/projects/{id}/streams/{streamId}/topics")
  @ApiOperation(value = "get topic offsets by request topics", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "topics", value = "topics name", required = true, dataType = "edp.rider.rest.persistence.entities.GetTopicsOffsetRequest", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postTopicsOffset: Route = modules.streamUserService.postTopicsOffsetRoute(basePath)


  //  // delete /user/projects/1/streams/1/topics/userdefined/
  //  @Path("/{id}/streams/{streamId}/topics/userdefined/{topicId}")
  //  @ApiOperation(value = "delete stream userdefined topic by id", notes = "", nickname = "", httpMethod = "DELETE")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
  //    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
  //    new ApiImplicitParam(name = "topicId", value = "topic id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 412, message = "can't delete stream now, please delete flow first"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not normal"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def deleteUserDefinedTopic: Route = modules.streamUserService.deleteUserDefinedTopicRoute(basePath)

  // get stream udfs
  // get /user/projects/1/streams/1/udfs
  @Path("/projects/{id}/streams/{streamId}/udfs")
  @ApiOperation(value = "get stream udfs", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getUdfs: Route = modules.streamUserService.getUdfsRoute(basePath)


  //  /streams/defaultconfigs?streamtype=spark
  @Path("/streams/defaultconfigs")
  @ApiOperation(value = "get stream default config", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "streamType", value = "streamType", required = true, dataType = "string",  paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getDefaultConfig: Route = modules.streamUserService.getDefaultConfig("streams")

  // get /user/projects/1/streams/1/udfs
  @Path("/projects/{id}/streams/{streamId}/yarnUi")
  @ApiOperation(value = "get stream yarnUi", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getYarnUi: Route = modules.streamUserService.getYarnUi(basePath)

  @Path("/projects/{id}/streams/{streamId}/flows/order")
  @ApiOperation(value = "get all flow priority info of the stream", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getFlowPrioritiesByStreamId: Route = modules.streamUserService.getFlowPrioritiesByStreamId(basePath)

  @Path("/projects/{id}/streams/{streamId}/flows/order")
  @ApiOperation(value = "update flow priorities under stream", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowPriorities", value = "flow priorities to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.FlowPriorities", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def updateFlowOrder: Route = modules.streamUserService.updateFlowOrder(basePath)
}
