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
import io.swagger.annotations._

@Api(value = "/app", consumes = "application/json", produces = "application/json")
@Path("/app/projects")
class JobAppRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = getJobHealthById ~ postRoute ~ stopRoute

  lazy val basePath = "projects"

  @Path("/{projectId}/jobs")
  @ApiOperation(value = "Add new job to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "job", value = "Job object to be added", required = true, dataType = "edp.rider.rest.persistence.entities.AppJob", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 403, message = "user is not app"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 409, message = "job already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postRoute: Route = modules.jobAppService.postRoute(basePath)

  @Path("/{projectId}/jobs/{jobId}/actions/stop")
  @ApiOperation(value = "insert/update/start job of the system", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "request failed for permission reason"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def stopRoute: Route = modules.jobAppService.stopRoute(basePath)


  @Path("/{projectId}/jobs/{jobId}/health")
  @ApiOperation(value = "get health status of stream from system by stream id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not app user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJobHealthById: Route = modules.monitorAppService.getJobHealthByIdRoute(basePath)

}

