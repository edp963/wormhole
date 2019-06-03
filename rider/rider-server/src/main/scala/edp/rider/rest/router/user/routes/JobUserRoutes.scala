package edp.rider.rest.router.user.routes

import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule, RoutesModuleImpl}
import io.swagger.annotations._


@Api(value = "/jobs", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class JobUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives{
  lazy val routes: Route = postRoute ~ startJob ~ getJobByIdRoute ~ getJobByFilterRoute ~ stopJob ~ deleteJob ~ getLogByJobId ~ reviseRoute ~ getDataVersions
  lazy val basePath = "projects"

  @Path("/{projectId}/jobs")
  @ApiOperation(value = "Add new job to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "job", value = "Job object to be added", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleJob", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 403, message = "user is not user"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 409, message = "job already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postRoute: Route = modules.jobUserService.postRoute(basePath)

  @Path("/{projectId}/jobs")
  @ApiOperation(value = "revise job to the system", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "job", value = "Job object to be revised", required = true, dataType = "edp.rider.rest.persistence.entities.Job", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "put success"),
    new ApiResponse(code = 403, message = "user is not user"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 409, message = "job already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def reviseRoute: Route = modules.jobUserService.putRoute(basePath)


  @Path("/{projectId}/jobs/{jobId}/start")
  @ApiOperation(value = "start job by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
     ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "start is forbidden"),
    new ApiResponse(code = 507, message = "resource not enough"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def startJob: Route = modules.jobUserService.startJob(basePath)

  @Path("/{projectId}/jobs/{jobId}/stop")
  @ApiOperation(value = "stop job by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "stop is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def stopJob: Route = modules.jobUserService.stopJob(basePath)

  @Path("/{projectId}/jobs/{jobId}/delete")
  @ApiOperation(value = "delete job by id", notes = "", nickname = "", httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "jobId id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 406, message = "delete is forbidden"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def deleteJob: Route = modules.jobUserService.deleteJob(basePath)


  @Path("/{projectId}/jobs/{jobId}")
  @ApiOperation(value = "get one job from system", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJobByIdRoute: Route = modules.jobUserService.getByIdRoute(basePath)

  @Path("/{projectId}/jobs")
  @ApiOperation(value = "check source sink existence from system, refresh, check the name of job", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "sourceNs", value = "source namespace", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "sinkNs", value = "sink namespace", required = false, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "jobName", value = "job name", required = false, dataType = "string", paramType = "query")

  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJobByFilterRoute: Route = modules.jobUserService.getByFilterRoute(basePath)


  @Path("/{projectId}/jobs/{jobId}/logs/")
  @ApiOperation(value = "get job log by job id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getLogByJobId: Route = modules.jobUserService.getLogByJobId(basePath)


  @Path("/{projectId}/jobs/{jobId}/heartbeat/latest")
  @ApiOperation(value = "get hdfslog stream of job source namespace latest heartbeat time by job id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getLatestHeartbeatById: Route = modules.jobUserService.getLogByJobId(basePath)


  @Path("/{projectId}/jobs/dataversions")
  @ApiOperation(value = "get hdfs data versions by namespace", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "namespace", value = "source namespace", required = true, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getDataVersions: Route = modules.jobUserService.getDataVersions(basePath)
}
