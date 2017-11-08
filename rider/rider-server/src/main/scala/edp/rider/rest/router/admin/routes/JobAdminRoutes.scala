package edp.rider.rest.router.admin.routes

import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule, RoutesModuleImpl}
import io.swagger.annotations._

@Api(value = "/jobs", consumes = "application/json", produces = "application/json")
@Path("/admin")
class JobAdminRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = getJobByAllRoute ~ getLogByJobId ~ getJobByProjectIdRoute

  lazy val basePath = "jobs"

  @Path("/jobs")
  @ApiOperation(value = "get all jobs", notes = "", nickname = "", httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJobByAllRoute: Route = modules.jobAdminService.getByAllRoute(basePath)

  @Path("/projects/{projectId}/jobs/{jobId}/logs/")
  @ApiOperation(value = "get job log by job id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "jobId", value = "job id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getLogByJobId: Route = modules.jobAdminService.getLogByJobId("projects")


  @Path("/projects/{projectId}/jobs")
  @ApiOperation(value = "get one project's jobs from system by projectId", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not admin"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getJobByProjectIdRoute: Route = modules.jobAdminService.getByProjectIdRoute("projects")

}
