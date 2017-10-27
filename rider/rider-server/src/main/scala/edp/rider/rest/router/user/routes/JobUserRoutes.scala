package edp.rider.rest.router.user.routes

import javax.ws.rs.Path

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule, RoutesModuleImpl}
import io.swagger.annotations._


@Api(value = "/jobs", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class JobUserRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives{
  lazy val routes: Route = postRoute ~ makeJob
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



  @Path("/{projectId}/jobs/{jobId}/start")
  @ApiOperation(value = "start job by id", notes = "", nickname = "", httpMethod = "GET")
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
  def makeJob: Route = modules.jobUserService.makeJob(basePath)



}
