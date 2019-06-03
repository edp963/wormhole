package edp.rider.rest.router.user.routes

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.common.RiderLogger
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule, RoutesModuleImpl}
import edp.rider.rest.router.JsonSerializer
import io.swagger.annotations._
import javax.ws.rs.Path

@Api(value = "/monitor", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class MonitorRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives  with RiderLogger with JsonSerializer{

  lazy val routes: Route = getMonitorInfoByFlowIdRoute ~ getMonitorInfoByStreamIdRoute
  lazy val basePath = "projects"

  @Path("/monitor/{projectId}/flow/{flowId}")
  @ApiOperation(value = "get monitor info from system by flow id", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "span", value = "time span", required = true, dataType = "edp.rider.rest.persistence.entities.MonitorTimeSpan", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getMonitorInfoByFlowIdRoute:Route =modules.monitorService.getMonitorInfoByFlowId(basePath)

  @Path("/monitor/{projectId}/stream/{streamId}")
  @ApiOperation(value = "get monitor info from system by stream id", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "projectId", value = "project id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "span", value = "time span", required = true, dataType = "edp.rider.rest.persistence.entities.MonitorTimeSpan", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getMonitorInfoByStreamIdRoute:Route=modules.monitorService.getMonitorInfoByStreamId(basePath)
}
