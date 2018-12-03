package edp.rider.rest.router.user.routes

import akka.http.scaladsl.server.{Directives, Route}
import edp.rider.module.{BusinessModule, ConfigurationModule, PersistenceModule, RoutesModuleImpl}
import io.swagger.annotations._
import javax.ws.rs.Path

@Api(value = "/monitor", consumes = "application/json", produces = "application/json")
@Path("/user/projects")
class MonitorRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives  {

  lazy val routes: Route = getMonitorInfoByFlowIdRoute ~ getMonitorInfoByStreamIdRoute
  lazy val basePath = "projects"

  @Path("/{projectId}/flow/{flowId}")
  @ApiOperation(value = "get monitor info from system by flow id", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "flowId", value = "flow id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "startTime", value = "pre time", required = true, dataType = "long", paramType = "body"),
    new ApiImplicitParam(name = "endTime", value = "current time", required = true, dataType = "long", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getMonitorInfoByFlowIdRoute=modules.monitorService.getMonitorInfoByFlowId(basePath)

  @Path("/{projectId}/stream/{streamId}")
  @ApiOperation(value = "get monitor info from system by stream id", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "streamId", value = "stream id", required = true, dataType = "integer", paramType = "path"),
    new ApiImplicitParam(name = "startTime", value = "pre time", required = true, dataType = "long", paramType = "body"),
    new ApiImplicitParam(name = "endTime", value = "current time", required = true, dataType = "long", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not normal user"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getMonitorInfoByStreamIdRoute=modules.monitorService.getMonitorInfoByStreamId(basePath)
}
