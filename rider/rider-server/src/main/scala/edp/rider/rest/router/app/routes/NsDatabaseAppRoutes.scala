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
@Path("/app/databases")
class NsDatabaseAppRoutes(modules: ConfigurationModule with PersistenceModule with BusinessModule with RoutesModuleImpl) extends Directives {

  lazy val routes: Route = postDatabaseRoute ~ getDatabaseByFilterRoute

  lazy val basePath = "databases"


  //  @Path("/{id}")
  //  @ApiOperation(value = "get one database from system by id", notes = "", nickname = "", httpMethod = "GET")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "database id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def getDatabaseByIdRoute: Route = modules.databaseAdminService.getByIdRoute(basePath)


  @ApiOperation(value = "get database id", notes = "", nickname = "", httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "type", value = "system type", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "nsInstance", value = "instance name", required = true, dataType = "string", paramType = "query"),
    new ApiImplicitParam(name = "nsDatabaseName", value = "database name", required = true, dataType = "string", paramType = "query")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "OK"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 403, message = "user is not app"),
    new ApiResponse(code = 404, message = "not found"),
    new ApiResponse(code = 501, message = "the request url is not supported"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def getDatabaseByFilterRoute: Route = modules.databaseAdminService.getByFilterRoute(basePath)


  @ApiOperation(value = "Add new database to the system", notes = "", nickname = "", httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "database", value = "Database object to be added", required = true, dataType = "edp.rider.rest.persistence.entities.SimpleNsDatabase", paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "post success"),
    new ApiResponse(code = 403, message = "permission denied"),
    new ApiResponse(code = 400, message = "config is not json type"),
    new ApiResponse(code = 401, message = "authorization error"),
    new ApiResponse(code = 409, message = "database already exists"),
    new ApiResponse(code = 451, message = "request process failed"),
    new ApiResponse(code = 500, message = "internal server error")
  ))
  def postDatabaseRoute: Route = modules.databaseAdminService.postRoute(basePath)


  //  @ApiOperation(value = "update database in the system", notes = "", nickname = "", httpMethod = "PUT")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "database", value = "Database object to be updated", required = true, dataType = "edp.rider.rest.persistence.entities.NsDatabase", paramType = "body")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "put success"),
  //    new ApiResponse(code = 400, message = "config is not json type"),
  //    new ApiResponse(code = 403, message = "user is not admin"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def putDatabaseRoute: Route = modules.databaseAdminService.putRoute(basePath)
  //
  //  @Path("/{id}/")
  //  @ApiOperation(value = "delete one database from system by id", notes = "", nickname = "", httpMethod = "DELETE")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "id", value = "database id", required = true, dataType = "integer", paramType = "path")
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "OK"),
  //    new ApiResponse(code = 401, message = "authorization error"),
  //    new ApiResponse(code = 403, message = "user is not admin user"),
  //    new ApiResponse(code = 412, message = "user still has some projects"),
  //    new ApiResponse(code = 451, message = "request process failed"),
  //    new ApiResponse(code = 500, message = "internal server error")
  //  ))
  //  def deleteByIdRoute: Route = modules.databaseAdminService.deleteRoute(basePath)
}

