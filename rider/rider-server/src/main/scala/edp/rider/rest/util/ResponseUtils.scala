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


package edp.rider.rest.util

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.RespondWithDirectives._
import edp.rider.rest.router.{ResponseHeader, ResponseJson, SessionClass}
import edp.rider.rest.util.JwtSupport._

object ResponseUtils {

  val msgMap = Map(200 -> "Success",
    210 -> "Wrong password",
    401 -> "Unauthorized",
    403 -> "Insufficient Permission",
    404 -> "Not found",
    418 -> "app type user has no permission to login",
    451 -> "Failed",
    501 -> "Not supported",
    406 -> "action is forbidden",
    507 -> "resource is not enough")

  def responseHeaderWithToken(session: SessionClass): Directive0 = {
    respondWithHeader(RawHeader("token", generateToken(session)))
  }

  def getHeader(code: Int, session: SessionClass): ResponseHeader = {
    if (session != null)
      ResponseHeader(code, msgMap(code), generateToken(session))
    else
      ResponseHeader(code, msgMap(code))
  }

  def getHeader(code: Int, msg: String, session: SessionClass): ResponseHeader = {
    if (session != null)
      ResponseHeader(code, msg, generateToken(session))
    else
      ResponseHeader(code, msg)
  }

  def setSuccessResponse(session: SessionClass): ResponseJson[String] =
    ResponseJson[String](getHeader(200, session), msgMap(200))

  def setFailedResponse(session: SessionClass, msg: String = "Failed"): ResponseJson[String] =
    ResponseJson[String](getHeader(451, session), msg)
}

