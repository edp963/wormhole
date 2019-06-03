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

import edp.rider.common.RiderConfig
import edp.rider.rest.router.SessionClass
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim, JwtHeader}
import edp.rider.rest.util.AuthorizationProvider._
import edp.wormhole.util.JsonUtils

object JwtSupport {

  private val typ = "JWT"
  private val secret = RiderConfig.riderServer.tokenKey
  private val timeout = RiderConfig.tokenTimeout * 24 * 3600
  private val algorithm = JwtAlgorithm.HS256
  private val header = JwtHeader(algorithm, typ)

  def generateToken(session: SessionClass): String = {
    val claim = JwtClaim(JsonUtils.caseClass2json(genCurrentSession(session))).expiresIn(timeout)
    Jwt.encode(header, claim, secret)
  }

//  def generatePermanentToken(session: SessionClass): String = {
//    val claim = JwtClaim(caseClass2json(genCurrentSession(session)))
//    Jwt.encode(header, claim, secret)
//  }

  def decodeToken(token: String): SessionClass = {
    val decodeToken = Jwt.decodeRawAll(token, secret, Seq(algorithm))
    val session = JsonUtils.json2caseClass[SessionClass](decodeToken.get._2)
    session
  }

}
