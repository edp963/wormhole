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


package edp.rider.rest.router

import edp.rider.rest.persistence.entities.User

case class LoginClass(email: String, password: String)

case class SessionClass(userId: Long, projectIdList: List[Long], roleType: String, currentTs: Long = System.currentTimeMillis())

case class LoginResult(user: User, session: SessionClass)

case class ChangeUserPwdClass(id: Long, oldPass: Option[String], newPass: String)

case class ActionClass(action: String, flowIds: String)

case class ResponseHeader(code: Int, msg: String, token: String = "")

case class ResponseJson[A](header: ResponseHeader, payload: A)

case class ResponseSeqJson[A](header: ResponseHeader, payload: Seq[A])
