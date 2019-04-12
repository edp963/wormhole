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

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.directives.Credentials
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.module.{ConfigurationModuleImpl, PersistenceModuleImpl}
import edp.rider.rest.persistence.entities.User
import edp.rider.rest.router.{LoginClass, LoginResult, SessionClass}
import edp.rider.rest.util.CommonUtils._
import slick.jdbc.MySQLProfile.api._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

sealed abstract class AuthorizationError extends Exception {
  val statusCode = Unauthorized
}

object UserNotFound extends AuthorizationError {

  override val statusCode = NotFound

}

object PassWordNotMatch extends AuthorizationError {
  override val statusCode = ClientError(210)("Wrong password", "Wrong password")

}

object AppForbidden extends AuthorizationError {
  override val statusCode = ClientError(418)("app type user has no permission to login", "app type user has no permission to login")
}

object AuthorizationProvider extends ConfigurationModuleImpl with PersistenceModuleImpl with RiderLogger {

  def createSessionClass(login: LoginClass, app: Boolean = false): Future[Either[AuthorizationError, LoginResult]] = {
    try {
      val user = findUser(login)
      user.flatMap {
        user =>
          relProjectUserDal.findByFilter(rel => rel.userId === user.id && rel.active === true).map[LoginResult] {
            relSeq =>
              val projectIdList = new ListBuffer[Long]
              if (relSeq.nonEmpty) relSeq.foreach(projectIdList += _.projectId)
              if (user.roleType == "app" && !app) throw AppForbidden
              else LoginResult(user, SessionClass(user.id, projectIdList.toList, user.roleType))
          }
      }.map(Right(_)).recover {
        case e: AuthorizationError => Left(e)
      }
    } catch {
      case e: AuthorizationError => Future.successful(Left(e))
    }

  }

  def genCurrentSession(session: SessionClass): SessionClass = {
    val projectIds = Await.result(relProjectUserDal.findByFilter(_.userId === session.userId), minTimeOut).map(_.projectId)
    SessionClass(session.userId, projectIds.toList, session.roleType)
  }

  def authorize(credentials: Credentials): Future[Option[SessionClass]] =
    credentials match {
      case p@Credentials.Provided(token) => validateToken(token)
      case _ => Future.successful(None)
    }


  private def findUser(login: LoginClass): Future[User] = {
    if (RiderConfig.ldap.enabled && LdapValidate.validate(login.email, login.password)) findUserByLdap(login)
    else userDal.findByFilter(user => user.email === login.email).map[User] {
      userSeq =>
        userSeq.headOption match {
          case Some(user) =>
            if (verifyPassWord(user.password, login.password)) user
            else throw PassWordNotMatch
          case None => throw UserNotFound
        }
    }
  }

  private def findUserByLdap(login: LoginClass): Future[User] = {
    val ldapUser = User(0, login.email, "", login.email.split("@")(0), "user", RiderConfig.riderServer.defaultLanguage, active = true, currentSec, 0, currentSec, 0)
    userDal.findByFilter(user => user.email === login.email && user.active === true).map[User] {
      userSeq =>
        userSeq.headOption match {
          case Some(user) => user
          case None =>
            riderLogger.info(s"user ${login.email} first login.")
            Await.result(userDal.insert(ldapUser), minTimeOut)
        }
    }
  }

  def validateToken(token: String): Future[Option[SessionClass]] = {
    try {
      val session = JwtSupport.decodeToken(token)
//      riderLogger.info("token validate success.")
      Future.successful(Some(session))
    } catch {
      case ex: Exception =>
        riderLogger.error("token validate failed.")
        Future.successful(None)
    }
  }


  private def verifyPassWord(storePass: String, pass: String): Boolean = {
    if (storePass == pass) true else false
  }


}
