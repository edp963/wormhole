/*-
 * <<
 * Davinci
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

import java.util.Properties
import javax.naming.directory.{InitialDirContext, SearchControls, SearchResult}
import javax.naming.{Context, NamingEnumeration}

import edp.rider.common.{RiderConfig, RiderLogger}

import scala.util.{Failure, Success, Try}

object LdapValidate extends LdapValidate

trait LdapValidate extends RiderLogger {

  private var context: InitialDirContext = _

  def validate(username: String, password: String): Boolean = {
    try {
      try {
        propsSet(RiderConfig.ldap.url + RiderConfig.ldapDc, RiderConfig.ldap.user, RiderConfig.ldap.pwd)
        val controls: SearchControls = new SearchControls
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
        val answers: NamingEnumeration[SearchResult] = context.search("", s"userPrincipalName=$username", controls)
        val nsName: String = answers.nextElement.getNameInNamespace
        val result = Try {
          this.propsSet(RiderConfig.ldap.url, nsName, password)
        }
        result match {
          case Success(_) => true
          case Failure(_) => false
        }
      } catch {
        case e: Throwable =>
          riderLogger.error(s"$username LdapValidate failed", e)
          false
      } finally {
        context.close()
      }
    } catch {
      case e: Throwable =>
        riderLogger.error(s"Ldap context initial failed", e)
        false
    }
  }

  private def propsSet(url: String, nsName: String, pwd: String) = {
    val props = new Properties
    props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    props.put(Context.PROVIDER_URL, url)
    props.put(Context.SECURITY_AUTHENTICATION, "simple")
    props.put(Context.SECURITY_PRINCIPAL, nsName)
    props.put(Context.SECURITY_CREDENTIALS, pwd)
    props.put("com.sun.jndi.ldap.read.timeout", RiderConfig.ldap.readTimeout.toString)
    props.put("com.sun.jndi.ldap.connect.timeout", RiderConfig.ldap.connectTimeout.toString)
    props.put("com.sun.jndi.ldap.connect.pool", RiderConfig.ldap.connectPoolEnabled.toString)
    context = new InitialDirContext(props)
  }
}
