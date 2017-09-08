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

import com.alibaba.fastjson.JSON
import edp.rider.common.RiderLogger
import edp.rider.rest.persistence.entities.{Instance, Namespace, NsDatabase}

object NamespaceUtils extends RiderLogger {

  def generateStandardNs(ns: Namespace) = Seq(ns.nsSys, ns.nsInstance, ns.nsDatabase, ns.nsTable, ns.nsVersion, ns.nsDbpar, ns.nsTablepar).mkString(".")

  def getConnUrl(instance: Instance, db: NsDatabase) = {

      instance.nsSys match {
        case "mysql" => s"jdbc:mysql://${instance.connUrl}/${db.nsDatabase}"
        case "oracle" =>
          val hostPort = instance.connUrl.split(":")
          val serviceName = db.config match {
            case Some(conf) =>
              if (conf != "") {
                if (JSON.parseObject(conf).containsKey("service_name"))
                  JSON.parseObject(conf).getString("service_name")
                else if (JSON.parseObject(conf).containsKey("SERVICE_NAME"))
                  JSON.parseObject(conf).getString("SERVICE_NAME")
                else ""
              } else ""
            case None => ""
          }
          s"jdbc:oracle:thin:@(DESCRIPTION=(FAILOVER = yes)(ADDRESS = (PROTOCOL = TCP)(HOST =${hostPort(0)})(PORT = ${hostPort(1)}))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = $serviceName)))"
        case _ => instance.connUrl
      }

  }
}
