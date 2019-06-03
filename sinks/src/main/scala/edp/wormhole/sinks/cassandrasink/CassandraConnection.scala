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


package edp.wormhole.sinks.cassandrasink

import java.net.{InetAddress, InetSocketAddress}

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object CassandraConnection {
  private lazy val logger = Logger.getLogger(this.getClass)
  val cassandraSessionMap: mutable.HashMap[List[java.net.InetSocketAddress], Session] = new mutable.HashMap[List[java.net.InetSocketAddress], Session]

  def getSortedAddress(nodes: String): List[java.net.InetSocketAddress] = {
    val nodeArray = nodes.split(",")
    val sortedNodeArray: Array[String] = nodeArray.sorted
    val addresses = ListBuffer.empty[InetSocketAddress]
    for (host <- sortedNodeArray) {
      val hostName=host.split("\\:")(0)
      val port=host.split("\\:")(1).toInt
      val ip=InetAddress.getByName(hostName)
      val ipAddress=new InetSocketAddress(ip,port)
      addresses +=ipAddress
    }
    addresses.toList
  }

  def getSession(addrs: List[java.net.InetSocketAddress], user: String, password: String) = {
    if (!cassandraSessionMap.contains(addrs)) {
      synchronized {
        if (!cassandraSessionMap.contains(addrs)) {
          try {
            var builder: Builder = Cluster.builder().addContactPointsWithPorts(addrs.asJava)
            if (user != null && password != null) {
              builder = builder.withCredentials(user, password)
            }
            val session = builder.build().connect()
            cassandraSessionMap(addrs) = session
          } catch {
            case e: Throwable => logger.error("getSession:", e)
          }
        }
      }
    }
    cassandraSessionMap(addrs)
  }
}
