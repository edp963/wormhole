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

import java.net.InetAddress

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import edp.wormhole.spark.log.EdpLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object CassandraConnection extends EdpLogging {
  val cassandraSessionMap: mutable.HashMap[List[java.net.InetAddress], Session] = new mutable.HashMap[List[java.net.InetAddress], Session]

  def getSortedAddress(nodes: String): List[java.net.InetAddress] = {
    val nodeArray = nodes.split(",")
    val sortedNodeArray = nodeArray.sorted
    val addresses = ListBuffer.empty[InetAddress]
    for (host <- sortedNodeArray) {
      addresses += InetAddress.getByName(host)
    }
    addresses.toList
  }

  def getSession(addrs: List[java.net.InetAddress], user: String, password: String) = {
    if (!cassandraSessionMap.contains(addrs)) {
      synchronized {
        if (!cassandraSessionMap.contains(addrs)) {
          try {
            var builder: Builder = Cluster.builder().addContactPoints(addrs.asJava)
            if (user != null && password != null) {
              builder = builder.withCredentials(user, password)
            }
            val session = builder.build().connect()
            cassandraSessionMap(addrs) = session
          } catch {
            case e: Throwable => logError("getSession:", e)
          }
        }
      }
    }
    cassandraSessionMap(addrs)
  }
}
