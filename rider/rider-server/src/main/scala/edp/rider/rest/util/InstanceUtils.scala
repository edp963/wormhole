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

import scala.util.hashing.MurmurHash3._

object InstanceUtils {

  val tcp_url_ip_pattern = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])(,(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))*$".r.pattern

  val tcp_url_host_pattern = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]))*$".r.pattern

  val tcp_url_ip_port_pattern = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\:\\d+(,(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\:\\d+)*$".r.pattern

  val tcp_url_host_port_pattern = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])\\:\\d+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])\\:\\d+)*$".r.pattern

  val one_tcp_url_ip_port_pattern = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\:\\d+$".r.pattern

  val one_tcp_url_host_port_pattern = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])\\:\\d+$".r.pattern

  val http_url_ip_port_pattern = "http(s)?://(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])(\\:\\d+)?$".r.pattern

  val http_host_ip_port_pattern = "http(s)?://(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])(\\:\\d+)?$".r.pattern

  val zk_node_ip_pattern = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\:\\d+(\\/(.)+)*(,(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\:\\d+(\\/(.)+)*)*$".r.pattern

  val zk_node_host_pattern = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])\\:\\d+(\\/(.)+)*(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])\\:\\d+(\\/(.)+)*)*$".r.pattern

  val hdfs_path_pattern = "hdfs://[A-Za-z]+[A-Za-z0-9_-]*(:\\d+)*(/[A-Za-z]+[A-Za-z0-9_-]*)*".r.pattern

  def checkFormat(nsSys: String, url: String): Boolean = {
    nsSys.toLowerCase match {
      case "mysql" | "oracle" | "postgresql" | "vertica" | "phoenix" => one_tcp_url_host_port_pattern.matcher(url).matches() || one_tcp_url_ip_port_pattern.matcher(url).matches()
      case "kafka" | "redis" | "cassandra" => tcp_url_ip_port_pattern.matcher(url).matches() || tcp_url_host_port_pattern.matcher(url).matches()
      case "es" => http_url_ip_port_pattern.matcher(url).matches() || http_host_ip_port_pattern.matcher(url).matches() || one_tcp_url_host_port_pattern.matcher(url).matches() || one_tcp_url_ip_port_pattern.matcher(url).matches()
      case "hbase" => zk_node_ip_pattern.matcher(url).matches() || zk_node_host_pattern.matcher(url).matches()
      case "mongodb" => tcp_url_ip_port_pattern.matcher(url).matches() || tcp_url_host_port_pattern.matcher(url).matches() || tcp_url_ip_pattern.matcher(url).matches() || tcp_url_host_pattern.matcher(url).matches()
      case "parquet" => hdfs_path_pattern.matcher(url).matches()
      case _ => tcp_url_ip_port_pattern.matcher(url).matches() || tcp_url_host_port_pattern.matcher(url).matches()
    }
  }

  def getTip(nsSys: String, url: String): String = {
    nsSys.toLowerCase match {
      case "mysql" | "oracle" | "postgresql" | "vertica" | "phoenix" => s"connection url $url format is wrong, please alter it as localhost:3306"
      case "kafka" | "redis" | "cassandra" => s"connection url $url format is wrong, please alter it as localhost:9092(,localhost1:9092)*"
      case "hbase" => s"connection url $url format is wrong, please alter it as localhost:2181(/hbase)*(,localhost:2181(/hbase)*)*"
      case "es" => s"connection url $url format is wrong, please alter it as http(s)?://localhost:9200 or localhost:9300"
      case "mongodb" => s"connection url $url format is wrong, please alter it as host1[:port1][,host2[:port2],...[,hostN[:portN]]"
      case "parquet" => s"connection url $url format is wrong, please alter it for example hdfs://host1[:port1]/user/wormhole"
      case _ => s"connection url $url format is wrong, please alter it as localhost:8088"
    }
  }

  def generateNsInstance(connUrl: String): String = stringHash(connUrl).toString
}
