package edp.wormhole.swifts

import edp.wormhole.util.config.{ConnectionConfig, KVConfig}
import org.apache.log4j.Logger

import scala.collection.mutable

object ConnectionMemoryStorage {

  private val logger: Logger = Logger.getLogger(ConnectionMemoryStorage.getClass)
  private val dataStoreConnectionsMap = mutable.HashMap.empty[String, ConnectionConfig]


  def getDataStoreConnectionsMap: Map[String, ConnectionConfig] = dataStoreConnectionsMap.toMap

  def registerDataStoreConnectionsMap(lookupNamespace: String, connectionUrl: String, username: Option[String], password: Option[String], parameters: Option[Seq[KVConfig]]) {
    logger.info("register datastore,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    val connectionNamespace: String = lookupNamespace.split("\\.").slice(0, 3).mkString(".")
    logger.info("connectionNamespace:" + connectionNamespace)
    if (!dataStoreConnectionsMap.contains(connectionNamespace)) {
      dataStoreConnectionsMap(connectionNamespace) = ConnectionConfig(connectionUrl, username, password, parameters)
      logger.info("register datastore success,lookupNamespace:" + lookupNamespace + ",connectionUrl;" + connectionUrl + ",username:" + username + ",password:" + password + ",parameters:" + parameters)
    }
  }

  def getDataStoreConnectionsWithMap(dataStoreConnectionsMap: Map[String, ConnectionConfig], namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")
    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

  def getDataStoreConnectionConfig(namespace: String): ConnectionConfig = {
    val connectionNs = namespace.split("\\.").slice(0, 3).mkString(".")

    if (dataStoreConnectionsMap.contains(connectionNs)) {
      dataStoreConnectionsMap(connectionNs)
    } else {
      throw new Exception("cannot resolve lookupNamespace, you do not send related directive.")
    }
  }

}
