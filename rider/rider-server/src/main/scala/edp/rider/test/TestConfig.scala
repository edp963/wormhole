package edp.rider.test

import edp.rider.RiderStarter.modules.config

/**
  * Author: lukong
  * Date: 2018/7/7
  * Description:
  */
object TestConfig {

  def main(args: Array[String]): Unit = {
    val host = config.getString("wormholeServer.host")
    val port = config.getInt("wormholeServer.port")
    println(s"host->$host port->$port")
  }
}
