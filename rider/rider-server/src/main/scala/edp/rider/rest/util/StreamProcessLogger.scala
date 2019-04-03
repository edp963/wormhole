package edp.rider.rest.util

import edp.rider.common.RiderLogger

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}

class StreamProcessLogger extends ProcessLogger with RiderLogger{

  override def buffer[T](f: => T): T = f

  override def out(s: => String): Unit = { riderLogger.info(s"read process out stream : $s") }

  override def err(s: => String): Unit = { riderLogger.info(s"read process err stream : $s") }

}
