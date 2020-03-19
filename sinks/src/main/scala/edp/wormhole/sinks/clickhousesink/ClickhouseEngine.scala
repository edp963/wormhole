package edp.wormhole.sinks.clickhousesink

import org.apache.log4j.Logger

object ClickhouseEngine extends Enumeration{
  private lazy val logger = Logger.getLogger(this.getClass)
  type ClickhouseEngine = Value

  val MERGETREE = Value("mergetree")
  val DISTRIBUTED = Value("distributed")

  def clickhouseEngine(s: String): ClickhouseEngine = try {
    ClickhouseEngine.withName(s.toLowerCase)
  } catch {
    case _: Throwable =>
      logger.warn(s"ClickhouseEngine invalid string: $s")
      DISTRIBUTED
  }

}
