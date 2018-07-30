package edp.wormhole

import java.sql.Timestamp

import edp.wormhole.common.util.DateUtils
import edp.wormhole.ums.UmsSysField
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

class FlinkxTimestampExtractor(sourceSchemaMap: Map[String, (TypeInformation[_], Int)]) extends AscendingTimestampExtractor[Row] with Serializable {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  override def extractAscendingTimestamp(element: Row): Long = {
    val umsTs = element.getField(sourceSchemaMap(UmsSysField.TS.toString)._2)
    logger.info(s"umsTs in assignTimestamp $umsTs")
    DateUtils.dt2long(umsTs.asInstanceOf[Timestamp])
  }

}
