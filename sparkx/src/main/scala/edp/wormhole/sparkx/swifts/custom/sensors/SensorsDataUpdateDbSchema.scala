package edp.wormhole.sparkx.swifts.custom.sensors

import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkx.swifts.custom.sensors.updatecolumn.{ParamUtils, SchemaUtils}
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, WormholeConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by IntelliJ IDEA.
  *
  * @author daemon
  *  19/11/14 15:35
  *       To change this template use File | Settings | File Templates.
  */
class SensorsDataUpdateDbSchema extends EdpLogging{
  def  transform(session: SparkSession, df: DataFrame, flowConfig: SwiftsProcessConfig,param:String,streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String):DataFrame= {
    if (param == null) {
      throw new IllegalArgumentException("param must be not empty")
    }
    val paramUtil = new ParamUtils(param, streamConfig.zookeeper_address, streamConfig.zookeeper_path + "/sensors/" + streamConfig.spark_config.stream_id, sourceNamespace)
    val schemaUtils = new SchemaUtils(paramUtil)

    val columnMapDf = new java.util.HashMap[String, String]()
    df.schema.foreach(dfSchema => {
      val dataTypeSparkToCK = DataTypeSparkToCK.sparkTypeOf(dfSchema.dataType)
      if (dataTypeSparkToCK == null) columnMapDf.put(dfSchema.name, DataTypeSparkToCK.STRING.getClickHouseDataType)
      else columnMapDf.put(dfSchema.name, dataTypeSparkToCK.getClickHouseDataType)
    })

    schemaUtils.checkSchemaNeedChange(columnMapDf)
    schemaUtils.destroy()
    df
  }

}
