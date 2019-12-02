package edp.wormhole.sparkx.swifts.custom

import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig, WormholeConfig}
import org.apache.spark.sql._

/**
  * Created by neo_qiang on 2019/10/7.
  */
class SplitColumnByKey extends SwiftsInterface{
  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, param:String, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String):DataFrame={
    println("SplitColumnByKey set param :"+param)
    df
  }
}
