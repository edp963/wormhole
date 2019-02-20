package edp.wormhole.sinks.customersink

import com.alibaba.fastjson.JSON
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.{DateUtils, JsonUtils}
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

class Data2CustomerSink extends SinkProcessor{
  private lazy val logger = Logger.getLogger(this.getClass)
  override def process(sourceNamespace: String,
                         sinkNamespace: String,
                         sinkProcessConfig: SinkProcessConfig,
                         schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                         tupleList: Seq[Seq[String]],
                         connectionConfig: ConnectionConfig): Unit = {
    logger.info("process data to customer sink")

    sinkProcessConfig.specialConfig match {
      case Some(specialConfig) =>
        var sinksConfig = Seq[ActorCase]()
        val specialConfigJson = JSON.parseObject(specialConfig)
        val parallel = if(specialConfigJson.containsKey("sink_parallel")) specialConfigJson.getString("sink_parallel").toBoolean else true
      //first
        val firstClassFullClass =
          if (specialConfigJson.containsKey("current_sink_class_fullname")) specialConfigJson.getString("current_sink_class_fullname")
          else ""
        val firstSinkProcessConfig = SinkProcessConfig(sinkProcessConfig.sinkOutput, sinkProcessConfig.tableKeys, sinkProcessConfig.specialConfig,
          sinkProcessConfig.jsonSchema, firstClassFullClass, sinkProcessConfig.retryTimes, sinkProcessConfig.retrySeconds, sinkProcessConfig.kerberos)
        sinksConfig = sinksConfig :+ ActorCase(firstClassFullClass, sourceNamespace, sinkNamespace, firstSinkProcessConfig, schemaMap, tupleList, connectionConfig)

        //others
        if (specialConfigJson.containsKey("other_sinks_config")) {
          val otherSinksConfig = specialConfigJson.getJSONArray("other_sinks_config")
          for (i <- 0 until otherSinksConfig.size) {
            val otherSinkConfigOrigin = otherSinksConfig.getJSONObject(i)
            val otherSinkConfig = CustomerUtils.getOtherSinkConfig(otherSinkConfigOrigin, sinkProcessConfig)
            if (otherSinkConfig != null) {
              sinksConfig = sinksConfig :+ ActorCase(otherSinkConfig.sinkProcessConfig.classFullname, sourceNamespace, otherSinkConfig.sinkNamespace, otherSinkConfig.sinkProcessConfig, schemaMap, tupleList, otherSinkConfig.connectionConfig)
            }
          }
        }

        if(parallel) {
          sinksConfig.par.foreach(sinkConfig => {
            //logger.info(s"sink ${sinkConfig.sinkClassFullClass} foreach test: ${DateUtils.currentyyyyMMddHHmmss}")
            val (sinkObject, sinkMethod) = CustomerUtils.getObjectAndMethod(sinkConfig.sinkClassFullClass)
            sinkMethod.invoke(sinkObject, sinkConfig.sourceNamespace, sinkConfig.sinkNamespace, sinkConfig.sinkProcessConfig, sinkConfig.schemaMap, sinkConfig.tupleList, sinkConfig.connectionConfig)
          })
        } else {
          sinksConfig.foreach(sinkConfig => {
            //logger.info(s"sink ${sinkConfig.sinkClassFullClass} foreach test: ${DateUtils.currentyyyyMMddHHmmss}")
            val (sinkObject, sinkMethod) = CustomerUtils.getObjectAndMethod(sinkConfig.sinkClassFullClass)
            sinkMethod.invoke(sinkObject, sinkConfig.sourceNamespace, sinkConfig.sinkNamespace, sinkConfig.sinkProcessConfig, sinkConfig.schemaMap, sinkConfig.tupleList, sinkConfig.connectionConfig)
          })
        }
      case None => None
    }
  }
}
