package edp.wormhole.sinks.customersink

import com.alibaba.fastjson.JSON
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.{DateUtils, JsonUtils}
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

class Data2CustomerSink extends SinkProcessor {
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
        val specialConfigJson = JSON.parseObject(specialConfig)
        if (specialConfigJson.containsKey("other_sinks_config")) {
          val otherSinksConfig = specialConfigJson.getJSONObject("other_sinks_config")
          var sinksConfig = Seq[ActorCase]()

          val sinkParallel = if (otherSinksConfig.containsKey("sink_parallel")) otherSinksConfig.getString("sink_parallel").toBoolean else false
          //first
          val firstClassFullClass =
            if (otherSinksConfig.containsKey("current_sink_class_fullname")) otherSinksConfig.getString("current_sink_class_fullname")
            else ""
          val firstSinkProcessConfig = SinkProcessConfig(sinkProcessConfig.sinkOutput, sinkProcessConfig.tableKeys, sinkProcessConfig.specialConfig,
            sinkProcessConfig.jsonSchema, firstClassFullClass, sinkProcessConfig.retryTimes, sinkProcessConfig.retrySeconds, sinkProcessConfig.kerberos)
          sinksConfig = sinksConfig :+ ActorCase(firstClassFullClass, sourceNamespace, sinkNamespace, firstSinkProcessConfig, schemaMap, tupleList, connectionConfig)

          //others
          if (otherSinksConfig.containsKey("other_sinks")) {
            val otherSinks = otherSinksConfig.getJSONArray("other_sinks")
            for (i <- 0 until otherSinks.size) {
              val otherSinkOrigin = otherSinks.getJSONObject(i)
              val otherSink = CustomerUtils.getOtherSinkConfig(otherSinkOrigin, sinkProcessConfig)
              if (otherSink != null) {
                sinksConfig = sinksConfig :+ ActorCase(otherSink.sinkProcessConfig.classFullname, sourceNamespace, otherSink.sinkNamespace, otherSink.sinkProcessConfig, schemaMap, tupleList, otherSink.connectionConfig)
              }
            }
          }

          var errorFlag = false
          if (sinkParallel) {
            sinksConfig.par.foreach(sinkConfig => {
              logger.info(s"sink ${sinkConfig.sinkClassFullClass} foreach test: ${DateUtils.currentyyyyMMddHHmmss}")
              val (sinkObject, sinkMethod) = CustomerUtils.getObjectAndMethod(sinkConfig.sinkClassFullClass)
              try {
                sinkMethod.invoke(sinkObject, sinkConfig.sourceNamespace, sinkConfig.sinkNamespace, sinkConfig.sinkProcessConfig, sinkConfig.schemaMap, sinkConfig.tupleList, sinkConfig.connectionConfig)
              } catch {
                case e: Throwable =>
                  logger.info(s"sink ${sinkConfig.sinkClassFullClass} error", e)
                  errorFlag = true
              }
            })
          } else {
            sinksConfig.foreach(sinkConfig => {
              logger.info(s"sink ${sinkConfig.sinkClassFullClass} foreach test: ${DateUtils.currentyyyyMMddHHmmss}")
              val (sinkObject, sinkMethod) = CustomerUtils.getObjectAndMethod(sinkConfig.sinkClassFullClass)
              try {
                sinkMethod.invoke(sinkObject, sinkConfig.sourceNamespace, sinkConfig.sinkNamespace, sinkConfig.sinkProcessConfig, sinkConfig.schemaMap, sinkConfig.tupleList, sinkConfig.connectionConfig)
              } catch {
                case e: Throwable =>
                  logger.info(s"sink ${sinkConfig.sinkClassFullClass} error", e)
                  errorFlag = true
              }
            })
          }
          if(errorFlag) throw new Exception("do data to customer sink error")
        }
      case None => None
    }
  }
}
