package edp.wormhole.sinks.customersink

import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig

case class CustomerConfig(sinkNamespace: String,
                          connectionConfig: ConnectionConfig,
                          sinkProcessConfig: SinkProcessConfig)

case class ActorCase(sinkClassFullClass: String,
                     sourceNamespace: String,
                     sinkNamespace: String,
                     sinkProcessConfig: SinkProcessConfig,
                     schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                     tupleList: Seq[Seq[String]],
                     connectionConfig: ConnectionConfig)
