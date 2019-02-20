package edp.wormhole.sinks.customersink

import java.lang.reflect.Method

import com.alibaba.fastjson.JSONObject
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig

object CustomerUtils {
  def getObjectAndMethod(className: String): (Any, Method) = {
    val clazz = Class.forName(className)
    val obj = clazz.newInstance()
    val method = clazz.getMethod("process",
      classOf[String],
      classOf[String],
      classOf[SinkProcessConfig],
      classOf[collection.Map[String, (Int, UmsFieldType, Boolean)]],
      classOf[Seq[Seq[String]]],
      classOf[ConnectionConfig])
    (obj, method)
  }

  def getOtherSinkConfig(otherSinkConfigOrigin: JSONObject, sinkProcessConfig: SinkProcessConfig): CustomerConfig = {
    if (otherSinkConfigOrigin.containsKey("namespace")) {
      val otherSinkNamespace = otherSinkConfigOrigin.getString("namespace")
      //val otherSinkOutput = sinkProcessConfig.sinkOutput
      val otherTableKeys =
        if (otherSinkConfigOrigin.containsKey("sink_table_keys")) {
          val otherTableKeysRe = Some(otherSinkConfigOrigin.getString("sink_table_keys"))
          otherSinkConfigOrigin.remove("sink_table_keys")
          otherTableKeysRe
        }
        else None

      val otherJsonSchema =
        if (otherSinkConfigOrigin.containsKey("sink_schema")) {
          val otherJsonSchemaRe = Some(otherSinkConfigOrigin.getString("sink_schema"))
          otherSinkConfigOrigin.remove("sink_schema")
          otherJsonSchemaRe
        } else None

      val otherClassFullClass =
        if (otherSinkConfigOrigin.containsKey("sink_process_class_fullname")) {
          val otherClassFullClassRe = otherSinkConfigOrigin.getString("sink_process_class_fullname")
          otherSinkConfigOrigin.remove("sink_process_class_fullname")
          otherClassFullClassRe
        }
        else ""

      val otherRetryTimes =
        if (otherSinkConfigOrigin.containsKey("sink_retry_times")) {
          val otherRetryTimesRe = otherSinkConfigOrigin.getString("sink_retry_times").trim.toLowerCase.toInt
          otherSinkConfigOrigin.remove("sink_retry_times")
          otherRetryTimesRe
        }
        else sinkProcessConfig.retryTimes
      val otherRetrySeconds =
        if (otherSinkConfigOrigin.containsKey("sink_retry_seconds")) {
          val otherRetrySecondsRe = otherSinkConfigOrigin.getString("sink_retry_seconds").trim.toLowerCase.toInt
          otherSinkConfigOrigin.remove("sink_retry_seconds")
          otherRetrySecondsRe
        }
        else sinkProcessConfig.retrySeconds
      val otherKerberos =
        if (otherSinkConfigOrigin.containsKey("kerberos")) {
          val otherKerberosRe = otherSinkConfigOrigin.getString("kerberos").toBoolean
          otherSinkConfigOrigin.remove("kerberos")
          otherKerberosRe
        }
        else sinkProcessConfig.kerberos

      val otherSinkConnection =
        if (otherSinkConfigOrigin.containsKey("sink_connection")) {
          val otherSinkConnectionRe = JsonUtils.json2caseClass[ConnectionConfig](otherSinkConfigOrigin.getString("sink_connection"))
          otherSinkConfigOrigin.remove("sink_connection")
          otherSinkConnectionRe
        }
        else null

      val otherSpecialConfig = if (otherSinkConfigOrigin.isEmpty) None else Some(otherSinkConfigOrigin.toString)

      val otherSinkProcessConfig = SinkProcessConfig(sinkProcessConfig.sinkOutput, otherTableKeys, otherSpecialConfig,
        otherJsonSchema, otherClassFullClass, otherRetryTimes, otherRetrySeconds, otherKerberos)

      CustomerConfig(otherSinkNamespace, otherSinkConnection, otherSinkProcessConfig)
    }
    else null
  }
}
