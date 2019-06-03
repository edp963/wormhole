package edp.wormhole.sinks.customersink

import java.lang.reflect.Method

import com.alibaba.fastjson.{JSON, JSONObject}
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

  def getOtherSinkConfig(otherSinkOrigin: JSONObject, sinkProcessConfig: SinkProcessConfig): CustomerConfig = {
    if (otherSinkOrigin.containsKey("namespace")) {
      val otherSinkNamespace = otherSinkOrigin.getString("namespace")
      //val otherSinkOutput = sinkProcessConfig.sinkOutput
      val otherTableKeys =
        if (otherSinkOrigin.containsKey("sink_table_keys")) {
          val otherTableKeysRe = Some(otherSinkOrigin.getString("sink_table_keys"))
          otherSinkOrigin.remove("sink_table_keys")
          otherTableKeysRe
        }
        else sinkProcessConfig.tableKeys

      val otherJsonSchema =
        if (otherSinkOrigin.containsKey("sink_schema")) {
          val otherJsonSchemaRe = Some(otherSinkOrigin.getString("sink_schema"))
          otherSinkOrigin.remove("sink_schema")
          otherJsonSchemaRe
        } else None

      val otherClassFullClass =
        if (otherSinkOrigin.containsKey("sink_process_class_fullname")) {
          val otherClassFullClassRe = otherSinkOrigin.getString("sink_process_class_fullname")
          otherSinkOrigin.remove("sink_process_class_fullname")
          otherClassFullClassRe
        }
        else ""

      val otherRetryTimes =
        if (otherSinkOrigin.containsKey("sink_retry_times")) {
          val otherRetryTimesRe = otherSinkOrigin.getString("sink_retry_times").trim.toLowerCase.toInt
          otherSinkOrigin.remove("sink_retry_times")
          otherRetryTimesRe
        }
        else sinkProcessConfig.retryTimes
      val otherRetrySeconds =
        if (otherSinkOrigin.containsKey("sink_retry_seconds")) {
          val otherRetrySecondsRe = otherSinkOrigin.getString("sink_retry_seconds").trim.toLowerCase.toInt
          otherSinkOrigin.remove("sink_retry_seconds")
          otherRetrySecondsRe
        }
        else sinkProcessConfig.retrySeconds

      val otherKerberos =
        if (otherSinkOrigin.containsKey("kerberos")) {
          otherSinkOrigin.getString("kerberos").toBoolean
          //otherSinkOrigin.remove("kerberos")
        } else sinkProcessConfig.kerberos

      if (!otherSinkOrigin.containsKey("kerberos") && sinkProcessConfig.specialConfig.isDefined) {
        val sinkSpecificConfig =  JSON.parseObject(sinkProcessConfig.specialConfig.getOrElse("{}"))
        if(sinkSpecificConfig.containsKey("kerberos")) {
          otherSinkOrigin.put("kerberos", sinkSpecificConfig.getString("kerberos").toBoolean)
        }
      }

      val otherSinkConnection =
        if (otherSinkOrigin.containsKey("sink_connection")) {
          val otherSinkConnectionRe = JsonUtils.json2caseClass[ConnectionConfig](otherSinkOrigin.getString("sink_connection"))
          otherSinkOrigin.remove("sink_connection")
          otherSinkConnectionRe
        }
        else null

      val otherSpecialConfig = if (otherSinkOrigin.isEmpty) None else Some(otherSinkOrigin.toString)

      val otherSinkProcessConfig = SinkProcessConfig(sinkProcessConfig.sinkOutput, otherTableKeys, otherSpecialConfig,
        otherJsonSchema, otherClassFullClass, otherRetryTimes, otherRetrySeconds, otherKerberos)

      CustomerConfig(otherSinkNamespace, otherSinkConnection, otherSinkProcessConfig)
    }
    else null
  }
}
